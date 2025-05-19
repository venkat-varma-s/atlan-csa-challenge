import boto3
from datetime import datetime
import os
import pandas as pd
import io
from typing import List, Dict, Any, Optional, Tuple
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, S3Bucket, S3Object, Asset, AtlasGlossaryTerm
from pyatlan.model.enums import AtlanConnectorType,CertificateStatus
from pyatlan.model.fluent_search import CompoundQuery, FluentSearch


# Configuration - can be moved to environment variables or config file in production
CONFIG = {
    "atlan": {
        "api_key": os.getenv("ATLAN_API_KEY"),  # Get API key from environment variable
        "base_url": os.getenv("ATLAN_BASE_URL")  # Get Atlan Base URL from environment variable
    },
    "aws": {
        "region": "us-east-2",  # US Ohio region
        "bucket_name": "atlan-tech-challenge",
        "access_key": os.getenv("AWS_ACCESS_KEY"),  # Get AWS Acces Key from environment variable
        "secret_key": os.getenv("AWS_SECRET_KEY")  # Get AS Secret Key from environment variable
    },
    "unique_identifier": "vvs",  # Use your initials or other unique identifier
    "connection_name": "aws-s3-connection-vv"
}

# Initialize Atlan client
def initialize_atlan_client() -> AtlanClient:
    """Initialize and return an Atlan client."""
    client = AtlanClient(
        base_url=CONFIG["atlan"]["base_url"],
        api_key=CONFIG["atlan"]["api_key"]
    )
    print(f"Connected to Atlan at {CONFIG['atlan']['base_url']}")
    return client


# Initialize AWS S3 client
def initialize_s3_client() -> boto3.client:
    """Initialize and return an AWS S3 client."""
    s3_client = boto3.client(
        's3',
        region_name=CONFIG["aws"]["region"],
        aws_access_key_id=CONFIG["aws"]["access_key"],
        aws_secret_access_key=CONFIG["aws"]["secret_key"]
    )
    print(f"Connected to AWS S3 in region {CONFIG['aws']['region']}")
    return s3_client


# Extract S3 bucket metadata
def extract_s3_bucket_metadata(s3_client: boto3.client) -> Dict[str, Any]:
    """Extract metadata for the configured S3 bucket."""
    bucket_name = CONFIG["aws"]["bucket_name"]
    
    try:
        # Get bucket metadata
        bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)
        bucket_tags_response = s3_client.get_bucket_tagging(Bucket=bucket_name)
        bucket_tags = {tag['Key']: tag['Value'] for tag in bucket_tags_response.get('TagSet', [])}
    except Exception as e:
        print(f"Warning: Couldn't fetch complete bucket metadata: {e}")
        bucket_location = {"LocationConstraint": CONFIG["aws"]["region"]}
        bucket_tags = {}
    
    return {
        "name": bucket_name,
        "arn": f"arn:aws:s3:::{bucket_name}",
        "region": bucket_location.get("LocationConstraint", CONFIG["aws"]["region"]),
        "tags": bucket_tags,
        "creation_date": datetime.now().isoformat()  # Actual creation date not available via API
    }

# Extract CSV Schema
def extract_csv_schema(s3_client: boto3.client, bucket_name: str, object_key: str, sample_size: int = 10000) -> List[Dict[str, str]]:
    """
    Extract column names and infer data types from a CSV file in S3.
    Returns a list of dictionaries with column information.
    """
    # Get object size to determine if we need to sample
    object_info = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    object_size = object_info['ContentLength']
    
    try:
        # For small files, read the entire file
        if object_size <= sample_size:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            csv_content = response['Body'].read()
        else:
            # For larger files, read just a sample
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key, Range=f'bytes=0-{sample_size-1}')
            csv_content = response['Body'].read()
        
        # Try to infer the delimiter
        potential_delimiters = [',', ';', '\t', '|']
        delimiter = None
        for delim in potential_delimiters:
            if delim.encode() in csv_content:
                # Check if this delimiter appears in multiple lines
                if csv_content.count(delim.encode()) > csv_content.count(b'\n'):
                    delimiter = delim
                    break
        
        if not delimiter:
            delimiter = ','  # Default to comma if we can't detect
        
        # Parse sample with pandas to infer types
        df_sample = pd.read_csv(io.BytesIO(csv_content), delimiter=delimiter, nrows=100, 
                               engine='python', on_bad_lines='skip')
        
        # Map pandas dtypes to more readable types
        schema = []
        for col_name in df_sample.columns:
            dtype = df_sample[col_name].dtype
            
            # Map pandas dtype to SQL-like type
            if pd.api.types.is_integer_dtype(dtype):
                atlan_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                atlan_type = "DOUBLE"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                atlan_type = "TIMESTAMP"
            elif pd.api.types.is_bool_dtype(dtype):
                atlan_type = "BOOLEAN"
            else:
                atlan_type = "VARCHAR"
            
            # Include some sample values for verification
            sample_values = df_sample[col_name].dropna().head(3).tolist()
            sample_values = [str(v) for v in sample_values]
            
            schema.append({
                "name": col_name,
                "data_type": atlan_type,
                "pandas_type": str(dtype),
                "sample_values": sample_values
            })
        
        # Also include overall CSV stats
        csv_stats = {
            "total_columns": len(schema),
            "detected_delimiter": delimiter,
            "sample_rows": len(df_sample),
            "has_header": True  # Assuming CSV has header. If needed, can add logic to detect this
        }
        
        return {
            "columns": schema,
            "stats": csv_stats
        }
        
    except Exception as e:
        raise Exception(f"Error extracting CSV schema: {str(e)}")

# Extract S3 object metadata
def extract_s3_objects_metadata(s3_client: boto3.client, bucket_name: str, prefix: str = "") -> List[Dict[str, Any]]:
    """Extract metadata for objects in the specified S3 bucket with optional prefix."""
    objects_metadata = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # Using pagination to handle large buckets
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip folders (objects ending with '/')
                    if obj['Key'].endswith('/'):
                        continue
                        
                    # Get object metadata
                    try:
                        object_metadata = s3_client.head_object(Bucket=bucket_name, Key=obj['Key'])
                        
                        # Extract file format from extension
                        file_format = "unknown"
                        if '.' in obj['Key']:
                            file_format = obj['Key'].split('.')[-1].lower()
                        
                        metadata_obj ={
                            "key": obj['Key'],
                            "size": obj['Size'],
                            "last_modified": obj['LastModified'].isoformat(),
                            "etag": obj['ETag'].strip('"'),
                            "content_type": object_metadata.get('ContentType', 'application/octet-stream'),
                            "file_format": file_format,
                            "metadata": object_metadata.get('Metadata', {}),
                            "storage_class": obj.get('StorageClass', 'STANDARD')
                        }
                        # For CSV files, extract schema information
                        if file_format.lower() == 'csv':
                            try:
                                csv_schema = extract_csv_schema(s3_client, bucket_name, obj['Key'])
                                metadata_obj["csv_schema"] = csv_schema
                            except Exception as csv_err:
                                print(f"Warning: Couldn't extract CSV schema for {obj['Key']}: {csv_err}")
                                metadata_obj["csv_schema_error"] = str(csv_err)
                        
                        objects_metadata.append(metadata_obj)                        
                    except Exception as e:
                        print(f"Warning: Couldn't fetch metadata for object {obj['Key']}: {e}")
    except Exception as e:
        print(f"Error listing objects in bucket {bucket_name}: {e}")
    
    return objects_metadata




# Get or Create S3 connection in Atlan

def get_create_s3_connection(client: AtlanClient, connection_name: str):

    """Create an S3 Connection in Atlan if not exists already"""    

    request = (
    FluentSearch()  # 
    .where(CompoundQuery.active_assets())  # 
    .where(CompoundQuery.asset_type(Connection))  #
    .where(Connection.NAME.eq(connection_name))
    .page_size(1)
    ).to_request()  # 

    results = client.asset.search(request)

    if results.count != 0:
        print("AWS S3 connection already exists with connection name:",connection_name)      
        for conn in results:
            connection_qualified_name = conn.qualified_name
            return connection_qualified_name
    else:
        admin_role_guid = client.role_cache.get_id_for_name("$admin")
        connection = Connection.creator(  # 
            name=connection_name,  # 
            connector_type=AtlanConnectorType.S3,  # 
            admin_roles=[admin_role_guid],  # 
            admin_groups=["vvs-test-group"],  # 
            admin_users=["venkatvarma"],  # 
        )
        response = client.asset.save(connection)  # 
        connection_qualified_name = response.assets_created(asset_type=Connection)[0].qualified_name
        print("Created new AWS S3 connection with name :",connection_name, " and qualified name:",connection_qualified_name)
        
        return connection_qualified_name

# Create S3 bucket asset in Atlan
def get_create_s3_bucket_asset(client: AtlanClient, connection_qualified_name: str, bucket_metadata: Dict[str, Any]) -> S3Bucket:

    """Create an S3 bucket asset in Atlan if not exists already"""    
    
    request = (
    FluentSearch()  # 
    .where(CompoundQuery.active_assets())  # 
    .where(CompoundQuery.asset_type(S3Bucket))  #
    .where(S3Bucket.QUALIFIED_NAME.eq(f"{connection_qualified_name}/{bucket_metadata['arn']}_{CONFIG['unique_identifier']}"))
    .page_size(1)
    ).to_request()  # 

    results = client.asset.search(request)
    if results.count != 0:
        print("Bucket Already Exists ", bucket_metadata['name'])
        return f"{connection_qualified_name}/{bucket_metadata['arn']}_{CONFIG['unique_identifier']}"
    else:
        # Create the S3 bucket asset
  
        bucket = S3Bucket()
        bucket.name = bucket_metadata["name"]
        bucket.connection_qualified_name = connection_qualified_name
        bucket.aws_arn = f"{bucket_metadata['arn']}_{CONFIG['unique_identifier']}"  # Make ARN unique
        bucket.qualified_name = f"{connection_qualified_name}/{bucket_metadata['arn']}_{CONFIG['unique_identifier']}"
        bucket.aws_region = bucket_metadata["region"]
            
        # Add description and set status
        bucket.description = f"S3 bucket for Delta Arc Corp data pipeline. This stores data extracted from Postgres before loading to Snowflake."
        bucket.certificate_status = CertificateStatus.VERIFIED

        # Create the asset in Atlan
        created_bucket = client.asset.save(bucket)
        print("Bucket Asset Created : ", created_bucket.assets_created(asset_type=S3Bucket)[0].qualified_name)
        return created_bucket.assets_created(asset_type=S3Bucket)[0].qualified_name


# Create table and column assets in Atlan from CSV schema
def create_table_from_csv_schema(
    client: AtlanClient, 
    connection_qualified_name: str,
    bucket_qualified_name: str,
    object_qualified_name: str, 
    object_name: str, 
    csv_schema: Dict[str, Any]
) -> Optional[Asset]:
    """
    Create a table and column assets in Atlan based on CSV schema extracted from S3 object.
    
    Args:
        client: Atlan client
        connection_qualified_name: Qualified name of the S3 Connection to be used as Database qualified name
        
        bucket_qualified_name: Qualified name of the S3 Bucket to be used as Schema qualified name
        object_qualified_name: Qualified name of the S3 Object to be used as Table qualified name
        object_name: S3 object name to be used as Table Name (filename without extension)
        csv_schema: CSV schema information containing columns and stats
        
    Returns:
        Created table asset or None if creation failed
    """
    from pyatlan.model.assets import Table, Column
    
    try:        
        # Generate table name from the object name (without extension)
        table_name = object_name.split('.')[0] if '.' in object_name else object_name
        
        # Check if table already exists
        from pyatlan.model.fluent_search import CompoundQuery, FluentSearch
        
        request = (
            FluentSearch()
            .where(CompoundQuery.active_assets())
            .where(CompoundQuery.asset_type(Table))
            .where(Table.QUALIFIED_NAME.eq(object_qualified_name))
            .page_size(1)
        ).to_request()
        
        results = client.asset.search(request)
        
        if results.count != 0:
            print(f"Table already exists for {object_name}")
            return
            
        # Create table asset
        table = Table()
        table.name = table_name.upper()  # Convention to use uppercase for table names
        table.connector_name = AtlanConnectorType.S3
        table.qualified_name = object_qualified_name
        table.database_qualified_name = bucket_qualified_name
        table.schema_qualified_name = connection_qualified_name
        table.database_name = bucket_qualified_name.split("arn:aws:s3:::")[1]
        table.schema_name = CONFIG['connection_name']

        # Filter out unnamed columns
        columns_data = csv_schema.get("columns", [])
        filtered_columns = []
        for col in columns_data:
            # Skip columns that have "Unnamed:" prefix or empty names
            if not col["name"].startswith("Unnamed:") and col["name"].strip():
                filtered_columns.append(col)        
        
 
        # Update stats with filtered column count
        stats = csv_schema.get("stats", {})
        original_column_count = stats.get("total_columns", 0)
        filtered_column_count = len(filtered_columns)
 
 
        # Generate description with schema information
        table.description = (
            f"Table derived from S3 CSV object '{object_name}'. "
            f"Contains {filtered_column_count} columns (filtered from {original_column_count} total). "
            f"Delimiter: '{stats.get('detected_delimiter', ',')}'. "
            f"Automatically generated from CSV schema."
        )
        
        # Save the table with explicit boolean parameters
        table_response = client.asset.save(
            entity=table,  # Use named parameter to avoid positional argument issues
            replace_atlan_tags=False,
            replace_custom_metadata=False,
            overwrite_custom_metadata=False
        )
        
        created_table = table_response.assets_created(asset_type=Table)[0]
        print(f"Created table '{table_name}' successfully")
        
        # Create and save columns one by one
        columns_data = csv_schema.get("columns", [])
        for i, col_info in enumerate(filtered_columns):
            try:
                column = Column()
                column.name = col_info["name"]
                column.qualified_name = f"{object_qualified_name}/columns/{col_info['name']}"
                column.data_type = col_info["data_type"]
                column.order = i
                column.table_qualified_name = object_qualified_name
                column.table_name = table_name.upper()
                
                # Add sample values to column description
 #               if "sample_values" in col_info and col_info["sample_values"]:
#                    sample_str = ", ".join([str(v) for v in col_info["sample_values"][:3]])
#                    column.description = f"Data type: {col_info['data_type']}. Sample values: {sample_str}"
                
                # Save column with explicit boolean parameters
                client.asset.save(
                    entity=column,  # Use named parameter to avoid positional argument issues
                    replace_atlan_tags=False,
                    replace_custom_metadata=False,
                    overwrite_custom_metadata=False
                )
                
                print(f"Created column: {column.name}")
            except Exception as col_err:
                print(f"Error creating column {col_info['name']}: {str(col_err)}")
                
        print(f"Created {len(columns_data)} columns for table '{table_name}'")
        return created_table
        
        
    except Exception as e:
        print(f"Error creating table from CSV schema for {object_name}: {str(e)}")
        return None

# Create S3 object assets in Atlan
def create_s3_object_assets(client: AtlanClient, bucket_qualified_name: str, objects_metadata: List[Dict[str, Any]]) -> List[S3Object]:
    """Create S3 object assets in Atlan for each object in the metadata list."""
    
    created_objects = []
    
    for obj_metadata in objects_metadata:        
        # Create the S3 object asset if does not exist

        
        # Extract just the filename from the key
        if "/" in obj_metadata["key"]:
            object_name = obj_metadata["key"].split("/")[-1]
        else:
            object_name = obj_metadata["key"]
            

        request = (
            FluentSearch()  # 
            .where(CompoundQuery.active_assets())  # 
            .where(CompoundQuery.asset_type(S3Object))  #
            .where(S3Object.QUALIFIED_NAME.eq(f"{bucket_qualified_name}/{object_name}"))
            .page_size(1)
            ).to_request()  # 

        results = client.asset.search(request)

        if results.count != 0:
            print("Object Already Exists ", object_name)
            
            # Create table if object has CSV schema
            if "csv_schema" in obj_metadata and obj_metadata["file_format"].lower() == "csv":
                print(f"Creating table asset for CSV file: {object_name}")
                    
                table = create_table_from_csv_schema(
                    client=client,
                    connection_qualified_name=bucket_qualified_name.split("/arn:aws:s3:::")[0],
                    bucket_qualified_name=bucket_qualified_name,
                    object_qualified_name=f"{bucket_qualified_name}/{object_name}",
                    object_name=object_name,
                    csv_schema=obj_metadata["csv_schema"]
                )
            continue
        else:
            s3_object = S3Object()
            s3_object.name = object_name

            # Set the parent bucket relationship
            s3_object.s3_bucket_qualified_name = bucket_qualified_name
            s3_object.s3_bucket_name = bucket_qualified_name.split("arn:aws:s3:::")[1]

            # Set object properties
            s3_object.qualified_name = f"{bucket_qualified_name}/{object_name}"
            s3_object.s3_object_key = obj_metadata["key"]
            s3_object.s3_object_last_modified_time = obj_metadata["last_modified"]
            s3_object.s3_object_size = obj_metadata["size"]
            s3_object.s3_object_content_type = obj_metadata["content_type"]
                
                # Add description based on the file content and format
            if obj_metadata["file_format"] in ["csv", "parquet", "json"]:
                s3_object.description = f"Data file extracted from Postgres in {obj_metadata['file_format']} format."
            else:
                s3_object.description = f"File stored in Delta Arc Corp data lake."
                

            # Create the asset in Atlan
            try:
                created_object = client.asset.save(s3_object)
                created_objects.append(created_object)
                print(f"Created S3 object asset for {object_name}")
            except Exception as e:
                print(f"Error creating S3 object asset for {object_name}: {e}")

            # Create table if object has CSV schema
            if "csv_schema" in obj_metadata and obj_metadata["file_format"].lower() == "csv":
                print(f"Creating table asset for CSV file: {object_name}")
                    
                table = create_table_from_csv_schema(
                    client=client,
                    connection_qualified_name=bucket_qualified_name.split("/arn:aws:s3:::")[0],
                    bucket_qualified_name=bucket_qualified_name,
                    object_qualified_name=f"{bucket_qualified_name}/{object_name}",
                    object_name=object_name,
                    csv_schema=obj_metadata["csv_schema"]
                )
            
            
    return created_objects


def main():

    # Initialize clients
    atlan_client = initialize_atlan_client()
    s3_client = initialize_s3_client()
    
    # Extract S3 metadata
    print("Extracting S3 bucket metadata...")
    bucket_metadata = extract_s3_bucket_metadata(s3_client)
    
    print("Extracting S3 objects metadata...")
    objects_metadata = extract_s3_objects_metadata(s3_client, bucket_metadata["name"])
    print(f"Found {len(objects_metadata)} objects in S3 bucket")

    # Create S3 assets in Atlan
    print("Creating S3 connection in Atlan...")
    connection_qualified_name = get_create_s3_connection(atlan_client, CONFIG['connection_name'])


    print("Creating S3 bucket asset in Atlan...")
    bucket_qualified_name = get_create_s3_bucket_asset(atlan_client, connection_qualified_name, bucket_metadata)
    
    print("Creating S3 object assets in Atlan...")
    object_assets = create_s3_object_assets(atlan_client, bucket_qualified_name, objects_metadata)
    


if __name__ == "__main__":
    main()
    