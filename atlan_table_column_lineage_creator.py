#!/usr/bin/env python3
"""
Table and Column Lineage Creator

This script finds matching tables and columns across PostgreSQL, S3, and Snowflake connections
and creates lineage relationships between them (Postgres -> S3 -> Snowflake).
"""

import os
import re
from typing import List, Dict, Any, Optional, Tuple, Set
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, Table, Column, Database, Schema
from pyatlan.model.fluent_search import CompoundQuery, FluentSearch
from pyatlan.model.lineage import LineageDirection, LineageRequest, LineageResponse
from pyatlan.model.enums import AtlanConnectorType

##############################################
# CONFIGURATION - MODIFY THESE VARIABLES
##############################################

# Atlan connection details (or set as environment variables)
ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL", "https://your-atlan-instance.com")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY", "your-api-key")

# Connection names for each data platform
POSTGRES_CONNECTION_NAME = "postgres-vv"  # Replace with your PostgreSQL connection name
S3_CONNECTION_NAME = "aws-s3-connection-vv"  # Replace with your S3 connection name
SNOWFLAKE_CONNECTION_NAME = "snowflake-vv"  # Replace with your Snowflake connection name

# Name normalization settings
# Whether to normalize table and column names for matching (removes special chars, makes lowercase)
NORMALIZE_NAMES = True

# Matching thresholds
# Minimum similarity score (0-100) to consider tables/columns a match
TABLE_MATCH_THRESHOLD = 80
COLUMN_MATCH_THRESHOLD = 80

##############################################
# HELPER FUNCTIONS
##############################################

def initialize_atlan_client() -> Optional[AtlanClient]:
    """Initialize Atlan client from configuration variables."""
    if not ATLAN_BASE_URL or not ATLAN_API_KEY:
        print("Error: ATLAN_BASE_URL and ATLAN_API_KEY must be configured.")
        return None
    
    try:
        client = AtlanClient(
            base_url=ATLAN_BASE_URL,
            api_key=ATLAN_API_KEY
        )
        print(f"Connected to Atlan at {ATLAN_BASE_URL}")
        return client
    except Exception as e:
        print(f"Error connecting to Atlan: {str(e)}")
        return None

def normalize_name(name: str) -> str:
    """
    Normalize a name by removing special characters, making lowercase, etc.
    This helps with matching names across different systems.
    """
    if not name:
        return ""
    
    # Convert to lowercase
    normalized = name.lower()
    
    # Remove special characters and replace with underscore
    normalized = re.sub(r'[^a-z0-9_]', '_', normalized)
    
    # Replace multiple underscores with a single one
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    return normalized

def calculate_name_similarity(name1: str, name2: str) -> int:
    """
    Calculate a similarity score (0-100) between two names.
    Higher score means more similar.
    """
    if not name1 or not name2:
        return 0
    
    # Normalize if configured
    if NORMALIZE_NAMES:
        name1 = normalize_name(name1)
        name2 = normalize_name(name2)
    
    # Exact match
    if name1 == name2:
        return 100
    
    # One is a substring of the other
    if name1 in name2 or name2 in name1:
        shorter = min(len(name1), len(name2))
        longer = max(len(name1), len(name2))
        return int((shorter / longer) * 100)
    
    # Calculate Levenshtein distance
    # Simple implementation, can be optimized
    m, n = len(name1), len(name2)
    if m == 0 or n == 0:
        return 0
    
    matrix = [[0 for _ in range(n + 1)] for _ in range(m + 1)]
    
    for i in range(m + 1):
        matrix[i][0] = i
    
    for j in range(n + 1):
        matrix[0][j] = j
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if name1[i-1] == name2[j-1] else 1
            matrix[i][j] = min(
                matrix[i-1][j] + 1,      # deletion
                matrix[i][j-1] + 1,      # insertion
                matrix[i-1][j-1] + cost  # substitution
            )
    
    distance = matrix[m][n]
    max_len = max(m, n)
    similarity = int(((max_len - distance) / max_len) * 100)
    
    return similarity

##############################################
# TABLE AND COLUMN RETRIEVAL FUNCTIONS
##############################################

def get_connection_by_name(client: AtlanClient, connection_name: str) -> Optional[Dict[str, Any]]:
    """
    Find a connection by name and return its details.
    
    Args:
        client: Atlan client
        connection_name: Name of the connection to find
        
    Returns:
        Dictionary with connection details or None if not found
    """
    try:
        connection_request = (
            FluentSearch()
            .where(CompoundQuery.active_assets())
            .where(CompoundQuery.asset_type(Connection))
            .where(Connection.NAME.eq(connection_name))
            .page_size(1)
        ).to_request()
        
        connection_results = client.asset.search(connection_request)
        
        if connection_results.count == 0:
            print(f"No connection found with name: {connection_name}")
            return None
            
        connections_list = list(connection_results)
        if not connections_list:
            return None
            
        connection = connections_list[0]
        
        return {
            "name": connection.name,
            "guid": connection.guid,
            "qualified_name": connection.qualified_name,
            "connector_type": connection.connector_type if hasattr(connection, 'connector_type') else None
        }
        
    except Exception as e:
        print(f"Error finding connection '{connection_name}': {str(e)}")
        return None

def get_tables_from_connection(client: AtlanClient, connection_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieve all tables associated with a connection.
    
    Args:
        client: Atlan client
        connection_details: Dictionary with connection details
        
    Returns:
        List of dictionaries with table details
    """
    tables = []
    connection_qn = connection_details["qualified_name"]

    try:
        # Different query approach based on connector type
        if connection_qn.startswith("default/s3"):
            # For S3, we can query tables directly with connection qualified name
            table_request = (
                FluentSearch()
                .where(CompoundQuery.active_assets())
                .where(CompoundQuery.asset_type(Table))
                .where(Table.QUALIFIED_NAME.startswith(connection_qn))
                .page_size(1000)
            ).to_request()
            
            table_results = client.asset.search(table_request)
            tables_list = list(table_results)
            
            for table in tables_list:
                tables.append({
                    "name": table.name,
                    "guid": table.guid,
                    "qualified_name": table.qualified_name,
                    "connection_name": connection_details["name"],
                    "normalized_name": normalize_name(table.name) if NORMALIZE_NAMES else table.name
                })
                
        else:
            # For PostgreSQL and Snowflake, we need to go through databases and schemas
            # First, find all databases in the connection
            database_request = (
                FluentSearch()
                .where(CompoundQuery.active_assets())
                .where(CompoundQuery.asset_type(Database))
                .where(Database.CONNECTION_QUALIFIED_NAME.eq(connection_qn))
                .page_size(100)
            ).to_request()
            
            database_results = client.asset.search(database_request)
            databases_list = list(database_results)
            
            for database in databases_list:
                # Find all schemas in the database
                schema_request = (
                    FluentSearch()
                    .where(CompoundQuery.active_assets())
                    .where(CompoundQuery.asset_type(Schema))
                    .where(Schema.DATABASE_QUALIFIED_NAME.eq(database.qualified_name))
                    .page_size(100)
                ).to_request()
                
                schema_results = client.asset.search(schema_request)
                schemas_list = list(schema_results)
                
                for schema in schemas_list:
                    # Find all tables in the schema
                    table_request = (
                        FluentSearch()
                        .where(CompoundQuery.active_assets())
                        .where(CompoundQuery.asset_type(Table))
                        .where(Table.SCHEMA_QUALIFIED_NAME.eq(schema.qualified_name))
                        .page_size(1000)
                    ).to_request()
                    
                    table_results = client.asset.search(table_request)
                    tables_list = list(table_results)
                    
                    for table in tables_list:
                        tables.append({
                            "name": table.name,
                            "guid": table.guid,
                            "qualified_name": table.qualified_name,
                            "connection_name": connection_details["name"],
                            "database_name": database.name,
                            "schema_name": schema.name,
                            "normalized_name": normalize_name(table.name) if NORMALIZE_NAMES else table.name
                        })
        
        print(f"Found {len(tables)} tables for connection: {connection_details['name']}")
        return tables
        
    except Exception as e:
        print(f"Error getting tables for connection '{connection_details['name']}': {str(e)}")
        return []

def get_columns_for_table(client: AtlanClient, table_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieve all columns for a specified table.
    
    Args:
        client: Atlan client
        table_details: Dictionary with table details
        
    Returns:
        List of dictionaries with column details
    """
    columns = []
    
    try:
        column_request = (
            FluentSearch()
            .where(CompoundQuery.active_assets())
            .where(CompoundQuery.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(table_details["qualified_name"]))
            .page_size(1000)
        ).to_request()
        
        column_results = client.asset.search(column_request)
        
        if column_results.count == 0:
            print(f"No columns found for table: {table_details['name']}")
            return []
            
        columns_list = list(column_results)
        
        for column in columns_list:
            data_type = None
            
            # Try different attribute names for data type
            for attr_name in ['data_type', 'column_type', 'type_name', 'sql_type']:
                if hasattr(column, attr_name) and getattr(column, attr_name):
                    data_type = getattr(column, attr_name)
                    break
            
            columns.append({
                "name": column.name,
                "guid": column.guid,
                "qualified_name": column.qualified_name,
                "data_type": data_type,
                "order": column.order if hasattr(column, 'order') else None,
                "normalized_name": normalize_name(column.name) if NORMALIZE_NAMES else column.name
            })
        
        # Sort columns by order if available
        columns.sort(key=lambda col: col["order"] if col["order"] is not None else 999999)
        
        return columns
        
    except Exception as e:
        print(f"Error getting columns for table '{table_details['name']}': {str(e)}")
        return []

##############################################
# MATCHING FUNCTIONS
##############################################

def find_matching_tables(source_tables: List[Dict[str, Any]], target_tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Find matching tables between source and target based on name similarity.
    
    Args:
        source_tables: List of source table details
        target_tables: List of target table details
        
    Returns:
        List of dictionaries with matching table pairs
    """
    matches = []
    
    for source_table in source_tables:
        best_match = None
        best_score = 0
        
        for target_table in target_tables:
            similarity = calculate_name_similarity(
                source_table["normalized_name"] if NORMALIZE_NAMES else source_table["name"],
                target_table["normalized_name"] if NORMALIZE_NAMES else target_table["name"]
            )
            
            if similarity > best_score and similarity >= TABLE_MATCH_THRESHOLD:
                best_score = similarity
                best_match = target_table
        
        if best_match:
            matches.append({
                "source_table": source_table,
                "target_table": best_match,
                "similarity": best_score
            })
    
    print(f"Found {len(matches)} matching table pairs")
    return matches

def find_matching_columns(client: AtlanClient, table_matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Find matching columns between the matched table pairs.
    
    Args:
        client: Atlan client
        table_matches: List of dictionaries with matching table pairs
        
    Returns:
        List of dictionaries with table pairs and their matching columns
    """
    results = []
    
    for match in table_matches:
        source_table = match["source_table"]
        target_table = match["target_table"]
        
        print(f"Finding matching columns between {source_table['name']} and {target_table['name']}")
        
        # Get columns for both tables
        source_columns = get_columns_for_table(client, source_table)
        target_columns = get_columns_for_table(client, target_table)
        
        if not source_columns or not target_columns:
            print(f"Warning: Missing columns for {source_table['name']} or {target_table['name']}")
            continue
        
        # Find matching columns
        column_matches = []
        for source_column in source_columns:
            best_match = None
            best_score = 0
            
            for target_column in target_columns:
                similarity = calculate_name_similarity(
                    source_column["normalized_name"] if NORMALIZE_NAMES else source_column["name"],
                    target_column["normalized_name"] if NORMALIZE_NAMES else target_column["name"]
                )
                
                if similarity > best_score and similarity >= COLUMN_MATCH_THRESHOLD:
                    best_score = similarity
                    best_match = target_column
            
            if best_match:
                column_matches.append({
                    "source_column": source_column,
                    "target_column": best_match,
                    "similarity": best_score
                })
        
        results.append({
            "source_table": source_table,
            "target_table": target_table,
            "table_similarity": match["similarity"],
            "column_matches": column_matches
        })
        
        print(f"  Found {len(column_matches)} matching column pairs")
    
    return results

##############################################
# LINEAGE CREATION FUNCTIONS
##############################################

def create_table_lineage(client: AtlanClient, source_table: Dict[str, Any], target_table: Dict[str, Any], process_name: str) -> Optional[str]:
    """
    Alternative approach to create a lineage relationship between two tables by creating
    the process first and then connecting it to source and target.
    
    Args:
        client: Atlan client
        source_table: Source table details
        target_table: Target table details
        process_name: Name of the process creating the lineage
        
    Returns:
        Process GUID if successful, None otherwise
    """
    try:
        from pyatlan.model.assets import Process, Table

        # Create a unique process qualified name
        process_qualified_name = f"process/vv/{source_table['connection_name']}-{source_table['name']}_to_{target_table['connection_name']}-{target_table['name']}"

        # Create the process asset
        process = Process()
        process.name = process_name
        process.qualified_name = process_qualified_name
        process.description = f"Lineage from {source_table['connection_name']}-{source_table['name']} to {target_table['connection_name']}-{target_table['name']}"
          
        # Create input/output connections
        # Add Table GUIDs to inputs and outputs
        process.inputs = [Table.ref_by_guid(guid=source_table['guid'])]
        process.outputs = [Table.ref_by_guid(guid=target_table['guid'])]

        # Save the process  
        response = client.asset.save(process)


        # Check if the process was created successfully
        if response:
            print(f"Created table lineage from {source_table['connection_name']}-{source_table['name']} to {target_table['connection_name']}-{target_table['name']}")
            return True
        else:
            print(f"Failed to create table lineage from {source_table['connection_name']}-{source_table['name']} to {target_table['connection_name']}-{target_table['name']}")
            return False
            
    except Exception as e:
        print(f"Error creating table lineage: {str(e)}")
        return None


def create_column_lineage(client: AtlanClient, source_table: Dict[str, Any], target_table: Dict[str, Any], column_match: Dict[str, Any],  process_name: str) -> bool:
    """
    Create a lineage relationship between two columns
    
    Args:
        client: Atlan client
        column_match: Dictionary with matching column details
        process_guid: GUID of the process created for the table lineage
        
    Returns:
        Boolean indicating success or failure
    """
    try:
        from pyatlan.model.assets import ColumnProcess, Column

        source_column = column_match['source_column']
        target_column = column_match['target_column']

        # Create a unique process qualified name
        column_process_qualified_name = f"process/vv/{source_table['connection_name']}-{source_table['name']}-{source_column['name']}_to_{target_table['connection_name']}-{target_table['name']}-{target_column['name']}"

        # Create the process asset
        column_process = ColumnProcess()
        column_process.name = process_name
        column_process.qualified_name = column_process_qualified_name
        column_process.description = f"Lineage from {source_table['connection_name']}-{source_table['name']}-{source_column['name']} to {target_table['connection_name']}-{target_table['name']}-{target_column['name']}"

        # Create input/output connections
        # Add Table GUIDs to inputs and outputs
        column_process.inputs = [Column.ref_by_guid(guid=source_column['guid'])]
        column_process.outputs = [Column.ref_by_guid(guid=target_column['guid'])]

        # Save the process  
        response = client.asset.save(column_process)

        if response:
            print(f"Created column lineage from {source_table['connection_name']}-{source_table['name']}-{source_column['name']} to {target_table['connection_name']}-{target_table['name']}-{target_column['name']}")
            return True
        else:
            print(f"Failed to create column lineage from {source_table['connection_name']}-{source_table['name']}-{source_column['name']} to {target_table['connection_name']}-{target_table['name']}-{target_column['name']}")
            return False
            
    except Exception as e:
        print(f"Error creating column lineage: {str(e)}")
        return False


##############################################
# MAIN PROCESS FUNCTIONS
##############################################

def create_end_to_end_lineage(client: AtlanClient):
    """
    Main function to create end-to-end lineage across PostgreSQL, S3, and Snowflake.
    
    Args:
        client: Atlan client
    """
    try:
        # Step 1: Get connection details
        print("\n=== Getting Connection Details ===\n")
        
        postgres_conn = get_connection_by_name(client, POSTGRES_CONNECTION_NAME)
        s3_conn = get_connection_by_name(client, S3_CONNECTION_NAME)
        snowflake_conn = get_connection_by_name(client, SNOWFLAKE_CONNECTION_NAME)
        
        if not postgres_conn:
            print(f"Error: PostgreSQL connection '{POSTGRES_CONNECTION_NAME}' not found")
            return
            
        if not s3_conn:
            print(f"Error: S3 connection '{S3_CONNECTION_NAME}' not found")
            return
            
        if not snowflake_conn:
            print(f"Error: Snowflake connection '{SNOWFLAKE_CONNECTION_NAME}' not found")
            return
        
        # Step 2: Get tables for each connection
        print("\n=== Getting Tables for Each Connection ===\n")
        
        postgres_tables = get_tables_from_connection(client, postgres_conn)
        s3_tables = get_tables_from_connection(client, s3_conn)
        snowflake_tables = get_tables_from_connection(client, snowflake_conn)
        
        if not postgres_tables:
            print(f"Warning: No tables found for PostgreSQL connection '{POSTGRES_CONNECTION_NAME}'")
        
        if not s3_tables:
            print(f"Warning: No tables found for S3 connection '{S3_CONNECTION_NAME}'")
        
        if not snowflake_tables:
            print(f"Warning: No tables found for Snowflake connection '{SNOWFLAKE_CONNECTION_NAME}'")
        
        # Step 3: Match tables between PostgreSQL and S3
        print("\n=== Matching PostgreSQL Tables to S3 Tables ===\n")
        postgres_to_s3_matches = find_matching_tables(postgres_tables, s3_tables)
        
        # Step 4: Match tables between S3 and Snowflake
        print("\n=== Matching S3 Tables to Snowflake Tables ===\n")
        s3_to_snowflake_matches = find_matching_tables(s3_tables, snowflake_tables)
        
        # Step 5: Find matching columns for each table pair
        print("\n=== Finding Matching Columns for PostgreSQL to S3 ===\n")
        postgres_to_s3_column_matches = find_matching_columns(client, postgres_to_s3_matches)
        
        print("\n=== Finding Matching Columns for S3 to Snowflake ===\n")
        s3_to_snowflake_column_matches = find_matching_columns(client, s3_to_snowflake_matches)
        
        # Step 6: Create lineage for PostgreSQL to S3
        print("\n=== Creating Lineage from PostgreSQL to S3 ===\n")
        for match in postgres_to_s3_column_matches:
            # Create table-level lineage
            success = create_table_lineage(
                client,
                match["source_table"],
                match["target_table"],
                "PostgreSQL_to_S3 ETL Process"
            )
            
            if success:
                # Create column-level lineage using the same process
                for column_match in match['column_matches']:
                    create_column_lineage(client,  match["source_table"], match["target_table"],column_match, "PostgreSQL to S3 ETL Process")

        
        # Step 7: Create lineage for S3 to Snowflake
        print("\n=== Creating Lineage from S3 to Snowflake ===\n")
        for match in s3_to_snowflake_column_matches:
            # Create table-level lineage
            success = create_table_lineage(
                client,
                match["source_table"],
                match["target_table"],
                "S3 to Snowflake ETL Process"
            )
            
            if success:
                # Create column-level lineage using the same process
                for column_match in match['column_matches']:
                    print(column_match)
                    create_column_lineage(client, match["source_table"], match["target_table"],column_match, "S3 to Snowflake ETL Process")
      
        print("\n=== End-to-End Lineage Creation Complete ===\n")
        
    except Exception as e:
        print(f"Error in create_end_to_end_lineage: {str(e)}")
        import traceback
        traceback.print_exc()

##############################################
# MAIN EXECUTION
##############################################

def main():
    """Main entry point for the script."""
    # Initialize Atlan client
    client = initialize_atlan_client()
    if not client:
        print("Failed to initialize Atlan client. Please check configuration.")
        return
    
    # Create end-to-end lineage
    create_end_to_end_lineage(client)

# Run the script
if __name__ == "__main__":
    main()