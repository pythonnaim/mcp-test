
"""
Multi-Database PostgreSQL MCP Server
A comprehensive MCP server that provides secure access to multiple PostgreSQL databases
with schema discovery, query execution, and safety controls.
"""

import os
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
import json
import re

import asyncpg
from mcp.server.fastmcp import FastMCP, Context
from mcp.types import Resource, Tool
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Configuration for a database connection"""
    name: str
    url: str
    description: str = ""
    
@dataclass
class AppContext:
    """Application context containing database connections"""
    db_pools: Dict[str, asyncpg.Pool]
    db_configs: Dict[str, DatabaseConfig]

class PostgreSQLMCPServer:
    """Multi-database PostgreSQL MCP Server"""
    
    def __init__(self):
        self.app = FastMCP("PostgreSQL Multi-Database Server")
        self.setup_routes()
        
    def load_database_configs(self) -> Dict[str, DatabaseConfig]:
        """Load database configurations from environment variables"""
        configs = {}
        
        # Define your database mappings
        db_mappings = {
            'POSTGRES_URL_DODB': 'postgresql://doadmin:AVNS_hcOw9PEgnb3KoBtO2eu@db-postgresql-nyc3-88579-do-user-18166942-0.h.db.ondigitalocean.com:25060/bjb_tableau'
        }
        
        for env_var, db_name in db_mappings.items():
            url = os.getenv(env_var)
            if url:
                # Parse URL to get database name for description
                parsed = urlparse(url)
                db_display_name = parsed.path.lstrip('/') if parsed.path else db_name
                
                configs[db_name] = DatabaseConfig(
                    name=db_name,
                    url=url,
                    description=f"Database: {db_display_name}"
                )
                logger.info(f"Loaded configuration for database: {db_name}")
            else:
                logger.warning(f"Environment variable {env_var} not found")
                
        return configs

    async def create_db_pools(self, configs: Dict[str, DatabaseConfig]) -> Dict[str, asyncpg.Pool]:
        """Create connection pools for all databases"""
        pools = {}
        
        for db_name, config in configs.items():
            try:
                # Create connection pool with optimal settings
                pool = await asyncpg.create_pool(
                    config.url,
                    min_size=1,
                    max_size=10,
                    max_queries=50000,
                    max_inactive_connection_lifetime=300,
                    command_timeout=30
                )
                pools[db_name] = pool
                logger.info(f"Created connection pool for {db_name}")
            except Exception as e:
                logger.error(f"Failed to create pool for {db_name}: {str(e)}")
                
        return pools

    @asynccontextmanager
    async def app_lifespan(self, server: FastMCP) -> AsyncIterator[AppContext]:
        """Manage application lifecycle with database connections"""
        logger.info("Starting PostgreSQL MCP Server...")
        
        # Load database configurations
        db_configs = self.load_database_configs()
        if not db_configs:
            raise RuntimeError("No database configurations found. Please check your environment variables.")
        
        # Create connection pools
        db_pools = await self.create_db_pools(db_configs)
        if not db_pools:
            raise RuntimeError("Failed to create any database connection pools.")
        
        logger.info(f"Initialized {len(db_pools)} database connections")
        
        try:
            yield AppContext(db_pools=db_pools, db_configs=db_configs)
        finally:
            # Cleanup connections
            logger.info("Shutting down database connections...")
            for db_name, pool in db_pools.items():
                try:
                    await pool.close()
                    logger.info(f"Closed connection pool for {db_name}")
                except Exception as e:
                    logger.error(f"Error closing pool for {db_name}: {str(e)}")

    def validate_sql_query(self, query: str) -> bool:
        """Validate SQL query for safety (basic validation)"""
        query_lower = query.lower().strip()
        
        # Block dangerous operations
        dangerous_patterns = [
            r'\bdrop\s+table\b',
            r'\bdrop\s+database\b', 
            r'\bdelete\s+from\b',
            r'\btruncate\b',
            r'\balter\s+table\b',
            r'\bcreate\s+table\b',
            r'\binsert\s+into\b',
            r'\bupdate\s+\w+\s+set\b',
            r'\bgrant\b',
            r'\brevoke\b',
            r'\bdrop\s+user\b',
            r'\bcreate\s+user\b'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_lower):
                return False
                
        return True

    def setup_routes(self):
        """Setup MCP server routes"""
        
        @self.app.resource("databases://list")
        async def list_databases(ctx: Context) -> str:
            """List all available databases"""
            app_ctx: AppContext = ctx.app_context
            
            db_list = []
            for db_name, config in app_ctx.db_configs.items():
                status = "connected" if db_name in app_ctx.db_pools else "disconnected"
                db_list.append({
                    "name": db_name,
                    "description": config.description,
                    "status": status
                })
            
            return json.dumps(db_list, indent=2)

        @self.app.resource("schema://{database_name}")
        async def get_database_schema(database_name: str, ctx: Context) -> str:
            """Get schema information for a specific database"""
            app_ctx: AppContext = ctx.app_context
            
            if database_name not in app_ctx.db_pools:
                return f"Error: Database '{database_name}' not found or not connected"
            
            pool = app_ctx.db_pools[database_name]
            
            try:
                async with pool.acquire() as conn:
                    # Get all tables with their schemas
                    tables_query = """
                    SELECT 
                        schemaname,
                        tablename,
                        tableowner
                    FROM pg_tables 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY schemaname, tablename;
                    """
                    
                    tables = await conn.fetch(tables_query)
                    
                    schema_info = {
                        "database": database_name,
                        "schemas": {}
                    }
                    
                    # Group tables by schema
                    for table in tables:
                        schema_name = table['schemaname']
                        if schema_name not in schema_info["schemas"]:
                            schema_info["schemas"][schema_name] = {"tables": []}
                        
                        # Get column information for each table
                        columns_query = """
                        SELECT 
                            column_name,
                            data_type,
                            is_nullable,
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                        ORDER BY ordinal_position;
                        """
                        
                        columns = await conn.fetch(columns_query, schema_name, table['tablename'])
                        
                        table_info = {
                            "name": table['tablename'],
                            "owner": table['tableowner'],
                            "columns": [
                                {
                                    "name": col['column_name'],
                                    "type": col['data_type'],
                                    "nullable": col['is_nullable'] == 'YES',
                                    "default": col['column_default']
                                }
                                for col in columns
                            ]
                        }
                        
                        schema_info["schemas"][schema_name]["tables"].append(table_info)
                
                return json.dumps(schema_info, indent=2)
                
            except Exception as e:
                logger.error(f"Error getting schema for {database_name}: {str(e)}")
                return f"Error retrieving schema: {str(e)}"

        @self.app.tool()
        async def execute_query(
            database_name: str,
            query: str,
            limit: int = 100,
            ctx: Context = None
        ) -> str:
            """Execute a read-only SQL query on the specified database"""
            app_ctx: AppContext = ctx.app_context
            
            if database_name not in app_ctx.db_pools:
                return f"Error: Database '{database_name}' not found or not connected"
            
            # Validate query for safety
            if not self.validate_sql_query(query):
                return "Error: Query contains potentially dangerous operations. Only SELECT queries are allowed."
            
            # Ensure limit is reasonable
            limit = min(limit, 1000)
            
            pool = app_ctx.db_pools[database_name]
            
            try:
                async with pool.acquire() as conn:
                    # Execute query with timeout
                    rows = await asyncio.wait_for(
                        conn.fetch(f"SELECT * FROM ({query}) AS subquery LIMIT {limit}"),
                        timeout=30.0
                    )
                    
                    if not rows:
                        return "Query executed successfully. No results returned."
                    
                    # Convert to list of dictionaries
                    results = []
                    for row in rows:
                        results.append(dict(row))
                    
                    return json.dumps({
                        "database": database_name,
                        "query": query,
                        "row_count": len(results),
                        "limit_applied": limit,
                        "results": results
                    }, indent=2, default=str)
                    
            except asyncio.TimeoutError:
                return "Error: Query timed out (30 second limit)"
            except Exception as e:
                logger.error(f"Error executing query on {database_name}: {str(e)}")
                return f"Error executing query: {str(e)}"

        @self.app.tool()
        async def explain_query(
            database_name: str,
            query: str,
            ctx: Context = None
        ) -> str:
            """Get the execution plan for a query"""
            app_ctx: AppContext = ctx.app_context
            
            if database_name not in app_ctx.db_pools:
                return f"Error: Database '{database_name}' not found or not connected"
            
            # Validate query for safety
            if not self.validate_sql_query(query):
                return "Error: Query contains potentially dangerous operations. Only SELECT queries are allowed."
            
            pool = app_ctx.db_pools[database_name]
            
            try:
                async with pool.acquire() as conn:
                    # Get execution plan
                    explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                    result = await conn.fetchval(explain_query)
                    
                    return json.dumps({
                        "database": database_name,
                        "original_query": query,
                        "execution_plan": result
                    }, indent=2)
                    
            except Exception as e:
                logger.error(f"Error explaining query on {database_name}: {str(e)}")
                return f"Error getting execution plan: {str(e)}"

        @self.app.tool()
        async def get_table_sample(
            database_name: str,
            schema_name: str,
            table_name: str,
            sample_size: int = 10,
            ctx: Context = None
        ) -> str:
            """Get a sample of data from a specific table"""
            app_ctx: AppContext = ctx.app_context
            
            if database_name not in app_ctx.db_pools:
                return f"Error: Database '{database_name}' not found or not connected"
            
            sample_size = min(sample_size, 100)  # Limit sample size
            pool = app_ctx.db_pools[database_name]
            
            try:
                async with pool.acquire() as conn:
                    # Get table sample
                    query = f"""
                    SELECT * FROM "{schema_name}"."{table_name}"
                    ORDER BY RANDOM()
                    LIMIT {sample_size}
                    """
                    
                    rows = await conn.fetch(query)
                    
                    if not rows:
                        return f"Table {schema_name}.{table_name} is empty"
                    
                    # Convert to list of dictionaries
                    results = []
                    for row in rows:
                        results.append(dict(row))
                    
                    return json.dumps({
                        "database": database_name,
                        "schema": schema_name,
                        "table": table_name,
                        "sample_size": len(results),
                        "data": results
                    }, indent=2, default=str)
                    
            except Exception as e:
                logger.error(f"Error getting sample from {database_name}.{schema_name}.{table_name}: {str(e)}")
                return f"Error getting table sample: {str(e)}"

        @self.app.tool()
        async def search_tables(
            database_name: str,
            search_term: str,
            ctx: Context = None
        ) -> str:
            """Search for tables containing the search term in their name or columns"""
            app_ctx: AppContext = ctx.app_context
            
            if database_name not in app_ctx.db_pools:
                return f"Error: Database '{database_name}' not found or not connected"
            
            pool = app_ctx.db_pools[database_name]
            
            try:
                async with pool.acquire() as conn:
                    # Search in table names
                    table_search_query = """
                    SELECT 
                        schemaname,
                        tablename,
                        'table_name' as match_type
                    FROM pg_tables 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                    AND LOWER(tablename) LIKE LOWER($1)
                    """
                    
                    # Search in column names
                    column_search_query = """
                    SELECT 
                        table_schema as schemaname,
                        table_name as tablename,
                        column_name,
                        'column_name' as match_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    AND LOWER(column_name) LIKE LOWER($1)
                    """
                    
                    search_pattern = f"%{search_term}%"
                    
                    table_matches = await conn.fetch(table_search_query, search_pattern)
                    column_matches = await conn.fetch(column_search_query, search_pattern)
                    
                    results = {
                        "database": database_name,
                        "search_term": search_term,
                        "table_matches": [dict(match) for match in table_matches],
                        "column_matches": [dict(match) for match in column_matches]
                    }
                    
                    return json.dumps(results, indent=2)
                    
            except Exception as e:
                logger.error(f"Error searching in {database_name}: {str(e)}")
                return f"Error searching: {str(e)}"

    def run(self, host: str = "127.0.0.1", port: int = 3000):
        """Run the MCP server"""
        # Set the lifespan
        self.app._lifespan = self.app_lifespan
        
        logger.info(f"Starting PostgreSQL MCP Server on {host}:{port}")
        logger.info("Available databases will be loaded from environment variables")
        
        # Run with FastMCP's built-in server
        self.app.run(transport="http", host=host, port=port)

def main():
    """Main entry point"""
    # Load environment variables from .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("Loaded environment variables from .env file")
    except ImportError:
        logger.info("python-dotenv not installed, skipping .env file loading")
    
    server = PostgreSQLMCPServer()
    server.run()

if __name__ == "__main__":
    main()
