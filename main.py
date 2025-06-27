#!/usr/bin/env python3
"""
Multi-Database MCP Server for Claude AI
Connects to multiple DigitalOcean PostgreSQL databases
Read-only access with safety rails
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Sequence
from datetime import datetime
import asyncpg
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    TextContent,
    Tool,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages connections to multiple PostgreSQL databases"""
    
    def __init__(self):
        self.pools: Dict[str, asyncpg.Pool] = {}
        self.configs: List[Dict[str, Any]] = []
        self.max_rows = int(os.getenv('MAX_ROWS', '10000'))
        self.query_timeout = int(os.getenv('QUERY_TIMEOUT', '30'))
        
    async def initialize(self):
        """Initialize database connections from environment config"""
        try:
            # Load database configurations from environment
            db_configs_str = os.getenv('DATABASE_CONFIGS', '[]')
            self.configs = json.loads(db_configs_str)
            
            if not self.configs:
                logger.warning("No database configurations found")
                return
                
            # Create connection pools for each database
            for config in self.configs:
                db_id = config['id']
                try:
                    pool = await asyncpg.create_pool(
                        host=config['host'],
                        port=config['port'],
                        database=config['database'],
                        user=config['username'],
                        password=config['password'],
                        ssl='require',
                        min_size=1,
                        max_size=5,
                        command_timeout=self.query_timeout
                    )
                    self.pools[db_id] = pool
                    logger.info(f"Connected to database: {config['name']} ({db_id})")
                except Exception as e:
                    logger.error(f"Failed to connect to database {db_id}: {e}")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid DATABASE_CONFIGS JSON: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize databases: {e}")
    
    async def close(self):
        """Close all database connections"""
        for db_id, pool in self.pools.items():
            try:
                await pool.close()
                logger.info(f"Closed connection to database: {db_id}")
            except Exception as e:
                logger.error(f"Error closing database {db_id}: {e}")
    
    def get_database_info(self, db_id: str) -> Optional[Dict[str, Any]]:
        """Get database configuration info"""
        for config in self.configs:
            if config['id'] == db_id:
                return config
        return None
    
    def validate_query(self, query: str) -> bool:
        """Validate that query is read-only"""
        # Convert to lowercase and remove extra whitespace
        clean_query = ' '.join(query.lower().split())
        
        # Check for write operations
        write_keywords = [
            'insert', 'update', 'delete', 'drop', 'create', 'alter',
            'truncate', 'grant', 'revoke', 'commit', 'rollback'
        ]
        
        for keyword in write_keywords:
            if keyword in clean_query:
                return False
        
        # Must start with SELECT
        if not clean_query.strip().startswith('select'):
            return False
            
        return True
    
    async def execute_query(self, db_id: str, query: str) -> Dict[str, Any]:
        """Execute a read-only query on specified database"""
        if db_id not in self.pools:
            return {
                'success': False,
                'error': f'Database {db_id} not found or not connected'
            }
        
        if not self.validate_query(query):
            return {
                'success': False,
                'error': 'Only SELECT queries are allowed'
            }
        
        try:
            pool = self.pools[db_id]
            db_info = self.get_database_info(db_id)
            
            start_time = datetime.now()
            
            async with pool.acquire() as conn:
                # Execute query with row limit
                limited_query = f"SELECT * FROM ({query}) subquery LIMIT {self.max_rows}"
                rows = await conn.fetch(limited_query)
                
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Convert rows to list of dictionaries
            results = []
            for row in rows:
                results.append(dict(row))
            
            # Log the query execution
            logger.info(f"Query executed on {db_id}: {len(results)} rows, {execution_time:.2f}s")
            
            return {
                'success': True,
                'database': db_info['name'] if db_info else db_id,
                'rows_returned': len(results),
                'execution_time_seconds': execution_time,
                'data': results,
                'limited': len(rows) == self.max_rows
            }
            
        except asyncio.TimeoutError:
            logger.error(f"Query timeout on database {db_id}")
            return {
                'success': False,
                'error': f'Query timeout after {self.query_timeout} seconds'
            }
        except Exception as e:
            logger.error(f"Query execution error on {db_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Initialize database manager
db_manager = DatabaseManager()

# Create MCP server
server = Server("multi-database-mcp-server")

@server.list_tools()
async def handle_list_tools() -> ListToolsResult:
    """List available tools"""
    tools = [
        Tool(
            name="list_databases",
            description="List all available databases",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="query_database",
            description="Execute a SELECT query on a specific database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_id": {
                        "type": "string",
                        "description": "ID of the database to query"
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL SELECT query to execute"
                    }
                },
                "required": ["database_id", "query"]
            }
        ),
        Tool(
            name="get_table_schema",
            description="Get schema information for a table in a specific database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_id": {
                        "type": "string",
                        "description": "ID of the database"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    }
                },
                "required": ["database_id", "table_name"]
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in a specific database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_id": {
                        "type": "string",
                        "description": "ID of the database"
                    }
                },
                "required": ["database_id"]
            }
        )
    ]
    
    return ListToolsResult(tools=tools)

@server.call_tool()
async def handle_call_tool(request: CallToolRequest) -> CallToolResult:
    """Handle tool calls"""
    
    if request.name == "list_databases":
        databases = []
        for config in db_manager.configs:
            databases.append({
                'id': config['id'],
                'name': config['name'],
                'host': config['host'],
                'database': config['database'],
                'connected': config['id'] in db_manager.pools
            })
        
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(databases, indent=2)
                )
            ]
        )
    
    elif request.name == "query_database":
        database_id = request.arguments.get("database_id")
        query = request.arguments.get("query")
        
        if not database_id or not query:
            return CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text="Error: database_id and query are required"
                    )
                ]
            )
        
        result = await db_manager.execute_query(database_id, query)
        
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(result, indent=2, default=str)
                )
            ]
        )
    
    elif request.name == "get_table_schema":
        database_id = request.arguments.get("database_id")
        table_name = request.arguments.get("table_name")
        
        if not database_id or not table_name:
            return CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text="Error: database_id and table_name are required"
                    )
                ]
            )
        
        schema_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = $1
        ORDER BY ordinal_position
        """
        
        result = await db_manager.execute_query(
            database_id, 
            schema_query.replace('$1', f"'{table_name}'")
        )
        
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(result, indent=2, default=str)
                )
            ]
        )
    
    elif request.name == "list_tables":
        database_id = request.arguments.get("database_id")
        
        if not database_id:
            return CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text="Error: database_id is required"
                    )
                ]
            )
        
        tables_query = """
        SELECT 
            table_name,
            table_type
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
        
        result = await db_manager.execute_query(database_id, tables_query)
        
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(result, indent=2, default=str)
                )
            ]
        )
    
    else:
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=f"Unknown tool: {request.name}"
                )
            ]
        )

async def main():
    """Main server function"""
    try:
        # Initialize database connections
        await db_manager.initialize()
        
        # Start the server
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="multi-database-mcp-server",
                    server_version="1.0.0",
                    capabilities=server.get_capabilities(
                        notification_options=None,
                        experimental_capabilities=None,
                    ),
                ),
            )
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)
    finally:
        await db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
