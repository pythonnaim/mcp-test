import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
import asyncpg
from contextlib import asynccontextmanager
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgreSQLMCPServer:
    """
    MCP Server for PostgreSQL database interactions with DigitalOcean hosted databases.
    Implements fetch_data and execute_query methods with safety rails.
    """
    
    def __init__(self, 
                 host: str,
                 port: int = 25060,  # DigitalOcean default SSL port
                 database: str = None,
                 user: str = "svc_readonly",
                 password: str = None,
                 ssl: str = "require",
                 max_rows: int = 1000,
                 query_timeout: int = 30):
        """
        Initialize the PostgreSQL MCP Server
        
        Args:
            host: DigitalOcean PostgreSQL cluster hostname
            port: Database port (default 25060 for SSL)
            database: Database name
            user: Service role username (default: svc_readonly)
            password: Service role password
            ssl: SSL mode (default: require)
            max_rows: Maximum rows to return (safety rail)
            query_timeout: Query timeout in seconds (safety rail)
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.ssl = ssl
        self.max_rows = max_rows
        self.query_timeout = query_timeout
        self.connection_pool = None
    
    async def initialize_pool(self):
        """Initialize the connection pool"""
        try:
            self.connection_pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                ssl=self.ssl,
                min_size=1,
                max_size=10,
                command_timeout=self.query_timeout
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool with proper cleanup"""
        if not self.connection_pool:
            await self.initialize_pool()
        
        connection = await self.connection_pool.acquire()
        try:
            yield connection
        finally:
            await self.connection_pool.release(connection)
    
    async def fetch_data(self, 
                        query: str, 
                        parameters: Optional[List] = None,
                        limit_override: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch data from PostgreSQL database (READ-ONLY operations)
        
        This method is designed for SELECT queries and data retrieval.
        It includes safety rails to prevent resource exhaustion.
        
        Args:
            query: SQL SELECT query to execute
            parameters: Optional query parameters for prepared statements
            limit_override: Override default row limit for this query
            
        Returns:
            Dict containing:
            - success: bool
            - data: List of dictionaries (rows)
            - columns: List of column names
            - row_count: Number of rows returned
            - execution_time: Query execution time in seconds
            - message: Status message
        """
        start_time = time.time()
        
        # Validate query is read-only
        query_upper = query.strip().upper()
        if not query_upper.startswith(('SELECT', 'WITH', 'SHOW', 'EXPLAIN')):
            return {
                "success": False,
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time": 0,
                "message": "fetch_data only supports read-only operations (SELECT, WITH, SHOW, EXPLAIN)"
            }
        
        # Apply row limit safety rail
        row_limit = limit_override if limit_override is not None else self.max_rows
        if 'LIMIT' not in query_upper:
            query = f"{query.rstrip(';')} LIMIT {row_limit}"
        
        try:
            async with self.get_connection() as conn:
                # Execute query with timeout
                if parameters:
                    rows = await asyncio.wait_for(
                        conn.fetch(query, *parameters),
                        timeout=self.query_timeout
                    )
                else:
                    rows = await asyncio.wait_for(
                        conn.fetch(query),
                        timeout=self.query_timeout
                    )
                
                # Convert rows to list of dictionaries
                data = [dict(row) for row in rows]
                columns = list(rows[0].keys()) if rows else []
                
                execution_time = time.time() - start_time
                
                # Log the query for auditing
                logger.info(f"fetch_data executed successfully: {len(data)} rows in {execution_time:.3f}s")
                
                return {
                    "success": True,
                    "data": data,
                    "columns": columns,
                    "row_count": len(data),
                    "execution_time": execution_time,
                    "message": f"Successfully fetched {len(data)} rows"
                }
                
        except asyncio.TimeoutError:
            return {
                "success": False,
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time": time.time() - start_time,
                "message": f"Query timed out after {self.query_timeout} seconds"
            }
        except Exception as e:
            logger.error(f"fetch_data error: {e}")
            return {
                "success": False,
                "data": [],
                "columns": [],
                "row_count": 0,
                "execution_time": time.time() - start_time,
                "message": f"Query failed: {str(e)}"
            }
    
    async def execute_query(self, 
                           query: str, 
                           parameters: Optional[List] = None,
                           allow_writes: bool = False) -> Dict[str, Any]:
        """
        Execute any SQL query including writes (INSERT, UPDATE, DELETE, DDL)
        
        This method can handle both read and write operations but includes
        additional safety measures for write operations.
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters for prepared statements
            allow_writes: Must be True to execute write operations
            
        Returns:
            Dict containing:
            - success: bool
            - affected_rows: Number of rows affected (for writes)
            - data: List of dictionaries (for SELECT queries)
            - columns: List of column names (for SELECT queries)
            - execution_time: Query execution time in seconds
            - message: Status message
            - operation_type: Type of SQL operation
        """
        start_time = time.time()
        
        # Determine operation type
        query_upper = query.strip().upper()
        operation_type = query_upper.split()[0]
        
        # Check for write operations
        write_operations = {'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE'}
        is_write = operation_type in write_operations
        
        if is_write and not allow_writes:
            return {
                "success": False,
                "affected_rows": 0,
                "data": [],
                "columns": [],
                "execution_time": 0,
                "message": "Write operations require allow_writes=True parameter",
                "operation_type": operation_type
            }
        
        try:
            async with self.get_connection() as conn:
                if operation_type == 'SELECT' or query_upper.startswith(('WITH', 'SHOW', 'EXPLAIN')):
                    # Handle SELECT queries (return data)
                    if parameters:
                        rows = await asyncio.wait_for(
                            conn.fetch(query, *parameters),
                            timeout=self.query_timeout
                        )
                    else:
                        rows = await asyncio.wait_for(
                            conn.fetch(query),
                            timeout=self.query_timeout
                        )
                    
                    data = [dict(row) for row in rows]
                    columns = list(rows[0].keys()) if rows else []
                    
                    execution_time = time.time() - start_time
                    logger.info(f"execute_query (SELECT) completed: {len(data)} rows in {execution_time:.3f}s")
                    
                    return {
                        "success": True,
                        "affected_rows": len(data),
                        "data": data,
                        "columns": columns,
                        "execution_time": execution_time,
                        "message": f"Query returned {len(data)} rows",
                        "operation_type": operation_type
                    }
                    
                else:
                    # Handle write operations (return affected row count)
                    if parameters:
                        result = await asyncio.wait_for(
                            conn.execute(query, *parameters),
                            timeout=self.query_timeout
                        )
                    else:
                        result = await asyncio.wait_for(
                            conn.execute(query),
                            timeout=self.query_timeout
                        )
                    
                    # Extract affected row count from result
                    affected_rows = 0
                    if isinstance(result, str) and result.startswith(('INSERT', 'UPDATE', 'DELETE')):
                        # Parse result string like "INSERT 0 5" or "UPDATE 3"
                        parts = result.split()
                        if len(parts) >= 2:
                            affected_rows = int(parts[-1])
                    
                    execution_time = time.time() - start_time
                    logger.info(f"execute_query ({operation_type}) completed: {affected_rows} rows affected in {execution_time:.3f}s")
                    
                    return {
                        "success": True,
                        "affected_rows": affected_rows,
                        "data": [],
                        "columns": [],
                        "execution_time": execution_time,
                        "message": f"{operation_type} completed successfully, {affected_rows} rows affected",
                        "operation_type": operation_type
                    }
                    
        except asyncio.TimeoutError:
            return {
                "success": False,
                "affected_rows": 0,
                "data": [],
                "columns": [],
                "execution_time": time.time() - start_time,
                "message": f"Query timed out after {self.query_timeout} seconds",
                "operation_type": operation_type
            }
        except Exception as e:
            logger.error(f"execute_query error: {e}")
            return {
                "success": False,
                "affected_rows": 0,
                "data": [],
                "columns": [],
                "execution_time": time.time() - start_time,
                "message": f"Query failed: {str(e)}",
                "operation_type": operation_type
            }
    
    async def close(self):
        """Close the connection pool"""
        if self.connection_pool:
            await self.connection_pool.close()
            logger.info("Database connection pool closed")

# Example usage and testing
async def example_usage():
    """Example of how to use the PostgreSQL MCP Server"""
    
    # Initialize the server (replace with your actual DigitalOcean credentials)
    server = PostgreSQLMCPServer(
        host="your-cluster-host.db.ondigitalocean.com",
        database="your_database_name",
        user="svc_readonly",
        password="your_service_role_password",
        max_rows=100,
        query_timeout=30
    )
    
    try:
        # Example 1: Fetch top customers by MRR (Monthly Recurring Revenue)
        result = await server.fetch_data("""
            SELECT 
                customer_id,
                customer_name,
                monthly_revenue as mrr,
                signup_date
            FROM customers 
            WHERE status = 'active'
            ORDER BY monthly_revenue DESC
        """)
        
        if result["success"]:
            print(f"Top customers: {result['row_count']} rows returned")
            for row in result["data"][:5]:  # Show first 5
                print(f"  {row}")
        else:
            print(f"Query failed: {result['message']}")
        
        # Example 2: Execute a more complex query with parameters
        result = await server.execute_query("""
            SELECT 
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as new_signups,
                SUM(monthly_revenue) as total_mrr
            FROM customers 
            WHERE created_at >= $1
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month DESC
        """, parameters=["2024-01-01"])
        
        if result["success"]:
            print(f"Monthly signups: {result['affected_rows']} rows returned")
            for row in result["data"]:
                print(f"  {row}")
        
        # Example 3: Attempt a write operation (will fail without allow_writes=True)
        result = await server.execute_query("""
            UPDATE customers 
            SET last_login = NOW() 
            WHERE customer_id = $1
        """, parameters=[123])
        
        print(f"Write attempt result: {result['message']}")
        
        # Example 4: Execute write operation with permission
        result = await server.execute_query("""
            UPDATE customers 
            SET last_login = NOW() 
            WHERE customer_id = $1
        """, parameters=[123], allow_writes=True)
        
        print(f"Write operation result: {result['message']}")
        
    finally:
        await server.close()

# Configuration for multi-database gateway pattern
class MultiDatabaseMCPServer:
    """
    Multi-database gateway that can handle multiple PostgreSQL databases
    """
    
    def __init__(self):
        self.databases = {}
    
    def add_database(self, db_name: str, config: Dict[str, Any]):
        """Add a database configuration"""
        self.databases[db_name] = PostgreSQLMCPServer(**config)
    
    async def fetch_data(self, db_name: str, query: str, parameters: Optional[List] = None):
        """Fetch data from a specific database"""
        if db_name not in self.databases:
            return {
                "success": False,
                "message": f"Database '{db_name}' not configured"
            }
        
        return await self.databases[db_name].fetch_data(query, parameters)
    
    async def execute_query(self, db_name: str, query: str, parameters: Optional[List] = None, allow_writes: bool = False):
        """Execute query on a specific database"""
        if db_name not in self.databases:
            return {
                "success": False,
                "message": f"Database '{db_name}' not configured"
            }
        
        return await self.databases[db_name].execute_query(query, parameters, allow_writes)
    
    async def close_all(self):
        """Close all database connections"""
        for db in self.databases.values():
            await db.close()

if __name__ == "__main__":
    # Run the example
    asyncio.run(example_usage())