
"""
Claude AI MCP Server for Multi-Database PostgreSQL
Enables Claude to query multiple DigitalOcean PostgreSQL databases safely via MCP protocol.
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
@dataclass
class DatabaseConfig:
    """Configuration for a single database connection"""
    name: str
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_mode: str = "require"
    max_connections: int = 10
    min_connections: int = 2
    
    @classmethod
    def from_url(cls, name: str, url: str) -> 'DatabaseConfig':
        """Create DatabaseConfig from PostgreSQL URL"""
        parsed = urlparse(url)
        
        return cls(
            name=name,
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path.lstrip('/'),
            username=parsed.username,
            password=parsed.password,
            ssl_mode="require"  # Always use SSL for DigitalOcean
        )

class MCPConfig:
    """MCP Server configuration"""
    def __init__(self):
        self.bearer_token = os.getenv("MCP_BEARER_TOKEN", "your-secure-token-here")
        self.max_query_time = int(os.getenv("MAX_QUERY_TIME", "30"))  # seconds
        self.max_rows = int(os.getenv("MAX_ROWS", "1000"))
        self.allowed_operations = set(os.getenv("ALLOWED_OPERATIONS", "SELECT").split(","))
        self.databases = self._load_database_configs()
    
    def _load_database_configs(self) -> List[DatabaseConfig]:
        """Load database configurations from environment variables"""
        configs = []
        
        # Loop through all environment variables looking for POSTGRES_URL_* pattern
        for key, value in os.environ.items():
            if key.startswith("POSTGRES_URL_"):
                # Extract database name from environment variable name
                # POSTGRES_URL_DODB -> dodb
                # POSTGRES_URL_BJB_INTAKES -> bjb_intakes
                db_name = key.replace("POSTGRES_URL_", "").lower()
                
                try:
                    config = DatabaseConfig.from_url(db_name, value)
                    configs.append(config)
                    logger.info(f"Loaded database config for: {db_name}")
                except Exception as e:
                    logger.error(f"Failed to parse database URL for {db_name}: {e}")
                    continue
        
        if not configs:
            logger.warning("No POSTGRES_URL_* environment variables found!")
            # Create a sample config to prevent startup errors
            configs.append(DatabaseConfig(
                name="example_db",
                host="localhost",
                port=5432,
                database="postgres",
                username="postgres",
                password="password"
            ))
        
        logger.info(f"Loaded {len(configs)} database configurations")
        return configs

# Global configuration
config = MCPConfig()

# Database Connection Pool Manager
class DatabaseManager:
    """Manages multiple database connection pools"""
    
    def __init__(self, db_configs: List[DatabaseConfig]):
        self.db_configs = {db.name: db for db in db_configs}
        self.pools: Dict[str, asyncpg.Pool] = {}
    
    async def initialize(self):
        """Initialize all database connection pools"""
        for db_name, db_config in self.db_configs.items():
            try:
                dsn = f"postgresql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}?sslmode={db_config.ssl_mode}"
                
                pool = await asyncpg.create_pool(
                    dsn,
                    min_size=db_config.min_connections,
                    max_size=db_config.max_connections,
                    command_timeout=config.max_query_time
                )
                
                self.pools[db_name] = pool
                logger.info(f"Initialized connection pool for database: {db_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize database {db_name}: {e}")
                raise
    
    async def close(self):
        """Close all database connection pools"""
        for db_name, pool in self.pools.items():
            await pool.close()
            logger.info(f"Closed connection pool for database: {db_name}")
    
    def get_pool(self, db_name: str) -> asyncpg.Pool:
        """Get connection pool for a specific database"""
        if db_name not in self.pools:
            raise ValueError(f"Database '{db_name}' not configured")
        return self.pools[db_name]
    
    def list_databases(self) -> List[str]:
        """List all configured database names"""
        return list(self.pools.keys())

# Global database manager
db_manager = DatabaseManager(config.databases)

# Pydantic models for API
class QueryRequest(BaseModel):
    database: str = Field(..., description="Database name to query")
    query: str = Field(..., description="SQL query to execute", max_length=10000)
    parameters: Optional[List[Any]] = Field(default=None, description="Query parameters")

class QueryResult(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    row_count: Optional[int] = None
    execution_time: Optional[float] = None
    error: Optional[str] = None

class SchemaInfo(BaseModel):
    database: str
    tables: List[Dict[str, Any]]

class MCPToolCall(BaseModel):
    name: str
    arguments: Dict[str, Any]

class MCPResponse(BaseModel):
    content: List[Dict[str, Any]]
    isError: Optional[bool] = False

# Security
security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify Bearer token"""
    if credentials.credentials != config.bearer_token:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )
    return credentials

# Query Safety Checks
class QueryValidator:
    """Validates SQL queries for safety"""
    
    DANGEROUS_KEYWORDS = {
        'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE',
        'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'DECLARE', 'EXEC'
    }
    
    @classmethod
    def is_query_safe(cls, query: str, allowed_operations: set) -> Tuple[bool, str]:
        """Check if query is safe to execute"""
        query_upper = query.upper().strip()
        
        # Check for dangerous keywords
        for keyword in cls.DANGEROUS_KEYWORDS:
            if keyword not in allowed_operations and keyword in query_upper:
                return False, f"Operation '{keyword}' is not allowed"
        
        # Check for common SQL injection patterns
        suspicious_patterns = [
            ';--', '/*', '*/', 'xp_', 'sp_', 'UNION SELECT', 'OR 1=1', "' OR '1'='1"
        ]
        
        for pattern in suspicious_patterns:
            if pattern.upper() in query_upper:
                return False, f"Potentially dangerous pattern detected: {pattern}"
        
        return True, "Query appears safe"

# Database Operations
class DatabaseOperations:
    """Database operation handlers"""
    
    @staticmethod
    async def execute_query(
        db_name: str, 
        query: str, 
        parameters: Optional[List[Any]] = None
    ) -> QueryResult:
        """Execute a SQL query safely"""
        start_time = time.time()
        
        try:
            # Validate query
            is_safe, safety_message = QueryValidator.is_query_safe(query, config.allowed_operations)
            if not is_safe:
                return QueryResult(success=False, error=f"Query rejected: {safety_message}")
            
            # Get database pool
            pool = db_manager.get_pool(db_name)
            
            async with pool.acquire() as conn:
                # Set query timeout
                await conn.execute(f"SET statement_timeout = '{config.max_query_time}s'")
                
                # Execute query
                if parameters:
                    rows = await conn.fetch(query, *parameters)
                else:
                    rows = await conn.fetch(query)
                
                # Limit rows
                if len(rows) > config.max_rows:
                    rows = rows[:config.max_rows]
                    logger.warning(f"Query returned more than {config.max_rows} rows, truncated")
                
                # Convert to dict format
                columns = list(rows[0].keys()) if rows else []
                data = [dict(row) for row in rows]
                
                execution_time = time.time() - start_time
                
                logger.info(f"Query executed successfully on {db_name}: {len(data)} rows in {execution_time:.2f}s")
                
                return QueryResult(
                    success=True,
                    data=data,
                    columns=columns,
                    row_count=len(data),
                    execution_time=execution_time
                )
                
        except asyncio.TimeoutError:
            return QueryResult(success=False, error="Query timeout exceeded")
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return QueryResult(success=False, error=str(e))
    
    @staticmethod
    async def get_schema_info(db_name: str) -> SchemaInfo:
        """Get database schema information"""
        schema_query = """
        SELECT 
            t.table_name,
            t.table_type,
            array_agg(
                json_build_object(
                    'column_name', c.column_name,
                    'data_type', c.data_type,
                    'is_nullable', c.is_nullable,
                    'column_default', c.column_default
                ) ORDER BY c.ordinal_position
            ) as columns
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = 'public'
        GROUP BY t.table_name, t.table_type
        ORDER BY t.table_name;
        """
        
        try:
            result = await DatabaseOperations.execute_query(db_name, schema_query)
            if result.success:
                return SchemaInfo(database=db_name, tables=result.data or [])
            else:
                raise Exception(result.error)
        except Exception as e:
            logger.error(f"Failed to get schema info for {db_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get schema info: {e}")

# FastAPI Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    await db_manager.initialize()
    logger.info("MCP Server started successfully")
    yield
    # Shutdown
    await db_manager.close()
    logger.info("MCP Server shutdown complete")

app = FastAPI(
    title="Claude AI MCP PostgreSQL Server",
    description="Multi-database PostgreSQL MCP server for Claude AI",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routes
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Claude AI MCP PostgreSQL Server",
        "status": "healthy",
        "databases": db_manager.list_databases(),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/databases")
async def list_databases(credentials: HTTPAuthorizationCredentials = Depends(verify_token)):
    """List all configured databases"""
    return {"databases": db_manager.list_databases()}

@app.get("/schema/{db_name}")
async def get_database_schema(
    db_name: str,
    credentials: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """Get schema information for a specific database"""
    return await DatabaseOperations.get_schema_info(db_name)

@app.post("/query")
async def execute_query(
    request: QueryRequest,
    credentials: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """Execute a SQL query"""
    return await DatabaseOperations.execute_query(
        request.database,
        request.query,
        request.parameters
    )

# MCP Protocol Endpoints
@app.post("/mcp/tools/call")
async def mcp_tool_call(
    tool_call: MCPToolCall,
    credentials: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """Handle MCP tool calls from Claude"""
    try:
        if tool_call.name == "fetch_data":
            # Handle data fetching
            db_name = tool_call.arguments.get("database")
            query = tool_call.arguments.get("query")
            
            if not db_name or not query:
                return MCPResponse(
                    content=[{"type": "text", "text": "Missing required parameters: database and query"}],
                    isError=True
                )
            
            result = await DatabaseOperations.execute_query(db_name, query)
            
            if result.success:
                # Format response for Claude
                response_text = f"Query executed successfully on {db_name}:\n"
                response_text += f"Returned {result.row_count} rows in {result.execution_time:.2f} seconds\n\n"
                
                if result.data:
                    # Convert to readable format
                    response_text += "Results:\n"
                    for i, row in enumerate(result.data[:10]):  # Show first 10 rows
                        response_text += f"Row {i+1}: {row}\n"
                    
                    if result.row_count > 10:
                        response_text += f"... and {result.row_count - 10} more rows\n"
                
                return MCPResponse(content=[{"type": "text", "text": response_text}])
            else:
                return MCPResponse(
                    content=[{"type": "text", "text": f"Query failed: {result.error}"}],
                    isError=True
                )
        
        elif tool_call.name == "get_schema":
            # Handle schema requests
            db_name = tool_call.arguments.get("database")
            
            if not db_name:
                return MCPResponse(
                    content=[{"type": "text", "text": "Missing required parameter: database"}],
                    isError=True
                )
            
            schema_info = await DatabaseOperations.get_schema_info(db_name)
            
            response_text = f"Schema for database '{db_name}':\n\n"
            for table in schema_info.tables:
                response_text += f"Table: {table['table_name']} ({table['table_type']})\n"
                for column in table['columns']:
                    response_text += f"  - {column['column_name']}: {column['data_type']}"
                    if column['is_nullable'] == 'NO':
                        response_text += " (NOT NULL)"
                    response_text += "\n"
                response_text += "\n"
            
            return MCPResponse(content=[{"type": "text", "text": response_text}])
        
        else:
            return MCPResponse(
                content=[{"type": "text", "text": f"Unknown tool: {tool_call.name}"}],
                isError=True
            )
    
    except Exception as e:
        logger.error(f"MCP tool call failed: {e}")
        return MCPResponse(
            content=[{"type": "text", "text": f"Internal error: {str(e)}"}],
            isError=True
        )

@app.get("/mcp/tools")
async def mcp_list_tools(credentials: HTTPAuthorizationCredentials = Depends(verify_token)):
    """List available MCP tools for Claude"""
    return {
        "tools": [
            {
                "name": "fetch_data",
                "description": "Execute SQL queries against configured databases",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name to query",
                            "enum": db_manager.list_databases()
                        },
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute"
                        }
                    },
                    "required": ["database", "query"]
                }
            },
            {
                "name": "get_schema",
                "description": "Get database schema information",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name",
                            "enum": db_manager.list_databases()
                        }
                    },
                    "required": ["database"]
                }
            }
        ]
    }

# Main entry point
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info"
    )
