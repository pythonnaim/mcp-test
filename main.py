
"""
Claude-Compatible PostgreSQL Web API Server for Render
Hosts your PostgreSQL databases as a web API that Claude can access
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from starlette.status import HTTP_401_UNAUTHORIZED
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration from URL"""
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        parsed = urlparse(url)
        self.host = parsed.hostname
        self.port = parsed.port or 5432
        self.database = parsed.path.lstrip('/')
        self.username = parsed.username
        self.password = parsed.password

class DatabaseManager:
    """Manages multiple database connection pools"""
    
    def __init__(self):
        self.databases = self._load_database_configs()
        self.pools: Dict[str, asyncpg.Pool] = {}
        self.max_query_time = int(os.getenv("MAX_QUERY_TIME", "30"))
        self.max_rows = int(os.getenv("MAX_ROWS", "1000"))
        self.allowed_operations = set(os.getenv("ALLOWED_OPERATIONS", "SELECT").split(","))
    
    def _load_database_configs(self) -> List[DatabaseConfig]:
        """Load database configurations from environment variables"""
        configs = []
        
        for key, value in os.environ.items():
            if key.startswith("POSTGRES_URL_"):
                db_name = key.replace("POSTGRES_URL_", "").lower()
                try:
                    config = DatabaseConfig(db_name, value)
                    configs.append(config)
                    logger.info(f"Loaded database config: {db_name}")
                except Exception as e:
                    logger.error(f"Failed to parse database URL for {db_name}: {e}")
        
        if not configs:
            logger.error("No POSTGRES_URL_* environment variables found!")
            raise ValueError("No database configurations found")
        
        return configs
    
    async def initialize(self):
        """Initialize database connection pools"""
        for db_config in self.databases:
            try:
                pool = await asyncpg.create_pool(
                    db_config.url,
                    min_size=2,
                    max_size=10,
                    command_timeout=self.max_query_time
                )
                self.pools[db_config.name] = pool
                logger.info(f"Initialized pool for database: {db_config.name}")
            except Exception as e:
                logger.error(f"Failed to initialize pool for {db_config.name}: {e}")
                raise
    
    async def close(self):
        """Close all database pools"""
        for name, pool in self.pools.items():
            await pool.close()
            logger.info(f"Closed pool for database: {name}")
    
    def get_pool(self, db_name: str) -> asyncpg.Pool:
        """Get connection pool for a specific database"""
        if db_name not in self.pools:
            raise ValueError(f"Database '{db_name}' not configured")
        return self.pools[db_name]
    
    def list_databases(self) -> List[str]:
        """List all configured database names"""
        return list(self.pools.keys())
    
    def _is_query_safe(self, query: str) -> tuple[bool, str]:
        """Check if query is safe to execute"""
        query_upper = query.upper().strip()
        
        # Check for dangerous keywords
        dangerous_keywords = {
            'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE',
            'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'DECLARE', 'EXEC'
        }
        
        for keyword in dangerous_keywords:
            if keyword not in self.allowed_operations and keyword in query_upper:
                return False, f"Operation '{keyword}' is not allowed"
        
        # Check for suspicious patterns
        suspicious_patterns = [';--', '/*', '*/', 'UNION SELECT', 'OR 1=1']
        for pattern in suspicious_patterns:
            if pattern.upper() in query_upper:
                return False, f"Potentially dangerous pattern detected: {pattern}"
        
        return True, "Query appears safe"
    
    async def execute_query(self, db_name: str, query: str) -> Dict[str, Any]:
        """Execute a SQL query safely"""
        start_time = time.time()
        
        try:
            # Validate query
            is_safe, safety_message = self._is_query_safe(query)
            if not is_safe:
                return {
                    "success": False,
                    "error": f"Query rejected: {safety_message}",
                    "data": None
                }
            
            # Get database pool
            pool = self.get_pool(db_name)
            
            async with pool.acquire() as conn:
                # Set query timeout
                await conn.execute(f"SET statement_timeout = '{self.max_query_time}s'")
                
                # Execute query
                rows = await conn.fetch(query)
                
                # Limit rows
                if len(rows) > self.max_rows:
                    rows = rows[:self.max_rows]
                    logger.warning(f"Query returned more than {self.max_rows} rows, truncated")
                
                # Convert to dict format
                columns = list(rows[0].keys()) if rows else []
                data = [dict(row) for row in rows]
                
                execution_time = time.time() - start_time
                
                logger.info(f"Query executed successfully on {db_name}: {len(data)} rows in {execution_time:.2f}s")
                
                return {
                    "success": True,
                    "data": data,
                    "columns": columns,
                    "row_count": len(data),
                    "execution_time": execution_time,
                    "database": db_name,
                    "query": query[:100] + "..." if len(query) > 100 else query
                }
                
        except asyncio.TimeoutError:
            return {"success": False, "error": "Query timeout exceeded", "data": None}
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {"success": False, "error": str(e), "data": None}
    
    async def get_schema_info(self, db_name: str) -> Dict[str, Any]:
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
            result = await self.execute_query(db_name, schema_query)
            if result["success"]:
                return {
                    "success": True,
                    "database": db_name,
                    "tables": result["data"] or []
                }
            else:
                return {
                    "success": False,
                    "error": result["error"],
                    "database": db_name
                }
        except Exception as e:
            logger.error(f"Failed to get schema info for {db_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "database": db_name
            }

# Global database manager
db_manager = DatabaseManager()

# Pydantic models
class QueryRequest(BaseModel):
    database: str = Field(..., description="Database name to query")
    query: str = Field(..., description="SQL query to execute", max_length=10000)

class ClaudeRequest(BaseModel):
    message: str = Field(..., description="Natural language request")
    database: Optional[str] = Field(None, description="Specific database to query (optional)")

# Security
security = HTTPBearer()
API_KEY = os.getenv("API_KEY", "your-secure-api-key-change-this")

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key"""
    if credentials.credentials != API_KEY:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return credentials

# FastAPI Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    await db_manager.initialize()
    logger.info("PostgreSQL API Server started successfully")
    yield
    # Shutdown
    await db_manager.close()
    logger.info("PostgreSQL API Server shutdown complete")

app = FastAPI(
    title="Claude PostgreSQL API Server",
    description="Multi-database PostgreSQL API server for Claude AI",
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
        "service": "Claude PostgreSQL API Server",
        "status": "healthy",
        "databases": db_manager.list_databases(),
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

@app.get("/health")
async def health():
    """Health check for Render"""
    return {"status": "healthy"}

@app.get("/databases")
async def list_databases(credentials: HTTPAuthorizationCredentials = Depends(verify_api_key)):
    """List all configured databases"""
    return {
        "databases": [
            {
                "name": db.name,
                "database": db.database,
                "host": db.host
            } for db in db_manager.databases
        ]
    }

@app.get("/schema/{db_name}")
async def get_database_schema(
    db_name: str,
    credentials: HTTPAuthorizationCredentials = Depends(verify_api_key)
):
    """Get schema information for a specific database"""
    return await db_manager.get_schema_info(db_name)

@app.post("/query")
async def execute_query(
    request: QueryRequest,
    credentials: HTTPAuthorizationCredentials = Depends(verify_api_key)
):
    """Execute a SQL query"""
    return await db_manager.execute_query(request.database, request.query)

@app.post("/claude")
async def claude_endpoint(
    request: ClaudeRequest,
    credentials: HTTPAuthorizationCredentials = Depends(verify_api_key)
):
    """
    Claude-friendly endpoint for natural language database queries
    This endpoint helps Claude understand how to interact with your databases
    """
    message = request.message.lower()
    
    # Simple natural language processing
    if "list" in message and "database" in message:
        return {
            "response": "Available databases: " + ", ".join(db_manager.list_databases()),
            "type": "database_list"
        }
    
    elif "schema" in message or "structure" in message or "tables" in message:
        # Try to extract database name
        db_name = request.database
        if not db_name:
            # Try to find database name in message
            for db in db_manager.list_databases():
                if db in message:
                    db_name = db
                    break
        
        if db_name:
            schema_info = await db_manager.get_schema_info(db_name)
            return {
                "response": f"Schema information for {db_name}",
                "data": schema_info,
                "type": "schema_info"
            }
        else:
            return {
                "response": "Please specify which database schema you want to see",
                "available_databases": db_manager.list_databases(),
                "type": "clarification_needed"
            }
    
    else:
        return {
            "response": "I can help you with database operations. Try asking about:\n- List databases\n- Show schema for [database_name]\n- Or use the /query endpoint for direct SQL",
            "available_endpoints": [
                {"endpoint": "/databases", "description": "List all databases"},
                {"endpoint": "/schema/{db_name}", "description": "Get database schema"},
                {"endpoint": "/query", "description": "Execute SQL query"}
            ],
            "type": "help"
        }

# Get port from environment variable (required for Render)
port = int(os.getenv("PORT", 8000))

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
