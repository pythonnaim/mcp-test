
"""
Complete MCP HTTP Server for Claude Web Integration
Includes your database manager code integrated directly
"""

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Your existing database manager code (copied directly)
class DatabaseConfig:
    """Database configuration from URL"""
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        # Add SSL if not present
        if '?' not in url:
            self.url += '?sslmode=require'
        elif 'sslmode' not in url:
            self.url += '&sslmode=require'
        
        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.port = parsed.port or 5432
        self.database = parsed.path.lstrip('/')
        self.username = parsed.username
        self.password = parsed.password

class PersistentDatabaseManager:
    """Manages persistent single connections to multiple databases"""
    
    def __init__(self):
        self.databases = self._load_database_configs()
        self.connections: Dict[str, asyncpg.Connection] = {}
        self.connection_status: Dict[str, Dict[str, Any]] = {}
        self.max_query_time = int(os.getenv("MAX_QUERY_TIME", "30"))
        self.max_rows = int(os.getenv("MAX_ROWS", "1000"))
        self.allowed_operations = set(os.getenv("ALLOWED_OPERATIONS", "SELECT").split(","))
        self.reconnect_attempts = 3
        self.reconnect_delay = 5  # seconds
        
        # Start background task for connection monitoring
        self.monitor_task = None
    
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
    
    async def _create_persistent_connection(self, db_config: DatabaseConfig) -> Optional[asyncpg.Connection]:
        """Create a persistent connection to a database"""
        for attempt in range(self.reconnect_attempts):
            try:
                logger.info(f"Connecting to {db_config.name} (attempt {attempt + 1}/{self.reconnect_attempts})")
                
                connection = await asyncpg.connect(
                    db_config.url,
                    command_timeout=self.max_query_time,
                    server_settings={
                        'application_name': f'mcp_postgres_server_{db_config.name}'
                    }
                )
                
                # Test the connection
                await connection.fetchval("SELECT 1")
                
                # Set connection parameters
                await connection.execute(f"SET statement_timeout = '{self.max_query_time}s'")
                await connection.execute("SET idle_in_transaction_session_timeout = '10min'")
                
                logger.info(f"✅ Successfully connected to {db_config.name}")
                
                self.connection_status[db_config.name] = {
                    "status": "connected",
                    "connected_at": datetime.utcnow().isoformat(),
                    "last_used": datetime.utcnow().isoformat(),
                    "reconnect_count": attempt,
                    "error": None
                }
                
                return connection
                
            except Exception as e:
                error_msg = f"Failed to connect to {db_config.name}: {str(e)}"
                logger.error(error_msg)
                
                self.connection_status[db_config.name] = {
                    "status": "failed",
                    "error": str(e),
                    "last_attempt": datetime.utcnow().isoformat(),
                    "attempt_count": attempt + 1
                }
                
                if attempt < self.reconnect_attempts - 1:
                    await asyncio.sleep(self.reconnect_delay)
        
        return None
    
    async def initialize(self):
        """Initialize all persistent database connections"""
        logger.info("Initializing persistent database connections...")
        
        connection_tasks = []
        for db_config in self.databases:
            task = self._create_persistent_connection(db_config)
            connection_tasks.append((db_config.name, task))
        
        # Connect to all databases concurrently
        for db_name, task in connection_tasks:
            try:
                connection = await task
                if connection:
                    self.connections[db_name] = connection
                    logger.info(f"✅ {db_name}: Connection established and ready")
                else:
                    logger.error(f"❌ {db_name}: Failed to establish connection")
            except Exception as e:
                logger.error(f"❌ {db_name}: Connection error: {e}")
        
        connected_count = len(self.connections)
        total_count = len(self.databases)
        
        logger.info(f"Database initialization complete: {connected_count}/{total_count} connections established")
        
        # Start connection monitoring
        if connected_count > 0:
            self.monitor_task = asyncio.create_task(self._monitor_connections())
        
        return connected_count, total_count - connected_count
    
    async def _monitor_connections(self):
        """Background task to monitor and maintain connections"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._health_check_connections()
            except asyncio.CancelledError:
                logger.info("Connection monitoring stopped")
                break
            except Exception as e:
                logger.error(f"Connection monitoring error: {e}")
    
    async def _health_check_connections(self):
        """Health check for all connections"""
        for db_name, connection in list(self.connections.items()):
            try:
                # Simple health check
                await connection.fetchval("SELECT 1")
                self.connection_status[db_name]["last_health_check"] = datetime.utcnow().isoformat()
            except Exception as e:
                logger.warning(f"Health check failed for {db_name}: {e}")
                # Try to reconnect
                await self._reconnect_database(db_name)
    
    async def _reconnect_database(self, db_name: str):
        """Reconnect to a specific database"""
        logger.info(f"Attempting to reconnect to {db_name}")
        
        # Close existing connection if it exists
        if db_name in self.connections:
            try:
                await self.connections[db_name].close()
            except:
                pass
            del self.connections[db_name]
        
        # Find the database config
        db_config = next((db for db in self.databases if db.name == db_name), None)
        if db_config:
            connection = await self._create_persistent_connection(db_config)
            if connection:
                self.connections[db_name] = connection
                logger.info(f"✅ Successfully reconnected to {db_name}")
                return True
        
        logger.error(f"❌ Failed to reconnect to {db_name}")
        return False
    
    async def close(self):
        """Close all database connections"""
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        for name, connection in self.connections.items():
            try:
                await connection.close()
                logger.info(f"Closed connection for database: {name}")
            except Exception as e:
                logger.error(f"Error closing connection for {name}: {e}")
        
        self.connections.clear()
        self.connection_status.clear()
    
    def get_connection(self, db_name: str) -> asyncpg.Connection:
        """Get connection for a specific database"""
        if db_name not in self.connections:
            raise ValueError(f"Database '{db_name}' not connected")
        return self.connections[db_name]
    
    def list_databases(self) -> List[str]:
        """List all connected database names"""
        return list(self.connections.keys())
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get status of all connections"""
        return {
            "connected_databases": list(self.connections.keys()),
            "total_configured": len(self.databases),
            "total_connected": len(self.connections),
            "connection_details": self.connection_status
        }
    
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
        """Execute a SQL query using persistent connection"""
        start_time = time.time()
        
        # Update last used time
        if db_name in self.connection_status:
            self.connection_status[db_name]["last_used"] = datetime.utcnow().isoformat()
        
        try:
            # Validate query
            is_safe, safety_message = self._is_query_safe(query)
            if not is_safe:
                return {
                    "success": False,
                    "error": f"Query rejected: {safety_message}",
                    "data": None
                }
            
            # Get persistent connection
            connection = self.get_connection(db_name)
            
            # Execute query
            try:
                rows = await connection.fetch(query)
            except Exception as e:
                # If query fails, try to reconnect and retry once
                logger.warning(f"Query failed on {db_name}, attempting reconnect: {e}")
                if await self._reconnect_database(db_name):
                    connection = self.get_connection(db_name)
                    rows = await connection.fetch(query)
                else:
                    raise e
            
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
                "query": query[:100] + "..." if len(query) > 100 else query,
                "connection_persistent": True
            }
            
        except asyncio.TimeoutError:
            return {"success": False, "error": "Query timeout exceeded", "data": None}
        except Exception as e:
            logger.error(f"Query execution failed on {db_name}: {e}")
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

# MCP Protocol Models
class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    method: str
    params: Optional[Dict[str, Any]] = None

class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

# MCP Session Manager
class MCPSessionManager:
    def __init__(self):
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.db_manager = PersistentDatabaseManager()
    
    async def initialize(self):
        """Initialize the database manager"""
        try:
            success_count, failed_count = await self.db_manager.initialize()
            logger.info(f"Database manager initialized: {success_count} connections, {failed_count} failed")
            return success_count > 0  # Return True if at least one connection succeeded
        except Exception as e:
            logger.error(f"Failed to initialize database manager: {e}")
            return False
    
    async def close(self):
        """Close the database manager"""
        await self.db_manager.close()
    
    def create_session(self) -> str:
        """Create a new MCP session"""
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            "created_at": datetime.utcnow(),
            "initialized": False,
            "client_info": None
        }
        logger.info(f"Created MCP session: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session info"""
        return self.sessions.get(session_id)
    
    def initialize_session(self, session_id: str, client_info: Dict[str, Any]):
        """Initialize a session after handshake"""
        if session_id in self.sessions:
            self.sessions[session_id]["initialized"] = True
            self.sessions[session_id]["client_info"] = client_info
            logger.info(f"Initialized MCP session: {session_id}")

# Global session manager
session_manager = MCPSessionManager()

# MCP Server Implementation
class MCPServer:
    def __init__(self, session_manager: MCPSessionManager):
        self.session_manager = session_manager
    
    async def handle_initialize(self, request: MCPRequest, session_id: str) -> MCPResponse:
        """Handle MCP initialize request"""
        client_info = request.params or {}
        self.session_manager.initialize_session(session_id, client_info)
        
        return MCPResponse(
            id=request.id,
            result={
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {
                        "listChanged": False
                    }
                },
                "serverInfo": {
                    "name": "postgresql-mcp-server",
                    "version": "1.0.0"
                }
            }
        )
    
    async def handle_ping(self, request: MCPRequest) -> MCPResponse:
        """Handle ping request"""
        return MCPResponse(id=request.id, result={})
    
    async def handle_list_tools(self, request: MCPRequest) -> MCPResponse:
        """Handle list_tools request"""
        databases = self.session_manager.db_manager.list_databases()
        
        tools = [
            {
                "name": "list_databases",
                "description": "List all available PostgreSQL databases",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "query_database",
                "description": "Execute a SQL query on a specific database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name",
                            "enum": databases if databases else ["no_databases_connected"]
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
                "description": "Get schema information for a database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name",
                            "enum": databases if databases else ["no_databases_connected"]
                        }
                    },
                    "required": ["database"]
                }
            },
            {
                "name": "connection_status",
                "description": "Get connection status for all databases",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        ]
        
        return MCPResponse(
            id=request.id,
            result={"tools": tools}
        )
    
    async def handle_call_tool(self, request: MCPRequest) -> MCPResponse:
        """Handle call_tool request"""
        params = request.params or {}
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        
        try:
            if tool_name == "list_databases":
                result = await self._list_databases()
            elif tool_name == "query_database":
                result = await self._query_database(arguments)
            elif tool_name == "get_schema":
                result = await self._get_schema(arguments)
            elif tool_name == "connection_status":
                result = await self._connection_status()
            else:
                raise ValueError(f"Unknown tool: {tool_name}")
            
            return MCPResponse(
                id=request.id,
                result={
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, indent=2, default=str)
                        }
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Tool execution error: {e}")
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32603,
                    "message": f"Tool execution failed: {str(e)}"
                }
            )
    
    async def _list_databases(self) -> Dict[str, Any]:
        """List all databases"""
        status = self.session_manager.db_manager.get_connection_status()
        return {
            "connected_databases": status["connected_databases"],
            "total_configured": status["total_configured"],
            "total_connected": status["total_connected"],
            "persistent_connections": True
        }
    
    async def _query_database(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute database query"""
        database = arguments.get("database")
        query = arguments.get("query")
        
        if not database or not query:
            raise ValueError("Both 'database' and 'query' parameters are required")
        
        result = await self.session_manager.db_manager.execute_query(database, query)
        return result
    
    async def _get_schema(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Get database schema"""
        database = arguments.get("database")
        
        if not database:
            raise ValueError("'database' parameter is required")
        
        result = await self.session_manager.db_manager.get_schema_info(database)
        return result
    
    async def _connection_status(self) -> Dict[str, Any]:
        """Get connection status"""
        return self.session_manager.db_manager.get_connection_status()

# Global MCP server
mcp_server = MCPServer(session_manager)

# FastAPI Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    try:
        initialized = await session_manager.initialize()
        if initialized:
            logger.info("MCP PostgreSQL Server started successfully")
        else:
            logger.warning("MCP PostgreSQL Server started but no databases connected")
    except Exception as e:
        logger.error(f"Startup error: {e}")
    
    yield
    
    # Shutdown
    try:
        await session_manager.close()
        logger.info("MCP PostgreSQL Server shutdown complete")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")

app = FastAPI(
    title="MCP PostgreSQL Server",
    description="MCP-compatible PostgreSQL server for Claude AI",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware - allow all origins for testing, restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, use: ["https://claude.ai", "https://*.anthropic.com"]
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# MCP Endpoints
@app.post("/mcp")
async def mcp_endpoint(
    request: Request,
    mcp_session_id: Optional[str] = Header(None, alias="Mcp-Session-Id")
):
    """Main MCP endpoint for POST requests"""
    try:
        body = await request.json()
        mcp_request = MCPRequest(**body)
        
        # Handle initialization without session
        if mcp_request.method == "initialize" and not mcp_session_id:
            session_id = session_manager.create_session()
            response = await mcp_server.handle_initialize(mcp_request, session_id)
            
            # Return response with session header
            response_dict = response.dict(exclude_none=True)
            return Response(
                content=json.dumps(response_dict),
                media_type="application/json",
                headers={"Mcp-Session-Id": session_id}
            )
        
        # Validate session for other requests
        if not mcp_session_id or not session_manager.get_session(mcp_session_id):
            return MCPResponse(
                id=mcp_request.id,
                error={"code": -32002, "message": "Invalid or missing session"}
            ).dict(exclude_none=True)
        
        # Route requests
        if mcp_request.method == "ping":
            response = await mcp_server.handle_ping(mcp_request)
        elif mcp_request.method == "tools/list":
            response = await mcp_server.handle_list_tools(mcp_request)
        elif mcp_request.method == "tools/call":
            response = await mcp_server.handle_call_tool(mcp_request)
        else:
            response = MCPResponse(
                id=mcp_request.id,
                error={"code": -32601, "message": f"Method not found: {mcp_request.method}"}
            )
        
        return response.dict(exclude_none=True)
    
    except Exception as e:
        logger.error(f"MCP request error: {e}")
        return MCPResponse(
            id=getattr(mcp_request, 'id', None) if 'mcp_request' in locals() else None,
            error={"code": -32603, "message": f"Internal error: {str(e)}"}
        ).dict(exclude_none=True)

@app.get("/mcp")
async def mcp_sse_endpoint(
    mcp_session_id: Optional[str] = Header(None, alias="Mcp-Session-Id")
):
    """MCP endpoint for Server-Sent Events (GET requests)"""
    if not mcp_session_id or not session_manager.get_session(mcp_session_id):
        raise HTTPException(status_code=400, detail="Invalid or missing session")
    
    async def event_generator():
        # Send keep-alive events
        try:
            while True:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                await asyncio.sleep(30)  # Keep connection alive
        except asyncio.CancelledError:
            logger.info("SSE connection closed")
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

# Health check endpoints
@app.get("/")
async def root():
    """Health check endpoint"""
    try:
        status = session_manager.db_manager.get_connection_status()
        return {
            "service": "MCP PostgreSQL Server",
            "status": "healthy",
            "protocol": "MCP 2024-11-05",
            "databases_connected": status["total_connected"],
            "databases_configured": status["total_configured"],
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "service": "MCP PostgreSQL Server",
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/health")
async def health():
    """Health check for deployment platforms"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# MCP Discovery endpoint
@app.get("/.well-known/mcp")
async def mcp_discovery():
    """MCP discovery endpoint"""
    return {
        "mcpVersion": "2024-11-05",
        "endpoints": {
            "mcp": "/mcp"
        },
        "capabilities": {
            "tools": True
        },
        "serverInfo": {
            "name": "postgresql-mcp-server",
            "version": "1.0.0"
        }
    }

# Get port from environment
port = int(os.getenv("PORT", 5000))

if __name__ == "__main__":
    logger.info(f"Starting MCP PostgreSQL Server on port {port}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
