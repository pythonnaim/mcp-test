
"""
Minimal MCP Server - Focused on fixing Claude connection issues
Simplified version to isolate and fix the connection problem
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import asyncpg
import uvicorn
from fastapi import FastAPI, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple database manager
class SimpleDatabaseManager:
    def __init__(self):
        self.pools = {}
        self.status = {}
    
    async def initialize(self):
        db_vars = {k: v for k, v in os.environ.items() if k.startswith("POSTGRES_URL_")}
        logger.info(f"Found database vars: {list(db_vars.keys())}")
        
        for key, url in db_vars.items():
            db_name = key.replace("POSTGRES_URL_", "").lower()
            try:
                # Simple connection without extra parameters
                if '?' not in url:
                    url += '?sslmode=require'
                elif 'sslmode' not in url:
                    url += '&sslmode=require'
                
                pool = await asyncpg.create_pool(url, min_size=1, max_size=3)
                
                # Test connection
                async with pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                
                self.pools[db_name] = pool
                self.status[db_name] = {"status": "connected", "connected_at": datetime.utcnow().isoformat()}
                logger.info(f"✅ Connected to {db_name}")
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to {db_name}: {e}")
                self.status[db_name] = {"status": "failed", "error": str(e)}
        
        return len(self.pools), len(db_vars) - len(self.pools)
    
    async def close(self):
        for name, pool in self.pools.items():
            await pool.close()
    
    def get_databases(self):
        return list(self.pools.keys())
    
    def get_status(self):
        return {
            "connected_databases": self.get_databases(),
            "total_connected": len(self.pools),
            "connection_details": self.status
        }

# MCP Models
class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Union[str, int, None] = None
    method: str
    params: Optional[Dict[str, Any]] = None

class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Union[str, int, None] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

# Session manager
class SimpleSessionManager:
    def __init__(self):
        self.sessions = {}
        self.db_manager = SimpleDatabaseManager()
    
    async def initialize(self):
        return await self.db_manager.initialize()
    
    async def close(self):
        await self.db_manager.close()
    
    def create_session(self):
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {"created_at": datetime.utcnow()}
        return session_id
    
    def get_session(self, session_id):
        return self.sessions.get(session_id)

# Global manager
session_manager = SimpleSessionManager()

# FastAPI app
app = FastAPI(title="Minimal MCP Server", version="1.0.0")

# CORS - Very permissive for testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

@app.on_event("startup")
async def startup():
    logger.info("🚀 Starting minimal MCP server...")
    try:
        success, failed = await session_manager.initialize()
        logger.info(f"📊 Databases: {success} connected, {failed} failed")
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")

@app.on_event("shutdown")
async def shutdown():
    await session_manager.close()

# Main MCP endpoint
@app.post("/mcp")
async def mcp_endpoint(
    request: Request,
    mcp_session_id: Optional[str] = Header(None, alias="mcp-session-id")
):
    """Simplified MCP endpoint"""
    try:
        body = await request.json()
        logger.info(f"📨 MCP Request: {body}")
        
        mcp_request = MCPRequest(**body)
        
        # Handle initialize
        if mcp_request.method == "initialize":
            session_id = session_manager.create_session()
            
            response = MCPResponse(
                id=mcp_request.id,
                result={
                    "protocolVersion": "2024-11-05",  # Use older stable version
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "minimal-mcp-server", "version": "1.0.0"}
                }
            )
            
            return JSONResponse(
                content=response.dict(exclude_none=True),
                headers={"mcp-session-id": session_id}
            )
        
        # Validate session
        if not mcp_session_id or not session_manager.get_session(mcp_session_id):
            return JSONResponse(
                content={
                    "jsonrpc": "2.0",
                    "error": {"code": -32002, "message": "Invalid session"},
                    "id": mcp_request.id
                }
            )
        
        # Handle other methods
        if mcp_request.method == "ping":
            response = MCPResponse(id=mcp_request.id, result={})
        
        elif mcp_request.method == "tools/list":
            databases = session_manager.db_manager.get_databases()
            tools = [
                {
                    "name": "list_databases",
                    "description": "List available databases",
                    "inputSchema": {"type": "object", "properties": {}, "required": []}
                },
                {
                    "name": "get_status",
                    "description": "Get database connection status",
                    "inputSchema": {"type": "object", "properties": {}, "required": []}
                }
            ]
            response = MCPResponse(id=mcp_request.id, result={"tools": tools})
        
        elif mcp_request.method == "tools/call":
            params = mcp_request.params or {}
            tool_name = params.get("name")
            
            if tool_name == "list_databases":
                result = {"databases": session_manager.db_manager.get_databases()}
            elif tool_name == "get_status":
                result = session_manager.db_manager.get_status()
            else:
                result = {"error": f"Unknown tool: {tool_name}"}
            
            response = MCPResponse(
                id=mcp_request.id,
                result={"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
            )
        
        else:
            response = MCPResponse(
                id=mcp_request.id,
                error={"code": -32601, "message": f"Method not found: {mcp_request.method}"}
            )
        
        logger.info(f"📤 MCP Response: {response.dict(exclude_none=True)}")
        return JSONResponse(content=response.dict(exclude_none=True))
    
    except Exception as e:
        logger.error(f"❌ MCP error: {e}")
        return JSONResponse(
            content={
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": f"Internal error: {str(e)}"},
                "id": getattr(mcp_request, 'id', None) if 'mcp_request' in locals() else None
            }
        )

# Health endpoints
@app.get("/")
async def root():
    try:
        status = session_manager.db_manager.get_status()
        return {
            "service": "Minimal MCP Server",
            "status": "healthy",
            "databases": status,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {"service": "Minimal MCP Server", "status": "error", "error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok"}

# MCP discovery
@app.get("/.well-known/mcp")
async def mcp_discovery():
    return {
        "mcpVersion": "2024-11-05",
        "endpoints": {"mcp": "/mcp"},
        "capabilities": {"tools": True},
        "serverInfo": {"name": "minimal-mcp-server", "version": "1.0.0"}
    }

# Debug endpoint
@app.get("/debug")
async def debug():
    return {
        "environment_vars": {
            k: "***" if "PASSWORD" in k.upper() else v
            for k, v in os.environ.items()
            if k.startswith(("POSTGRES", "PORT", "RENDER"))
        },
        "sessions": len(session_manager.sessions),
        "databases": session_manager.db_manager.get_status()
    }

# OPTIONS handler for CORS
@app.options("/{path:path}")
async def options_handler(path: str):
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logger.info(f"🚀 Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
