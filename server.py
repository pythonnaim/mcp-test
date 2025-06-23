from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import asyncio
from main import PostgreSQLMCPServer
from typing import Optional, Dict, Any, List
import uvicorn

load_dotenv()

app = FastAPI(title="PostgreSQL MCP Server", version="1.0.0")
security = HTTPBearer()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database server instance
db_server = None

async def get_db_server():
    global db_server
    if db_server is None:
        db_server = PostgreSQLMCPServer(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 25060)),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            ssl=os.getenv("DB_SSL", "require"),
            max_rows=int(os.getenv("MAX_ROWS", 1000)),
            query_timeout=int(os.getenv("QUERY_TIMEOUT", 30))
        )
    return db_server

async def verify_token(authorization: str = Header(None)):
    expected_token = f"Bearer {os.getenv('BEARER_TOKEN')}"
    if authorization != expected_token:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    return True

@app.post("/fetch_data")
async def fetch_data_endpoint(
    request: Dict[str, Any],
    _: bool = Depends(verify_token)
):
    """Endpoint for fetch_data MCP method"""
    try:
        db = await get_db_server()
        result = await db.fetch_data(
            query=request.get("query"),
            parameters=request.get("parameters"),
            limit_override=request.get("limit_override")
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute_query")
async def execute_query_endpoint(
    request: Dict[str, Any],
    _: bool = Depends(verify_token)
):
    """Endpoint for execute_query MCP method"""
    try:
        db = await get_db_server()
        result = await db.execute_query(
            query=request.get("query"),
            parameters=request.get("parameters"),
            allow_writes=request.get("allow_writes", False)
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        db = await get_db_server()
        # Simple connection test
        result = await db.fetch_data("SELECT 1 as health_check")
        if result["success"]:
            return {"status": "healthy", "database": "connected"}
        else:
            return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up database connections on shutdown"""
    global db_server
    if db_server:
        await db_server.close()

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host=os.getenv("SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVER_PORT", 8000)),
        reload=False
    )