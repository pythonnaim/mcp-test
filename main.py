# /// script
# dependencies = ["asyncpg"]
# ///

"""
Simple MCP server for Claude with PostgreSQL database operations.
No external API dependencies - works directly with Claude Desktop.
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
from mcp.server.fastmcp import FastMCP

# Create MCP server
mcp = FastMCP(
    "claude-postgres-server",
    dependencies=["asyncpg"],
)

# Your DigitalOcean PostgreSQL connection
DB_DSN = os.getenv("POSTGRES_DSN")


async def get_db_connection():
    """Get a database connection"""
    try:
        return await asyncpg.connect(DB_DSN)
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")


@mcp.tool()
async def create_table(table_name: str, columns: str) -> str:
    """
    Create a new table in the database.
    
    Args:
        table_name: Name of the table to create
        columns: Column definitions (e.g., "id SERIAL PRIMARY KEY, name TEXT, age INTEGER")
    """
    try:
        conn = await get_db_connection()
        try:
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            await conn.execute(query)
            return f"Table '{table_name}' created successfully"
        finally:
            await conn.close()
    except Exception as e:
        return f"Error creating table: {str(e)}"


@mcp.tool()
async def insert_data(table_name: str, data: Dict[str, Any]) -> str:
    """
    Insert data into a table.
    
    Args:
        table_name: Name of the table
        data: Dictionary of column names and values to insert
    """
    try:
        conn = await get_db_connection()
        try:
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ", ".join([f"${i+1}" for i in range(len(values))])
            
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            await conn.execute(query, *values)
            return f"Data inserted into '{table_name}' successfully"
        finally:
            await conn.close()
    except Exception as e:
        return f"Error inserting data: {str(e)}"


@mcp.tool()
async def query_data(table_name: str, where_clause: Optional[str] = None, limit: int = 100) -> str:
    """
    Query data from a table.
    
    Args:
        table_name: Name of the table to query
        where_clause: Optional WHERE clause (e.g., "age > 18")
        limit: Maximum number of rows to return (default: 100)
    """
    try:
        conn = await get_db_connection()
        try:
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" LIMIT {limit}"
            
            rows = await conn.fetch(query)
            
            if not rows:
                return f"No data found in table '{table_name}'"
            
            # Convert rows to readable format
            result = []
            for row in rows:
                result.append(dict(row))
            
            return json.dumps(result, indent=2, default=str)
        finally:
            await conn.close()
    except Exception as e:
        return f"Error querying data: {str(e)}"


@mcp.tool()
async def update_data(table_name: str, set_clause: str, where_clause: str) -> str:
    """
    Update data in a table.
    
    Args:
        table_name: Name of the table
        set_clause: SET clause (e.g., "name = 'John', age = 25")
        where_clause: WHERE clause (e.g., "id = 1")
    """
    try:
        conn = await get_db_connection()
        try:
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            result = await conn.execute(query)
            return f"Updated {result.split()[-1]} row(s) in '{table_name}'"
        finally:
            await conn.close()
    except Exception as e:
        return f"Error updating data: {str(e)}"


@mcp.tool()
async def delete_data(table_name: str, where_clause: str) -> str:
    """
    Delete data from a table.
    
    Args:
        table_name: Name of the table
        where_clause: WHERE clause (e.g., "id = 1")
    """
    try:
        conn = await get_db_connection()
        try:
            query = f"DELETE FROM {table_name} WHERE {where_clause}"
            result = await conn.execute(query)
            return f"Deleted {result.split()[-1]} row(s) from '{table_name}'"
        finally:
            await conn.close()
    except Exception as e:
        return f"Error deleting data: {str(e)}"


@mcp.tool()
async def list_tables() -> str:
    """List all tables in the database"""
    try:
        conn = await get_db_connection()
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            rows = await conn.fetch(query)
            
            if not rows:
                return "No tables found in database"
            
            tables = [row['table_name'] for row in rows]
            return "Tables in database:\n" + "\n".join(f"- {table}" for table in tables)
        finally:
            await conn.close()
    except Exception as e:
        return f"Error listing tables: {str(e)}"


@mcp.tool()
async def describe_table(table_name: str) -> str:
    """Get the structure of a table"""
    try:
        conn = await get_db_connection()
        try:
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            rows = await conn.fetch(query, table_name)
            
            if not rows:
                return f"Table '{table_name}' not found"
            
            result = f"Structure of table '{table_name}':\n"
            for row in rows:
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                result += f"- {row['column_name']}: {row['data_type']} {nullable}{default}\n"
            
            return result
        finally:
            await conn.close()
    except Exception as e:
        return f"Error describing table: {str(e)}"


@mcp.tool()
async def execute_custom_query(sql_query: str) -> str:
    """
    Execute a custom SQL query (READ-ONLY for safety).
    
    Args:
        sql_query: The SQL query to execute (SELECT statements only)
    """
    try:
        # Basic safety check - only allow SELECT statements
        query_upper = sql_query.strip().upper()
        if not query_upper.startswith('SELECT'):
            return "Error: Only SELECT queries are allowed for safety"
        
        conn = await get_db_connection()
        try:
            rows = await conn.fetch(sql_query)
            
            if not rows:
                return "Query executed successfully - no results returned"
            
            result = []
            for row in rows:
                result.append(dict(row))
            
            return json.dumps(result, indent=2, default=str)
        finally:
            await conn.close()
    except Exception as e:
        return f"Error executing query: {str(e)}"


@mcp.tool()
async def store_note(title: str, content: str, tags: Optional[str] = None) -> str:
    """
    Store a note in the database.
    
    Args:
        title: Title of the note
        content: Content of the note
        tags: Optional comma-separated tags
    """
    try:
        conn = await get_db_connection()
        try:
            # Create notes table if it doesn't exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notes (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert the note
            await conn.execute(
                "INSERT INTO notes (title, content, tags) VALUES ($1, $2, $3)",
                title, content, tags
            )
            return f"Note '{title}' stored successfully"
        finally:
            await conn.close()
    except Exception as e:
        return f"Error storing note: {str(e)}"


@mcp.tool()
async def search_notes(search_term: str) -> str:
    """
    Search for notes by title or content.
    
    Args:
        search_term: Term to search for in title or content
    """
    try:
        conn = await get_db_connection()
        try:
            query = """
            SELECT id, title, content, tags, created_at 
            FROM notes 
            WHERE title ILIKE $1 OR content ILIKE $1
            ORDER BY created_at DESC
            LIMIT 20
            """
            rows = await conn.fetch(query, f"%{search_term}%")
            
            if not rows:
                return f"No notes found containing '{search_term}'"
            
            result = f"Found {len(rows)} note(s) containing '{search_term}':\n\n"
            for row in rows:
                result += f"ID: {row['id']}\n"
                result += f"Title: {row['title']}\n"
                result += f"Content: {row['content'][:200]}{'...' if len(row['content']) > 200 else ''}\n"
                if row['tags']:
                    result += f"Tags: {row['tags']}\n"
                result += f"Created: {row['created_at']}\n"
                result += "-" * 50 + "\n"
            
            return result
        finally:
            await conn.close()
    except Exception as e:
        return f"Error searching notes: {str(e)}"


@mcp.resource("database://tables")
async def get_database_info() -> str:
    """Get information about all database tables"""
    try:
        conn = await get_db_connection()
        try:
            # Get table list with row counts
            query = """
            SELECT 
                schemaname as schema,
                tablename as table_name,
                hasindexes as has_indexes,
                hasrules as has_rules,
                hastriggers as has_triggers
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
            """
            rows = await conn.fetch(query)
            
            result = "Database Information:\n\n"
            for row in rows:
                result += f"Table: {row['table_name']}\n"
                
                # Get row count
                count_result = await conn.fetchval(f"SELECT COUNT(*) FROM {row['table_name']}")
                result += f"  Rows: {count_result}\n"
                result += f"  Has Indexes: {row['has_indexes']}\n"
                result += f"  Has Rules: {row['has_rules']}\n"
                result += f"  Has Triggers: {row['has_triggers']}\n\n"
            
            return result
        finally:
            await conn.close()
    except Exception as e:
        return f"Error getting database info: {str(e)}"


async def initialize_database():
    """Test database connection and create sample table"""
    try:
        print("Testing database connection...")
        conn = await get_db_connection()
        try:
            # Test connection
            result = await conn.fetchval("SELECT version()")
            print(f"✅ Connected to PostgreSQL: {result}")
            
            # Create a sample table for demonstration
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_test (
                    id SERIAL PRIMARY KEY,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("✅ Sample table 'mcp_test' created")
            
            return True
        finally:
            await conn.close()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def main():
    """Initialize and start the MCP server"""
    print("🚀 Starting Claude MCP Server on Render...")
    
    # Test database connection first
    async def init_db():
        return await initialize_database()
    
    try:
        db_success = asyncio.run(init_db())
        if not db_success:
            print("⚠️  Warning: Database not available. Server will start but database tools won't work.")
        else:
            print("✅ Database ready!")
    except Exception as e:
        print(f"⚠️  Database initialization failed: {e}")
        print("Server will start but database tools won't work.")
    
    print(f"🔌 Starting MCP server on port {port}...")
    print("📝 Available tools: create_table, insert_data, query_data, update_data, delete_data, list_tables, describe_table, execute_custom_query, store_note, search_notes")
    print("📚 Available resources: database://tables")
    
    # Start the MCP server with port binding for Render
    mcp.run(port=8080, host="0.0.0.0")


if __name__ == "__main__":
    main()
