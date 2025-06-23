import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from functools import wraps

app = Flask(__name__)

# --- Configuration ---
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASS"),
    "host": os.environ.get("DB_HOST"),
    "port": 25060,
    "sslmode": "require"
}
API_TOKEN = os.environ.get("MCP_BEARER_TOKEN")


# --- Utilities ---
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth.split(" ")[1] != API_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper


# --- Endpoints ---
@app.route("/fetch_schema", methods=["GET"])
@require_auth
def fetch_schema():
    """Returns the public schema with tables and column types."""
    query = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    finally:
        conn.close()

    schema = {}
    for row in rows:
        table = row["table_name"]
        schema.setdefault(table, []).append({
            "name": row["column_name"],
            "type": row["data_type"]
        })

    return jsonify(schema)


@app.route("/execute_query", methods=["POST"])
@require_auth
def execute_query():
    """Executes a safe SELECT query with timeout and row limit."""
    data = request.get_json()
    sql = data.get("query", "").strip()

    if not sql.lower().startswith("select"):
        return jsonify({"error": "Only SELECT statements are allowed."}), 400

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 3000;")
            cur.execute(f"{sql} LIMIT 100;")
            results = cur.fetchall()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify(results)


# --- Health Check ---
@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# --- Run ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
