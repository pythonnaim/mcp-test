import os
from flask import Flask, request, jsonify
from functools import wraps

# You would install Flask using: pip install Flask

app = Flask(__name__)

# --- Configuration ---
# In a real application, keep this secure (e.g., environment variables, KMS)
# This is a placeholder token for demonstration purposes.
VALID_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "your_secret_bearer_token_here")

# --- Authentication Decorator ---
def token_required(f):
    """
    Decorator to check for a valid Bearer token in the Authorization header.
    Simulates the "HTTPS + Auth" step mentioned in the document.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        # Check if the Authorization header is present
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            # Expected format: "Bearer <token>"
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]

        if not token or token != VALID_AUTH_TOKEN:
            # For demonstration, a simple 401 Unauthorized response.
            # In production, more detailed logging and specific error messages
            # might be appropriate, but avoid leaking sensitive info.
            return jsonify({"message": "Authentication Required: Invalid or missing token"}), 401
        return f(*args, **kwargs)
    return decorated

# --- Mock Database Data ---
# This dictionary simulates a very simple table for demonstration.
# In a real scenario, you'd connect to a PostgreSQL database.
MOCK_DATABASE = {
    "customers": [
        {"id": 1, "name": "Alice Smith", "mrr": 150.00, "city": "New York"},
        {"id": 2, "name": "Bob Johnson", "mrr": 200.00, "city": "London"},
        {"id": 3, "name": "Charlie Brown", "mrr": 100.00, "city": "New York"},
        {"id": 4, "name": "Diana Prince", "mrr": 300.00, "city": "Paris"},
        {"id": 5, "name": "Eve Adams", "mrr": 120.00, "city": "London"},
    ]
}

# --- MCP Endpoint: /execute_query ---
@app.route('/execute_query', methods=['POST'])
@token_required
def execute_query():
    """
    This endpoint simulates the 'execute_query' tool.
    It expects a JSON payload with a 'query' field.
    """
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Missing 'query' in request body"}), 400

        sql_query = data['query'].lower().strip()
        print(f"Received query: {sql_query}") # Log the query for debugging

        # --- Simplified Query Parsing and Mock Execution ---
        # This is a highly simplified parser. A real implementation would:
        # 1. Use a robust SQL parser or ORM.
        # 2. Connect to and query the actual PostgreSQL database.
        # 3. Handle various SQL clauses (SELECT, WHERE, ORDER BY, LIMIT, JOINs etc.).

        # Example 1: "Show top 10 customers by MRR" (translated to "select * from customers order by mrr desc limit 10")
        if "select * from customers order by mrr desc limit" in sql_query:
            limit = 10 # Default or parse from query
            try:
                # Attempt to extract limit if it's like 'limit X'
                parts = sql_query.split('limit')
                if len(parts) > 1:
                    limit = int(parts[1].strip().split(' ')[0])
            except ValueError:
                pass # Use default limit if parsing fails

            # Sort mock data by MRR in descending order and apply limit
            sorted_customers = sorted(MOCK_DATABASE["customers"], key=lambda c: c["mrr"], reverse=True)
            results = sorted_customers[:limit]
            return jsonify({"success": True, "results": results})

        # Example 2: Simple select all from customers
        elif "select * from customers" in sql_query:
            results = MOCK_DATABASE["customers"]
            return jsonify({"success": True, "results": results})

        # Example 3: Count customers
        elif "select count(*) from customers" in sql_query:
            count = len(MOCK_DATABASE["customers"])
            return jsonify({"success": True, "results": [{"count": count}]})

        # Example 4: Filter by city
        elif "select * from customers where city =" in sql_query:
            try:
                city_start_index = sql_query.find("city = '") + len("city = '")
                city_end_index = sql_query.find("'", city_start_index)
                city = sql_query[city_start_index:city_end_index]
                filtered_customers = [c for c in MOCK_DATABASE["customers"] if c["city"].lower() == city.lower()]
                return jsonify({"success": True, "results": filtered_customers})
            except Exception:
                return jsonify({"error": "Could not parse city from query"}), 400


        else:
            return jsonify({"message": f"Unsupported mock query: '{sql_query}'. Try 'select * from customers' or 'select * from customers order by mrr desc limit 5'"}), 400

    except Exception as e:
        # Basic error handling. In production, log this error securely.
        print(f"An error occurred: {e}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

# --- Health Check Endpoint (Optional but Recommended) ---
@app.route('/health', methods=['GET'])
def health_check():
    """Simple endpoint to check if the server is running."""
    return jsonify({"status": "running"}), 200

# --- Running the Flask App ---
if __name__ == '__main__':
    # To run this mock server:
    # 1. Save the code as, e.g., `mcp_mock_server.py`.
    # 2. Set the environment variable: export MCP_AUTH_TOKEN="your_secret_bearer_token_here"
    #    (Replace with a strong, secret token)
    # 3. Run from your terminal: python mcp_mock_server.py
    #
    # This will start the Flask development server on http://127.0.0.1:5000/
    # For a production deployment, use a WSGI server like Gunicorn/uWSGI.
    app.run(debug=True, host='0.0.0.0', port=5000)
