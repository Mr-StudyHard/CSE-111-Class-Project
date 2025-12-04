import os
import sys
import traceback

# Ensure project root is on sys.path so `import app` resolves consistently
ROOT = os.path.dirname(__file__)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)  # ensure relative paths (like SQLite) resolve to project root

try:
    from app import create_app
except Exception:
    print("[run_server] Failed to import app:create_app")
    traceback.print_exc()
    raise

app = create_app()

if __name__ == "__main__":
    # Default to 127.0.0.1 to align with Vite proxy and avoid odd adapter issues
    host = os.getenv("APP_HOST", "127.0.0.1")
    port = int(os.getenv("APP_PORT", "5000"))
    print(f"[run_server] Starting Flask on {host}:{port}")
    # Disable reloader to keep a single process managed by this script
    app.run(host=host, port=port, debug=False, use_reloader=False)
