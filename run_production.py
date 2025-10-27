"""
X-Commerce Monitoring - Production Server
==========================================

This is the SINGLE ENTRY POINT for running the application in production.

How to run:
    python run_production.py

What it does:
    1. Starts background monitoring jobs (3 threads)
    2. Starts Gunicorn server with eventlet worker
    3. Serves Flask + WebSocket application on port 5000

Environment variables (loaded from Vault):
    - METABASE_URL
    - METABASE_USERNAME
    - METABASE_PASSWORD
    - APP_SECRET_KEY
    - APP_PORT (optional, default: 5000)
"""

import os
import sys
import logging
import multiprocessing
from gunicorn.app.base import BaseApplication

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# GUNICORN CONFIGURATION (embedded in this file)
# =============================================================================

class GunicornConfig:
    """Gunicorn server configuration"""

    # Server socket
    bind = f"0.0.0.0:{os.getenv('APP_PORT', '5000')}"
    backlog = 2048

    # Worker processes
    # CRITICAL: Must be 1 worker because we use in-memory state
    # Multiple workers = separate memory spaces = data inconsistency
    workers = 1

    # CRITICAL: Must be eventlet for WebSocket support
    worker_class = 'eventlet'
    worker_connections = 1000
    max_requests = 10000
    max_requests_jitter = 1000

    # Timeout settings
    timeout = 300  # 5 minutes for long Metabase queries
    graceful_timeout = 120
    keepalive = 5

    # Logging
    accesslog = os.getenv('GUNICORN_ACCESS_LOG', '-')  # stdout
    errorlog = os.getenv('GUNICORN_ERROR_LOG', '-')    # stderr
    loglevel = os.getenv('GUNICORN_LOG_LEVEL', 'info')
    access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

    # Process naming
    proc_name = 'vendor-monitoring'

    # Server mechanics
    daemon = False
    pidfile = None

    # SSL (if needed)
    keyfile = os.getenv('SSL_KEY_FILE')
    certfile = os.getenv('SSL_CERT_FILE')

# =============================================================================
# CUSTOM GUNICORN APPLICATION
# =============================================================================

class VendorMonitoringApp(BaseApplication):
    """
    Custom Gunicorn application that:
    1. Starts background jobs before starting server
    2. Loads Flask + SocketIO application
    3. Applies configuration
    """

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        """Load configuration from GunicornConfig class"""
        config_vars = {
            key: getattr(GunicornConfig, key)
            for key in dir(GunicornConfig)
            if not key.startswith('_')
        }

        for key, value in config_vars.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self):
        """Load the Flask + SocketIO application"""
        return self.application

    def init(self, parser, opts, args):
        """Initialize (called before load)"""
        pass

# =============================================================================
# APPLICATION STARTUP
# =============================================================================

def start_application():
    """
    Main function to start the X-Commerce Monitoring Dashboard.

    Steps:
    1. Import Flask app and SocketIO
    2. Start background monitoring jobs
    3. Start Gunicorn server
    """

    logger.info("=" * 60)
    logger.info("Starting X-Commerce Monitoring Dashboard")
    logger.info("=" * 60)

    # Step 1: Import application
    logger.info("Step 1/3: Loading Flask application...")
    try:
        from app import app, socketio, start_background_jobs
        logger.info("✅ Flask application loaded successfully")
    except ImportError as e:
        logger.error(f"❌ Failed to import application: {e}")
        logger.error("Make sure app.py, config.py, and mini.py are in the same directory")
        sys.exit(1)

    # Step 2: Start background jobs
    logger.info("Step 2/3: Starting background monitoring jobs...")
    try:
        start_background_jobs()
        logger.info("✅ Background jobs started:")
        logger.info("   - Discount Stock Monitoring (every 5 minutes)")
        logger.info("   - Vendor Status Monitoring (every 5 minutes)")
        logger.info("   - Vendor Product Status Monitoring (every 6 minutes)")
    except Exception as e:
        logger.error(f"❌ Failed to start background jobs: {e}")
        sys.exit(1)

    # Step 3: Start Gunicorn server
    logger.info("Step 3/3: Starting Gunicorn server...")
    logger.info(f"Server configuration:")
    logger.info(f"   - Bind address: {GunicornConfig.bind}")
    logger.info(f"   - Workers: {GunicornConfig.workers}")
    logger.info(f"   - Worker class: {GunicornConfig.worker_class}")
    logger.info(f"   - Worker connections: {GunicornConfig.worker_connections}")
    logger.info(f"   - Timeout: {GunicornConfig.timeout}s")
    logger.info("=" * 60)

    try:
        # Create and run Gunicorn application
        gunicorn_app = VendorMonitoringApp(socketio, options={})
        gunicorn_app.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping server...")
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        sys.exit(1)

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    Production entry point.

    Usage:
        python run_production.py

    Or in Docker:
        CMD ["python", "run_production.py"]
    """
    start_application()
