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
    # CRITICAL: Must be False for eventlet workers
    # preload_app = True breaks eventlet after fork (monkey patching issues)
    preload_app = False

    # SSL (if needed)
    keyfile = os.getenv('SSL_KEY_FILE')
    certfile = os.getenv('SSL_CERT_FILE')

    # Worker lifecycle hooks
    @staticmethod
    def post_worker_init(worker):
        """Called after worker initialization - perform initial fetch then start background jobs"""
        import logging
        import time
        from app import perform_initial_fetch, start_background_jobs

        logger = logging.getLogger(__name__)

        # STEP 0: Startup delay to allow DNS to fully initialize in containerized environments
        # This is critical in Kubernetes/Docker where DNS may not be immediately available
        startup_delay = int(os.getenv('STARTUP_DELAY_SECONDS', '3'))
        if startup_delay > 0:
            logger.info(f"⏳ Worker {worker.pid} waiting {startup_delay}s for DNS initialization...")
            time.sleep(startup_delay)
            logger.info(f"✅ Worker {worker.pid} startup delay complete")

        # STEP 1: Perform initial data fetch (BLOCKING - must complete before serving requests)
        logger.info(f"🔄 Worker {worker.pid} performing initial data fetch...")
        try:
            fetch_success = perform_initial_fetch()
            if fetch_success:
                logger.info(f"✅ Worker {worker.pid} initial fetch completed successfully")
            else:
                logger.warning(f"⚠️  Worker {worker.pid} initial fetch had errors (see logs above)")
        except Exception as e:
            logger.error(f"❌ Worker {worker.pid} initial fetch failed: {e}")
            logger.error("Application will start but may not have data available")

        # STEP 2: Start background jobs (for periodic refreshes)
        logger.info(f"🔄 Worker {worker.pid} starting background jobs...")
        try:
            start_background_jobs()
            logger.info(f"✅ Worker {worker.pid} background jobs started")
        except Exception as e:
            logger.error(f"❌ Worker {worker.pid} failed to start background jobs: {e}")

        logger.info(f"✅ Worker {worker.pid} initialized and ready to handle requests")

# =============================================================================
# CUSTOM GUNICORN APPLICATION
# =============================================================================

class VendorMonitoringApp(BaseApplication):
    """
    Custom Gunicorn application that:
    1. Loads Flask + SocketIO application
    2. Applies Gunicorn configuration
    3. Starts background jobs in worker via post_worker_init hook
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
        logger.info("🔄 Worker loading application...")
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
    2. Start Gunicorn server (background jobs start in worker via post_worker_init hook)
    """

    logger.info("=" * 60)
    logger.info("Starting X-Commerce Monitoring Dashboard")
    logger.info("=" * 60)

    # Step 1: Import application
    logger.info("Step 1/2: Loading Flask application...")
    try:
        from app import app, socketio
        logger.info("✅ Flask application loaded successfully")
    except ImportError as e:
        logger.error(f"❌ Failed to import application: {e}")
        logger.error("Make sure app.py, config.py, and mini.py are in the same directory")
        sys.exit(1)

    # Step 2: Start Gunicorn server
    # NOTE: Background jobs will start in worker via post_worker_init hook
    logger.info("Step 2/2: Starting Gunicorn server...")
    logger.info("   (Background jobs will start in worker after eventlet initialization)")
    logger.info(f"Server configuration:")
    logger.info(f"   - Bind address: {GunicornConfig.bind}")
    logger.info(f"   - Workers: {GunicornConfig.workers}")
    logger.info(f"   - Worker class: {GunicornConfig.worker_class}")
    logger.info(f"   - Worker connections: {GunicornConfig.worker_connections}")
    logger.info(f"   - Timeout: {GunicornConfig.timeout}s")
    logger.info("=" * 60)

    try:
        # Create and run Gunicorn application
        # IMPORTANT: Pass 'app' not 'socketio' - SocketIO is already attached to app
        gunicorn_app = VendorMonitoringApp(app, options={})
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
