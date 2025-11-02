"""
Real-Time Vendor Monitoring & Alerting Dashboard
Flask application with WebSocket support for live updates
"""

# CRITICAL: Monkey patch MUST be first for Python 3.12 + eventlet compatibility
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import pandas as pd
from datetime import datetime
import pytz
import threading
import time
import io
from typing import Dict, List, Set, Optional
import uuid
import traceback
import logging
import re
from mini import fetch_question_data
from config import (
    # Flask & WebSocket Settings
    APP_SECRET_KEY, APP_HOST, APP_PORT, APP_DEBUG,
    CORS_ALLOWED_ORIGINS, ASYNC_MODE,

    # Metabase Configuration
    METABASE_URL, METABASE_USERNAME, METABASE_PASSWORD,
    METABASE_DATABASE, METABASE_PAGE_SIZE,
    QUESTION_ID_DISCOUNT_STOCK, QUESTION_ID_VENDOR_STATUS, QUESTION_ID_VENDOR_PRODUCT_STATUS,

    # Background Job Intervals
    DISCOUNT_STOCK_JOB_INTERVAL, VENDOR_STATUS_JOB_INTERVAL, VENDOR_PRODUCT_STATUS_JOB_INTERVAL,

    # Alert Severity Levels
    SEVERITY_CHERRY, SEVERITY_RED, SEVERITY_YELLOW, SEVERITY_GREEN,
    SEVERITY_LIGHT_GREEN, SEVERITY_DARK_GREEN,
    SEVERITY_RED_HIGH, SEVERITY_RED_MEDIUM, SEVERITY_RED_LIGHT,

    # Alert Priority Levels
    PRIORITY_HIGH, PRIORITY_LOW,

    # Vendor Status Values
    VENDOR_STATUS_ACTIVE, VENDOR_STATUS_INACTIVE,

    # Product Status Values
    PRODUCT_STATUS_STOCK_GOOD, PRODUCT_STATUS_STOCK_ISSUE,
    PRODUCT_STATUS_VISIBILITY_GOOD, PRODUCT_STATUS_VISIBILITY_ISSUE,

    # Alert Type Names
    ALERT_TYPE_PRODUCT_STOCK_FINISHED, ALERT_TYPE_DISCOUNT_STOCK_NEAR_END,
    ALERT_TYPE_VENDOR_DEACTIVATED, ALERT_TYPE_VENDOR_NOT_ACTIVE, ALERT_TYPE_VENDOR_ACTIVATED,
    ALERT_TYPE_DISCOUNTED_ITEM_FIXED,
    ALERT_TYPE_STOCK_ISSUES_NEW, ALERT_TYPE_STOCK_ISSUES_PERSISTENT, ALERT_TYPE_STOCK_ISSUES_FIXED,
    ALERT_TYPE_VISIBILITY_ISSUES_NEW, ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT, ALERT_TYPE_VISIBILITY_ISSUES_FIXED,

    # Thresholds
    DISCOUNT_STOCK_NEAR_END_THRESHOLD
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tehran timezone configuration
TEHRAN_TZ = pytz.timezone('Asia/Tehran')

def get_tehran_time():
    """Get current time in Tehran timezone"""
    return datetime.now(TEHRAN_TZ)

def format_tehran_time(dt=None):
    """Format datetime to Tehran timezone string (YYYY-MM-DD HH:MM:SS)"""
    if dt is None:
        dt = get_tehran_time()
    elif dt.tzinfo is None:
        # If naive datetime, assume UTC and convert to Tehran
        dt = pytz.utc.localize(dt).astimezone(TEHRAN_TZ)
    elif dt.tzinfo != TEHRAN_TZ:
        # If different timezone, convert to Tehran
        dt = dt.astimezone(TEHRAN_TZ)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

app = Flask(__name__)
app.config['SECRET_KEY'] = APP_SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins=CORS_ALLOWED_ORIGINS, async_mode=ASYNC_MODE)

# Global state management
class AppState:
    def __init__(self):
        # ========== CENTRALIZED DATA CACHE (ALL DATA, NO FILTERING) ==========
        # This data is fetched by background jobs and on startup
        # Users filter from this cached data for fast response
        self.full_discount_stock_data: pd.DataFrame = pd.DataFrame()
        self.full_vendor_status_data: pd.DataFrame = pd.DataFrame()
        self.full_vendor_product_status_data: pd.DataFrame = pd.DataFrame()

        # Tracking for fetch status
        self.last_fetch_times: Dict[str, Optional[datetime]] = {
            'discount_stock': None,
            'vendor_status': None,
            'vendor_product_status': None
        }
        self.fetch_errors: Dict[str, Optional[str]] = {
            'discount_stock': None,
            'vendor_status': None,
            'vendor_product_status': None
        }
        self.initial_fetch_complete: bool = False

        # Background job heartbeat monitoring
        self.last_heartbeat: datetime = get_tehran_time()

        # ========== MULTI-USER SESSION SUPPORT ==========
        # Each user gets their own session with vendor codes
        # Format: {session_id: {'vendor_codes': Set[str], 'created_at': datetime, 'last_accessed': datetime}}
        self.user_sessions: Dict[str, Dict] = {}

        # ========== LEGACY SUPPORT (for backward compatibility) ==========
        # These are kept for admin/global monitoring view
        self.vendor_codes: Set[str] = set()  # Global vendor codes (if needed)
        self.alerts: Dict[str, Dict[str, Dict]] = {
            'discount_stock': {},      # Key: product_id
            'vendor_status': {},       # Key: vendor_code
            'vendor_product_stock': {},     # Key: vendor_code
            'vendor_product_visibility': {} # Key: vendor_code
        }
        self.cleared_alerts: Dict[str, Dict[str, Dict]] = {
            'discount_stock': {},
            'vendor_status': {},
            'vendor_product_stock': {},
            'vendor_product_visibility': {}
        }
        self.previous_vendor_status: Dict[str, str] = {}
        self.previous_product_status: Dict[str, Dict] = {}

        # Filtered data (kept for compatibility, but users get their own filtered data)
        self.vendor_status_data: pd.DataFrame = pd.DataFrame()
        self.discount_stock_data: pd.DataFrame = pd.DataFrame()
        self.vendor_product_status_data: pd.DataFrame = pd.DataFrame()

        # Use RLock for re-entrant safety and add lock monitoring
        self.lock = threading.RLock()
        self.lock_holder = None  # Track which function holds the lock (for debugging)
        self.lock_acquired_at = None  # Track when lock was acquired

state = AppState()

# Lock timeout wrapper with automatic release
class TimedLock:
    """Context manager for lock with timeout and monitoring"""
    def __init__(self, lock, timeout=30, name="unknown"):
        self.lock = lock
        self.timeout = timeout
        self.name = name
        self.acquired = False

    def __enter__(self):
        import time
        start_time = time.time()
        self.acquired = self.lock.acquire(timeout=self.timeout)
        if not self.acquired:
            logger.error(f"⚠️ LOCK TIMEOUT: {self.name} couldn't acquire lock after {self.timeout}s")
            logger.error(f"   Lock holder: {state.lock_holder}")
            logger.error(f"   Held since: {state.lock_acquired_at}")
            raise TimeoutError(f"Failed to acquire lock for {self.name} after {self.timeout}s")

        state.lock_holder = self.name
        state.lock_acquired_at = get_tehran_time()
        elapsed = time.time() - start_time
        if elapsed > 1:
            logger.warning(f"⚠️  {self.name} waited {elapsed:.2f}s for lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            state.lock_holder = None
            state.lock_acquired_at = None
            self.lock.release()

def format_datetime(dt_string: str) -> str:
    """Convert datetime from '2025-11-02T00:00:00+03:30' to '2025-11-02 - 00:00'"""
    try:
        # Parse the datetime string
        match = re.match(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):\d{2}', str(dt_string))
        if match:
            year, month, day, hour, minute = match.groups()
            return f"{year}-{month}-{day} - {hour}:{minute}"
        return str(dt_string)
    except:
        return str(dt_string)

def format_percentage(rate: float) -> str:
    """Convert rate to percentage with 2 decimal places"""
    try:
        percentage = float(rate) * 100
        return f"{percentage:.2f}%"
    except:
        return str(rate)

def create_product_id(row) -> str:
    """Create unique product identifier"""
    return f"{row.get('vendor_code', '')}_{row.get('vendor_product_header_name', '')}_{row.get('product_name', '')}"

def calculate_discount_severity(discount_stock: int) -> str:
    """Calculate color severity for discount stock (1-3 range)"""
    if discount_stock <= 0:
        return 'none'
    elif discount_stock == 1:
        return SEVERITY_RED_HIGH
    elif discount_stock == 2:
        return SEVERITY_RED_MEDIUM
    elif discount_stock == 3:
        return SEVERITY_RED_LIGHT
    return 'none'

# =============================================================================
# HELPER FUNCTIONS FOR MULTI-USER SUPPORT
# =============================================================================

def create_user_session(vendor_codes: Set[str]) -> str:
    """Create a new user session and return session ID"""
    session_id = str(uuid.uuid4())
    with TimedLock(state.lock, timeout=30, name="create_user_session"):
        state.user_sessions[session_id] = {
            'vendor_codes': vendor_codes,
            'created_at': get_tehran_time(),
            'last_accessed': get_tehran_time(),
            'status': 'active'  # Track session status: active, inactive, expired
        }
    logger.info(f"✅ SESSION CREATED: {session_id[:8]}... | Vendors: {len(vendor_codes)} | Status: active")
    return session_id

def is_session_valid(session_id: str, max_age_hours: float = 5/60) -> bool:
    """Check if a session is valid (exists and not expired). Default: 5 minutes"""
    now = get_tehran_time()
    with TimedLock(state.lock, timeout=30, name="is_session_valid"):
        if session_id not in state.user_sessions:
            return False

        session = state.user_sessions[session_id]
        age_hours = (now - session['last_accessed']).total_seconds() / 3600
        is_expired = age_hours > max_age_hours

        if is_expired:
            # Mark as expired if not already
            if session.get('status') != 'expired':
                session['status'] = 'expired'
                session['expired_at'] = now
                logger.info(f"⏱️  SESSION EXPIRED: {session_id[:8]}... | Inactive for {age_hours:.1f}h | Status: expired")
            return False

        return session.get('status') == 'active'

def get_session_vendor_codes(session_id: str) -> Optional[Set[str]]:
    """Get vendor codes for a session, update last accessed time"""
    with TimedLock(state.lock, timeout=30, name="get_session_vendor_codes"):
        if session_id in state.user_sessions:
            session = state.user_sessions[session_id]
            # Check if session is expired
            now = get_tehran_time()
            age_hours = (now - session['last_accessed']).total_seconds() / 3600

            if age_hours > 5/60:  # 5 minutes
                if session.get('status') != 'expired':
                    session['status'] = 'expired'
                    session['expired_at'] = now
                    logger.warning(f"⏱️  SESSION EXPIRED: {session_id[:8]}... | Inactive for {age_hours:.1f}h")
                return None

            # Update last accessed time only if session is active
            if session.get('status') == 'active':
                session['last_accessed'] = now
                return session['vendor_codes']
    return None

def filter_data_for_session(session_id: str) -> Dict[str, pd.DataFrame]:
    """Filter all cached data for a specific user session"""
    vendor_codes = get_session_vendor_codes(session_id)
    if not vendor_codes:
        return {
            'discount_stock': pd.DataFrame(),
            'vendor_status': pd.DataFrame(),
            'vendor_product_status': pd.DataFrame()
        }

    with TimedLock(state.lock, timeout=30, name="filter_data_for_session"):
        # Filter discount stock data
        discount_df = state.full_discount_stock_data
        if not discount_df.empty and 'vendor_code' in discount_df.columns:
            discount_df = discount_df[discount_df['vendor_code'].isin(vendor_codes)].copy()

        # Filter vendor status data
        vendor_df = state.full_vendor_status_data
        if not vendor_df.empty and 'vendor_code' in vendor_df.columns:
            vendor_df = vendor_df[vendor_df['vendor_code'].isin(vendor_codes)].copy()

        # Filter vendor product status data
        product_df = state.full_vendor_product_status_data
        if not product_df.empty and 'vendor_code' in product_df.columns:
            product_df = product_df[product_df['vendor_code'].isin(vendor_codes)].copy()

    return {
        'discount_stock': discount_df,
        'vendor_status': vendor_df,
        'vendor_product_status': product_df
    }

def cleanup_old_sessions(max_age_hours: float = 5/60):
    """
    Remove sessions older than max_age_hours (default 5 minutes).
    This function gracefully handles session cleanup:
    1. Identifies sessions inactive for more than max_age_hours
    2. Marks them as 'expired' first (graceful handling)
    3. Removes them from active sessions
    """
    now = get_tehran_time()
    with TimedLock(state.lock, timeout=30, name="cleanup_old_sessions"):
        expired_sessions = [
            sid for sid, session in state.user_sessions.items()
            if (now - session['last_accessed']).total_seconds() / 3600 > max_age_hours
        ]

        if expired_sessions:
            logger.info(f"🧹 SESSION CLEANUP: Found {len(expired_sessions)} expired session(s)")

            for sid in expired_sessions:
                session = state.user_sessions[sid]
                age_hours = (now - session['last_accessed']).total_seconds() / 3600
                vendor_count = len(session.get('vendor_codes', set()))

                # Mark as expired before removal for graceful handling
                session['status'] = 'expired'
                session['cleaned_up_at'] = now

                # Remove the expired session
                del state.user_sessions[sid]
                logger.info(
                    f"   🗑️  Removed session {sid[:8]}... | "
                    f"Age: {age_hours:.1f}h | "
                    f"Vendors: {vendor_count} | "
                    f"Status: expired"
                )

            logger.info(f"✅ Cleanup complete: {len(expired_sessions)} expired session(s) removed")
        else:
            logger.debug(f"✅ SESSION CLEANUP: No expired sessions found")

# =============================================================================
# CENTRALIZED DATA FETCHING (WITH ERROR HANDLING)
# =============================================================================

def fetch_all_data_with_error_handling() -> Dict[str, bool]:
    """
    Fetch all data from Metabase and store in centralized cache.
    This function NEVER crashes - it logs errors and continues.

    Returns:
        Dict with success status for each dataset
    """
    results = {
        'discount_stock': False,
        'vendor_status': False,
        'vendor_product_status': False
    }

    # Fetch discount stock
    try:
        logger.info("🔄 Fetching discount stock data (centralized)...")
        df = fetch_discount_stock()
        with TimedLock(state.lock, timeout=30, name="fetch_discount_stock_update"):
            state.full_discount_stock_data = df
            state.last_fetch_times['discount_stock'] = get_tehran_time()
            state.fetch_errors['discount_stock'] = None
        results['discount_stock'] = True
        logger.info(f"✅ Discount stock: {len(df)} rows fetched")
    except Exception as e:
        error_msg = f"Failed to fetch discount stock: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        with TimedLock(state.lock, timeout=30, name="fetch_discount_stock_error"):
            state.fetch_errors['discount_stock'] = error_msg

    # Fetch vendor status
    try:
        logger.info("🔄 Fetching vendor status data (centralized)...")
        df = fetch_vendor_status()
        with TimedLock(state.lock, timeout=30, name="fetch_vendor_status_update"):
            state.full_vendor_status_data = df
            state.last_fetch_times['vendor_status'] = get_tehran_time()
            state.fetch_errors['vendor_status'] = None
        results['vendor_status'] = True
        logger.info(f"✅ Vendor status: {len(df)} rows fetched")
    except Exception as e:
        error_msg = f"Failed to fetch vendor status: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        with TimedLock(state.lock, timeout=30, name="fetch_vendor_status_error"):
            state.fetch_errors['vendor_status'] = error_msg

    # Fetch vendor product status
    try:
        logger.info("🔄 Fetching vendor product status data (centralized)...")
        df = fetch_vendor_product_status()
        with TimedLock(state.lock, timeout=30, name="fetch_vendor_product_status_update"):
            state.full_vendor_product_status_data = df
            state.last_fetch_times['vendor_product_status'] = get_tehran_time()
            state.fetch_errors['vendor_product_status'] = None
        results['vendor_product_status'] = True
        logger.info(f"✅ Vendor product status: {len(df)} rows fetched")
    except Exception as e:
        error_msg = f"Failed to fetch vendor product status: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        with TimedLock(state.lock, timeout=30, name="fetch_vendor_product_status_error"):
            state.fetch_errors['vendor_product_status'] = error_msg

    return results

def perform_initial_fetch() -> bool:
    """
    Perform initial data fetch on startup.
    This must complete before the application serves requests.

    Returns:
        True if at least one dataset was fetched successfully
    """
    logger.info("=" * 70)
    logger.info("🚀 INITIAL DATA FETCH - Loading data before serving requests...")
    logger.info("=" * 70)

    results = fetch_all_data_with_error_handling()

    success_count = sum(results.values())
    total_count = len(results)

    with TimedLock(state.lock, timeout=30, name="perform_initial_fetch_complete"):
        state.initial_fetch_complete = True

    if success_count == total_count:
        logger.info("=" * 70)
        logger.info(f"✅ INITIAL FETCH COMPLETE - All {total_count} datasets loaded successfully")
        logger.info("=" * 70)
        return True
    elif success_count > 0:
        logger.warning("=" * 70)
        logger.warning(f"⚠️  INITIAL FETCH PARTIAL - {success_count}/{total_count} datasets loaded")
        logger.warning("=" * 70)
        return True
    else:
        logger.error("=" * 70)
        logger.error("❌ INITIAL FETCH FAILED - No datasets loaded")
        logger.error("Application will start but data will be empty until background jobs succeed")
        logger.error("=" * 70)
        return False

# =============================================================================
# DATA FETCHING FUNCTIONS (Original)
# =============================================================================

def fetch_discount_stock() -> pd.DataFrame:
    """Fetch discount stock data from Metabase"""
    return fetch_question_data(
        question_id=QUESTION_ID_DISCOUNT_STOCK,
        metabase_url=METABASE_URL,
        username=METABASE_USERNAME,
        password=METABASE_PASSWORD,
        database=METABASE_DATABASE,
        page_size=METABASE_PAGE_SIZE
    )

def fetch_vendor_status() -> pd.DataFrame:
    """Fetch vendor status data from Metabase"""
    return fetch_question_data(
        question_id=QUESTION_ID_VENDOR_STATUS,
        metabase_url=METABASE_URL,
        username=METABASE_USERNAME,
        password=METABASE_PASSWORD,
        database=METABASE_DATABASE,
        page_size=METABASE_PAGE_SIZE
    )

def fetch_vendor_product_status() -> pd.DataFrame:
    """Fetch vendor product status data from Metabase (base CTE only)"""
    base_df = fetch_question_data(
        question_id=QUESTION_ID_VENDOR_PRODUCT_STATUS,
        metabase_url=METABASE_URL,
        username=METABASE_USERNAME,
        password=METABASE_PASSWORD,
        database=METABASE_DATABASE,
        page_size=METABASE_PAGE_SIZE
    )

    # Process the base CTE data to calculate vendor-level aggregations
    if base_df.empty:
        return pd.DataFrame()

    return calculate_vendor_product_status(base_df)

def calculate_vendor_product_status(base_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate vendor product status from base CTE data.

    Input columns (from base CTE):
        - vendor_code
        - vendor_name
        - business_line
        - vendor_product_header_name
        - vendor_product_id
        - product_stock (0 or positive)
        - is_visible (0 or 1)

    Output columns:
        - vendor_code
        - vendor_name
        - business_line
        - total_headers
        - visibility_issue_headers
        - stock_issue_headers
        - visibility_issue_rate
        - stock_issue_rate
        - size_quantile
        - vendor_visibility_status
        - vendor_stock_status
    """
    import numpy as np

    logger.info(f"📊 Starting vendor product status calculation for {len(base_df)} rows...")

    # Step 1: Pre-calculate flags for efficiency
    base_df['has_stock_issue'] = (base_df['product_stock'] == 0).astype(int)
    base_df['has_visibility_issue'] = (base_df['is_visible'] == 0).astype(int)
    base_df['has_pure_stock_issue'] = ((base_df['is_visible'] == 1) & (base_df['product_stock'] == 0)).astype(int)

    logger.info("   ✓ Calculated issue flags")

    # Step 2: header_issues - Calculate issues per header (optimized)
    header_issues = base_df.groupby(
        ['vendor_code', 'vendor_name', 'business_line', 'vendor_product_header_name'],
        as_index=False
    ).agg({
        'has_stock_issue': 'max',
        'has_visibility_issue': 'max',
        'has_pure_stock_issue': 'max'
    }).rename(columns={
        'has_stock_issue': 'stock_issue',
        'has_visibility_issue': 'visibility_issue',
        'has_pure_stock_issue': 'pure_stock_issue'
    })

    logger.info(f"   ✓ Calculated header issues for {len(header_issues)} headers")

    # Step 3: vendor_agg - Aggregate to vendor level
    vendor_agg = header_issues.groupby(
        ['vendor_code', 'vendor_name', 'business_line'],
        as_index=False
    ).agg({
        'vendor_product_header_name': 'count',
        'visibility_issue': 'sum',
        'pure_stock_issue': 'sum'
    }).rename(columns={
        'vendor_product_header_name': 'total_headers',
        'visibility_issue': 'visibility_issue_headers',
        'pure_stock_issue': 'stock_issue_headers'
    })

    logger.info(f"   ✓ Aggregated to {len(vendor_agg)} vendors")

    # Step 4: bounds - Calculate quantiles
    if len(vendor_agg) > 0:
        # Use method='linear' which is universally supported and close to ClickHouse's quantileExactInclusive
        q20 = np.quantile(vendor_agg['total_headers'], 0.2, method='linear')
        q40 = np.quantile(vendor_agg['total_headers'], 0.4, method='linear')
        q60 = np.quantile(vendor_agg['total_headers'], 0.6, method='linear')
        q80 = np.quantile(vendor_agg['total_headers'], 0.8, method='linear')
        logger.info(f"   ✓ Calculated quantiles: Q20={q20}, Q40={q40}, Q60={q60}, Q80={q80}")
    else:
        q20 = q40 = q60 = q80 = 0

    # Step 5: scored - Assign size quantiles and thresholds (vectorized)
    conditions = [
        vendor_agg['total_headers'] <= q20,
        vendor_agg['total_headers'] <= q40,
        vendor_agg['total_headers'] <= q60,
        vendor_agg['total_headers'] <= q80
    ]
    quantile_choices = ['Q1', 'Q2', 'Q3', 'Q4']
    threshold_choices = [0.35, 0.25, 0.20, 0.15]

    vendor_agg['size_quantile'] = np.select(conditions, quantile_choices, default='Q5')
    vendor_agg['threshold_pct'] = np.select(conditions, threshold_choices, default=0.10)

    logger.info("   ✓ Assigned quantiles and thresholds")

    # Step 6: Final calculations (vectorized)
    vendor_agg['visibility_issue_rate'] = (
        vendor_agg['visibility_issue_headers'] / vendor_agg['total_headers']
    ).fillna(0).round(4)

    vendor_agg['stock_issue_rate'] = (
        vendor_agg['stock_issue_headers'] / vendor_agg['total_headers']
    ).fillna(0).round(4)

    # Calculate threshold counts
    vendor_agg['visibility_threshold'] = np.ceil(
        vendor_agg['total_headers'] * vendor_agg['threshold_pct']
    ).astype(int)

    vendor_agg['stock_threshold'] = np.ceil(
        vendor_agg['total_headers'] * vendor_agg['threshold_pct']
    ).astype(int)

    logger.info("   ✓ Calculated rates and thresholds")

    # Determine vendor status (vectorized)
    vendor_agg['vendor_visibility_status'] = np.where(
        vendor_agg['visibility_issue_headers'] >= vendor_agg['visibility_threshold'],
        'vendor_product_visibility_issue',
        'visibility_good'
    )

    vendor_agg['vendor_stock_status'] = np.where(
        vendor_agg['stock_issue_headers'] >= vendor_agg['stock_threshold'],
        'vendor_stock_issue',
        'stock_good'
    )

    logger.info("   ✓ Determined vendor statuses")

    # Drop intermediate columns
    vendor_agg = vendor_agg.drop(columns=['threshold_pct', 'visibility_threshold', 'stock_threshold'])

    logger.info(f"✅ Vendor product status calculation completed: {len(vendor_agg)} vendors")

    return vendor_agg

def filter_by_vendor_codes(df: pd.DataFrame, vendor_column: str = 'vendor_code') -> pd.DataFrame:
    """Filter DataFrame by uploaded vendor codes"""
    if state.vendor_codes and not df.empty:
        if vendor_column in df.columns:
            return df[df[vendor_column].isin(state.vendor_codes)]
    return df if not state.vendor_codes else pd.DataFrame()

def process_discount_stock_alerts(df: pd.DataFrame, session_id: Optional[str] = None):
    """
    Process discount stock data and generate alerts with key-based updates.

    Args:
        df: DataFrame with discount stock data
        session_id: Optional session ID to emit alerts to specific room (prevents data leaks)
    """
    if df.empty:
        return

    with TimedLock(state.lock, timeout=30, name="process_discount_stock_alerts"):
        current_keys = set()

        for _, row in df.iterrows():
            discount_stock = int(row.get('discount_stock', 0))
            product_stock = float(row.get('product_stock', 0))
            product_id = create_product_id(row)
            current_keys.add(product_id)
            
            alert = None
            alert_status = 'cleared'
            
            # Check if item is fixed: product_stock > 0 AND discount_stock > 3
            if product_stock > 0 and discount_stock > 3:
                # Item is fixed - clear alert if it exists
                if product_id in state.alerts['discount_stock']:
                    cleared_alert = state.alerts['discount_stock'][product_id].copy()
                    cleared_alert['status'] = 'cleared'
                    cleared_alert['severity'] = SEVERITY_GREEN
                    cleared_alert['cleared_at'] = format_tehran_time()
                    cleared_alert['alert_type'] = ALERT_TYPE_DISCOUNTED_ITEM_FIXED
                    cleared_alert['product_stock'] = int(product_stock)
                    cleared_alert['discount_stock'] = discount_stock
                    state.cleared_alerts['discount_stock'][product_id] = cleared_alert

                    # Remove from active alerts
                    del state.alerts['discount_stock'][product_id]

                    # Emit to specific session room if provided, otherwise broadcast
                    if session_id:
                        socketio.emit('alert_cleared', {
                            'tab': 'discount_stock',
                            'product_id': product_id,
                            'alert': cleared_alert
                        }, room=session_id)
                    else:
                        socketio.emit('alert_cleared', {
                            'tab': 'discount_stock',
                            'product_id': product_id,
                            'alert': cleared_alert
                        })
            # Rule 1: Product stock finished
            elif discount_stock > 0 and product_stock == 0:
                alert_status = 'active'
                severity = SEVERITY_CHERRY  # Cherry color for finished stock
                alert = {
                    'product_id': product_id,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_PRODUCT_STOCK_FINISHED,
                    'vendor_code': row.get('vendor_code', 'N/A'),
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'discount_stock': discount_stock,
                    'product_stock': int(product_stock) if product_stock == product_stock else 0,
                    'product_discount_ratio': row.get('product_discount_ratio', 0),
                    'product_header_name': row.get('vendor_product_header_name', 'N/A'),
                    'product_name': row.get('product_name', 'N/A'),
                    'discount_start_at': format_datetime(row.get('discount_start_at', 'N/A')),
                    'discount_end_at': format_datetime(row.get('discount_end_at', 'N/A')),
                    'status': alert_status,
                    'severity': severity
                }
            # Rule 2: Discount stock near end (1-3 range)
            elif discount_stock > 0 and discount_stock <= DISCOUNT_STOCK_NEAR_END_THRESHOLD and product_stock > 0:
                alert_status = 'active'
                severity = calculate_discount_severity(discount_stock)
                alert = {
                    'product_id': product_id,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_DISCOUNT_STOCK_NEAR_END,
                    'vendor_code': row.get('vendor_code', 'N/A'),
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'discount_stock': discount_stock,
                    'product_stock': int(product_stock) if product_stock == product_stock else 0,
                    'product_discount_ratio': row.get('product_discount_ratio', 0),
                    'product_header_name': row.get('vendor_product_header_name', 'N/A'),
                    'product_name': row.get('product_name', 'N/A'),
                    'discount_start_at': format_datetime(row.get('discount_start_at', 'N/A')),
                    'discount_end_at': format_datetime(row.get('discount_end_at', 'N/A')),
                    'status': alert_status,
                    'severity': severity
                }
            
            if alert:
                # Check if this is a new alert or update
                is_new = product_id not in state.alerts['discount_stock']

                state.alerts['discount_stock'][product_id] = alert

                # Emit to specific session room if provided, otherwise broadcast
                event_name = 'new_alert' if is_new else 'update_alert'
                event_data = {
                    'tab': 'discount_stock',
                    'alert': alert,
                    'is_new': is_new
                }
                if session_id:
                    socketio.emit(event_name, event_data, room=session_id)
                else:
                    socketio.emit(event_name, event_data)
        
        # Check for alerts that are no longer in the data (removed products)
        for product_id in list(state.alerts['discount_stock'].keys()):
            if product_id not in current_keys:
                cleared_alert = state.alerts['discount_stock'][product_id].copy()
                cleared_alert['status'] = 'cleared'
                cleared_alert['severity'] = SEVERITY_GREEN
                cleared_alert['cleared_at'] = format_tehran_time()
                cleared_alert['alert_type'] = ALERT_TYPE_DISCOUNTED_ITEM_FIXED
                state.cleared_alerts['discount_stock'][product_id] = cleared_alert
                del state.alerts['discount_stock'][product_id]

                # Emit to specific session room if provided, otherwise broadcast
                if session_id:
                    socketio.emit('alert_cleared', {
                        'tab': 'discount_stock',
                        'product_id': product_id,
                        'alert': cleared_alert
                    }, room=session_id)
                else:
                    socketio.emit('alert_cleared', {
                        'tab': 'discount_stock',
                        'product_id': product_id,
                        'alert': cleared_alert
                    })

def process_vendor_status_alerts(df: pd.DataFrame, session_id: Optional[str] = None):
    """
    Process vendor status data and generate alerts with key-based updates.

    Args:
        df: DataFrame with vendor status data
        session_id: Optional session ID to emit alerts to specific room (prevents data leaks)
    """
    if df.empty:
        return

    with TimedLock(state.lock, timeout=30, name="process_vendor_status_alerts"):
        current_keys = set()

        for _, row in df.iterrows():
            vendor_code = row.get('vendor_code', 'N/A')
            current_status = row.get('vendor_status', '')
            previous_status = state.previous_vendor_status.get(vendor_code, '')
            current_keys.add(vendor_code)

            alert = None
            alert_status = 'cleared'

            # Rule 1: Vendor transitioned from active to inactive - RED alert
            if (previous_status == VENDOR_STATUS_ACTIVE and
                current_status == VENDOR_STATUS_INACTIVE):
                alert_status = 'active'
                alert = {
                    'vendor_code': vendor_code,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_VENDOR_DEACTIVATED,
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'vendor_status': current_status,
                    'status': alert_status,
                    'severity': SEVERITY_RED
                }
            # Rule 2: Vendor is not active (persistent or initial state) - YELLOW alert
            elif current_status == VENDOR_STATUS_INACTIVE:
                # Check if we already have a red alert for this vendor
                existing_alert = state.alerts['vendor_status'].get(vendor_code)
                if existing_alert and existing_alert.get('severity') == SEVERITY_RED:
                    # Keep the red alert (don't downgrade to yellow)
                    alert = existing_alert
                else:
                    # Create or update yellow alert
                    alert_status = 'active'
                    alert = {
                        'vendor_code': vendor_code,
                        'time': format_tehran_time(),
                        'alert_type': ALERT_TYPE_VENDOR_NOT_ACTIVE,
                        'vendor_name': row.get('vendor_name', 'N/A'),
                        'vendor_status': current_status,
                        'status': alert_status,
                        'severity': SEVERITY_YELLOW
                    }
            # Rule 3: Vendor became active - Clear the alert
            elif current_status == VENDOR_STATUS_ACTIVE:
                if vendor_code in state.alerts['vendor_status']:
                    existing_alert = state.alerts['vendor_status'][vendor_code]
                    cleared_alert = existing_alert.copy()
                    cleared_alert['status'] = 'cleared'
                    cleared_alert['severity'] = SEVERITY_GREEN
                    cleared_alert['cleared_at'] = format_tehran_time()
                    cleared_alert['alert_type'] = ALERT_TYPE_VENDOR_ACTIVATED
                    cleared_alert['vendor_status'] = current_status
                    state.cleared_alerts['vendor_status'][vendor_code] = cleared_alert
                    del state.alerts['vendor_status'][vendor_code]

                    # Emit to specific session room if provided, otherwise broadcast
                    if session_id:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_status',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        }, room=session_id)
                    else:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_status',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        })

            if alert:
                is_new = vendor_code not in state.alerts['vendor_status']
                state.alerts['vendor_status'][vendor_code] = alert

                # Emit to specific session room if provided, otherwise broadcast
                event_name = 'new_alert' if is_new else 'update_alert'
                event_data = {
                    'tab': 'vendor_status',
                    'alert': alert,
                    'is_new': is_new
                }
                if session_id:
                    socketio.emit(event_name, event_data, room=session_id)
                else:
                    socketio.emit(event_name, event_data)

            # Update state
            state.previous_vendor_status[vendor_code] = current_status

def process_vendor_product_status_alerts(df: pd.DataFrame, session_id: Optional[str] = None):
    """
    Process vendor product status data and generate alerts with key-based updates.

    Args:
        df: DataFrame with vendor product status data
        session_id: Optional session ID to emit alerts to specific room (prevents data leaks)
    """
    if df.empty:
        return

    with TimedLock(state.lock, timeout=30, name="process_vendor_product_status_alerts"):
        current_keys = set()

        for _, row in df.iterrows():
            vendor_code = row.get('vendor_code', 'N/A')
            business_line = row.get('business_line', 'N/A')
            key = f"{vendor_code}_{business_line}"
            current_keys.add(key)
            
            current_stock_status = row.get('vendor_stock_status', '')
            current_visibility_status = row.get('vendor_visibility_status', '')
            
            previous = state.previous_product_status.get(key, {})
            prev_stock_status = previous.get('stock_status', '')
            prev_visibility_status = previous.get('visibility_status', '')
            
            # Stock alerts
            stock_alert = None
            stock_status = 'cleared'
            
            if prev_stock_status == PRODUCT_STATUS_STOCK_GOOD and current_stock_status == PRODUCT_STATUS_STOCK_ISSUE:
                stock_status = 'active'
                stock_alert = {
                    'vendor_code': vendor_code,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_STOCK_ISSUES_NEW,
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'business_line': business_line,
                    'total_p_headers': row.get('total_headers', 0),
                    'stock_issues': row.get('stock_issue_headers', 0),
                    'stock_rate': format_percentage(row.get('stock_issue_rate', 0.0)),
                    'severity': SEVERITY_RED,
                    'status': stock_status
                }
            elif (prev_stock_status == PRODUCT_STATUS_STOCK_ISSUE and
                  current_stock_status == PRODUCT_STATUS_STOCK_ISSUE):
                stock_status = 'active'
                stock_alert = {
                    'vendor_code': vendor_code,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_STOCK_ISSUES_PERSISTENT,
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'business_line': business_line,
                    'total_p_headers': row.get('total_headers', 0),
                    'stock_issues': row.get('stock_issue_headers', 0),
                    'stock_rate': format_percentage(row.get('stock_issue_rate', 0.0)),
                    'severity': SEVERITY_YELLOW,
                    'status': stock_status
                }
            elif (prev_stock_status == PRODUCT_STATUS_STOCK_ISSUE and
                  current_stock_status == PRODUCT_STATUS_STOCK_GOOD):
                # Stock issue cleared
                if key in state.alerts['vendor_product_stock']:
                    cleared_alert = state.alerts['vendor_product_stock'][key].copy()
                    cleared_alert['status'] = 'cleared'
                    cleared_alert['severity'] = SEVERITY_GREEN
                    cleared_alert['cleared_at'] = format_tehran_time()
                    cleared_alert['alert_type'] = ALERT_TYPE_STOCK_ISSUES_FIXED
                    state.cleared_alerts['vendor_product_stock'][key] = cleared_alert
                    del state.alerts['vendor_product_stock'][key]

                    # Emit to specific session room if provided, otherwise broadcast
                    if session_id:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_product_stock',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        }, room=session_id)
                    else:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_product_stock',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        })

            if stock_alert:
                is_new = key not in state.alerts['vendor_product_stock']
                state.alerts['vendor_product_stock'][key] = stock_alert

                # Emit to specific session room if provided, otherwise broadcast
                event_name = 'new_alert' if is_new else 'update_alert'
                event_data = {
                    'tab': 'vendor_product_stock',
                    'alert': stock_alert,
                    'is_new': is_new
                }
                if session_id:
                    socketio.emit(event_name, event_data, room=session_id)
                else:
                    socketio.emit(event_name, event_data)
            
            # Visibility alerts
            visibility_alert = None
            visibility_status = 'cleared'
            
            if (prev_visibility_status == PRODUCT_STATUS_VISIBILITY_GOOD and
                current_visibility_status == PRODUCT_STATUS_VISIBILITY_ISSUE):
                visibility_status = 'active'
                visibility_alert = {
                    'vendor_code': vendor_code,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_VISIBILITY_ISSUES_NEW,
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'business_line': business_line,
                    'total_p_headers': row.get('total_headers', 0),
                    'visibility_issues': row.get('visibility_issue_headers', 0),
                    'visibility_rate': format_percentage(row.get('visibility_issue_rate', 0.0)),
                    'severity': SEVERITY_RED,
                    'status': visibility_status
                }
            elif (prev_visibility_status == PRODUCT_STATUS_VISIBILITY_ISSUE and
                  current_visibility_status == PRODUCT_STATUS_VISIBILITY_ISSUE):
                visibility_status = 'active'
                visibility_alert = {
                    'vendor_code': vendor_code,
                    'time': format_tehran_time(),
                    'alert_type': ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT,
                    'vendor_name': row.get('vendor_name', 'N/A'),
                    'business_line': business_line,
                    'total_p_headers': row.get('total_headers', 0),
                    'visibility_issues': row.get('visibility_issue_headers', 0),
                    'visibility_rate': format_percentage(row.get('visibility_issue_rate', 0.0)),
                    'severity': SEVERITY_YELLOW,
                    'status': visibility_status
                }
            elif (prev_visibility_status == PRODUCT_STATUS_VISIBILITY_ISSUE and
                  current_visibility_status == PRODUCT_STATUS_VISIBILITY_GOOD):
                # Visibility issue cleared
                if key in state.alerts['vendor_product_visibility']:
                    cleared_alert = state.alerts['vendor_product_visibility'][key].copy()
                    cleared_alert['status'] = 'cleared'
                    cleared_alert['severity'] = SEVERITY_GREEN
                    cleared_alert['cleared_at'] = format_tehran_time()
                    cleared_alert['alert_type'] = ALERT_TYPE_VISIBILITY_ISSUES_FIXED
                    state.cleared_alerts['vendor_product_visibility'][key] = cleared_alert
                    del state.alerts['vendor_product_visibility'][key]

                    # Emit to specific session room if provided, otherwise broadcast
                    if session_id:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_product_visibility',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        }, room=session_id)
                    else:
                        socketio.emit('alert_cleared', {
                            'tab': 'vendor_product_visibility',
                            'vendor_code': vendor_code,
                            'alert': cleared_alert
                        })

            if visibility_alert:
                is_new = key not in state.alerts['vendor_product_visibility']
                state.alerts['vendor_product_visibility'][key] = visibility_alert

                # Emit to specific session room if provided, otherwise broadcast
                event_name = 'new_alert' if is_new else 'update_alert'
                event_data = {
                    'tab': 'vendor_product_visibility',
                    'alert': visibility_alert,
                    'is_new': is_new
                }
                if session_id:
                    socketio.emit(event_name, event_data, room=session_id)
                else:
                    socketio.emit(event_name, event_data)
            
            # Update state
            state.previous_product_status[key] = {
                'stock_status': current_stock_status,
                'visibility_status': current_visibility_status
            }

def run_immediate_fetch():
    """Run all fetches immediately after vendor upload (sequential)"""
    try:
        if not state.vendor_codes:
            return

        logger.info("🚀 Starting immediate fetch after vendor upload...")

        # 1. Fetch discount stock
        logger.info("📦 Fetching discount stock data...")
        df = fetch_discount_stock()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with TimedLock(state.lock, timeout=30, name="immediate_fetch_discount_stock"):
            state.discount_stock_data = df
        process_discount_stock_alerts(df)
        logger.info("✅ Discount stock fetch completed")

        # 2. Fetch vendor status
        logger.info("👥 Fetching vendor status data...")
        df = fetch_vendor_status()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with TimedLock(state.lock, timeout=30, name="immediate_fetch_vendor_status"):
            state.vendor_status_data = df
        process_vendor_status_alerts(df)
        socketio.emit('stats_update', get_vendor_status_stats())
        logger.info("✅ Vendor status fetch completed")

        # 3. Fetch vendor product status (first time)
        logger.info("📊 Fetching vendor product status data (1/2)...")
        df = fetch_vendor_product_status()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with TimedLock(state.lock, timeout=30, name="immediate_fetch_vendor_product_1"):
            state.vendor_product_status_data = df
        process_vendor_product_status_alerts(df)
        socketio.emit('stats_update', get_vendor_product_stats())
        logger.info("✅ Vendor product status fetch 1/2 completed")

        # 4. Wait 10 seconds before second fetch
        logger.info("⏳ Waiting 10 seconds before second vendor product status fetch...")
        time.sleep(10)

        # 5. Fetch vendor product status (second time)
        logger.info("📊 Fetching vendor product status data (2/2)...")
        df = fetch_vendor_product_status()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with TimedLock(state.lock, timeout=30, name="immediate_fetch_vendor_product_2"):
            state.vendor_product_status_data = df
        process_vendor_product_status_alerts(df)
        socketio.emit('stats_update', get_vendor_product_stats())
        logger.info("✅ Vendor product status fetch 2/2 completed")

        logger.info("🎉 All immediate fetches completed successfully!")

    except Exception as e:
        logger.error(f"❌ Error in immediate fetch: {e}")

def update_all_sessions_with_new_data():
    """
    After a scheduled fetch, update all ACTIVE (connected) sessions with new filtered data.
    Gracefully skips expired/disconnected sessions - does NOT send updates to them.
    Process alerts and emit updates via WebSocket for each connected session.
    """
    with TimedLock(state.lock, timeout=30, name="update_all_sessions_get_sessions"):
        active_sessions = list(state.user_sessions.items())

    if not active_sessions:
        logger.info("📭 No sessions to process")
        return

    # Filter out expired/disconnected sessions
    connected_sessions = []
    expired_sessions = []

    for session_id, session_data in active_sessions:
        # Skip sessions that are expired or disconnected
        if session_data.get('status') in ['expired', 'disconnected']:
            expired_sessions.append((session_id, session_data))
        else:
            connected_sessions.append((session_id, session_data))

    if expired_sessions:
        logger.info(f"⏭️  Skipping {len(expired_sessions)} disconnected/expired session(s) - NOT sending updates")
        for session_id, _ in expired_sessions:
            logger.debug(f"   ⏸️  Skipped: {session_id[:8]}... (status: {_['status']})")

    if not connected_sessions:
        logger.info("📭 No connected sessions to update")
        return

    logger.info(f"🔄 Updating {len(connected_sessions)} connected session(s) with new data...")

    for session_id, session_data in connected_sessions:
        try:
            # Skip if session is not active
            if session_data.get('status') != 'active':
                logger.debug(f"   ⏸️  Skipped session {session_id[:8]}... (status: {session_data.get('status')})")
                continue

            vendor_codes = session_data['vendor_codes']

            # Filter new data for this session
            filtered_data = filter_data_for_session(session_id)

            # CRITICAL FIX: Do NOT update global state - each session is isolated
            # Process alerts and emit ONLY to this session's WebSocket room
            if not filtered_data['discount_stock'].empty:
                process_discount_stock_alerts(filtered_data['discount_stock'], session_id=session_id)
            if not filtered_data['vendor_status'].empty:
                process_vendor_status_alerts(filtered_data['vendor_status'], session_id=session_id)
                # Emit stats ONLY to this session's room (with session-specific data)
                socketio.emit('stats_update', get_vendor_status_stats(
                    vendor_codes=vendor_codes,
                    vendor_status_df=filtered_data['vendor_status']
                ), room=session_id)
            if not filtered_data['vendor_product_status'].empty:
                process_vendor_product_status_alerts(filtered_data['vendor_product_status'], session_id=session_id)
                # Emit stats ONLY to this session's room (with session-specific data)
                socketio.emit('stats_update', get_vendor_product_stats(
                    vendor_codes=vendor_codes,
                    vendor_product_status_df=filtered_data['vendor_product_status']
                ), room=session_id)

            logger.info(f"   ✅ Updated session {session_id[:8]}... ({len(vendor_codes)} vendors)")

        except Exception as e:
            logger.error(f"   ❌ Failed to update session {session_id[:8]}...: {e}")
            logger.error(traceback.format_exc())
            # Continue with other sessions

    logger.info(f"✅ Finished updating {len(connected_sessions)} connected session(s)")

def centralized_fetch_job():
    """
    SINGLE centralized background job that:
    1. Fetches ALL data from Metabase (centralized cache)
    2. Updates all active user sessions with new filtered data
    3. Processes alerts and emits via WebSocket
    4. Cleans up old sessions

    Never crashes - logs errors and continues.
    Runs on the shortest interval of the 3 datasets.
    Uses eventlet.sleep for compatibility with eventlet green threads.
    """
    # Use the shortest interval for maximum freshness
    interval = min(DISCOUNT_STOCK_JOB_INTERVAL, VENDOR_STATUS_JOB_INTERVAL, VENDOR_PRODUCT_STATUS_JOB_INTERVAL)

    while True:
        try:
            # Update heartbeat to indicate job is alive
            with TimedLock(state.lock, timeout=30, name="centralized_fetch_job_heartbeat"):
                state.last_heartbeat = get_tehran_time()

            logger.info("=" * 70)
            logger.info(f"⏰ SCHEDULED FETCH - Starting centralized data refresh...")
            logger.info("=" * 70)

            # Fetch all data (centralized cache)
            results = fetch_all_data_with_error_handling()

            # Update all active sessions with new data
            update_all_sessions_with_new_data()

            # Cleanup old sessions periodically (5 minutes)
            # Sessions inactive for 5 minutes are automatically removed
            cleanup_old_sessions(max_age_hours=5/60)

            logger.info("=" * 70)
            logger.info(f"✅ SCHEDULED FETCH COMPLETE - Sleeping for {interval}s")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"❌ Error in centralized fetch job: {e}")
            logger.error(traceback.format_exc())
        finally:
            # CRITICAL: Use eventlet.sleep for compatibility with eventlet green threads
            eventlet.sleep(interval)

def get_vendor_status_stats(vendor_codes: Set[str] = None, vendor_status_df: pd.DataFrame = None) -> Dict:
    """
    Calculate vendor status statistics.

    Args:
        vendor_codes: Set of vendor codes for this session (if None, uses global state for backward compatibility)
        vendor_status_df: DataFrame with vendor status data (if None, uses global state)
    """
    with TimedLock(state.lock, timeout=30, name="get_vendor_status_stats"):
        # Use provided data or fall back to global state
        codes = vendor_codes if vendor_codes is not None else state.vendor_codes
        df = vendor_status_df if vendor_status_df is not None else state.vendor_status_data

        if df.empty:
            return {
                'type': 'vendor_status',
                'total_vendors': len(codes),
                'active_vendors': 0,
                'inactive_vendors': 0,
                'active_alerts': len([a for a in state.alerts['vendor_status'].values() if a.get('vendor_code') in codes]) if codes else len(state.alerts['vendor_status']),
                'cleared_alerts': len([a for a in state.cleared_alerts['vendor_status'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_status'])
            }

        active_count = len(df[df['vendor_status'] == VENDOR_STATUS_ACTIVE]) if 'vendor_status' in df.columns else 0
        inactive_count = len(df[df['vendor_status'] == VENDOR_STATUS_INACTIVE]) if 'vendor_status' in df.columns else 0

        return {
            'type': 'vendor_status',
            'total_vendors': len(codes),
            'active_vendors': active_count,
            'inactive_vendors': inactive_count,
            'active_alerts': len([a for a in state.alerts['vendor_status'].values() if a.get('vendor_code') in codes]) if codes else len(state.alerts['vendor_status']),
            'cleared_alerts': len([a for a in state.cleared_alerts['vendor_status'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_status'])
        }

def get_vendor_product_stats(vendor_codes: Set[str] = None, vendor_product_status_df: pd.DataFrame = None) -> Dict:
    """
    Calculate vendor product status statistics.

    Args:
        vendor_codes: Set of vendor codes for this session (if None, uses global state for backward compatibility)
        vendor_product_status_df: DataFrame with vendor product status data (if None, uses global state)
    """
    with TimedLock(state.lock, timeout=30, name="get_vendor_product_stats"):
        # Use provided data or fall back to global state
        codes = vendor_codes if vendor_codes is not None else state.vendor_codes
        df = vendor_product_status_df if vendor_product_status_df is not None else state.vendor_product_status_data

        if df.empty:
            return {
                'type': 'vendor_product',
                'total_vendors': len(codes),
                'business_lines': {},
                'stock_alert_counts': {'has_issues': 0, 'had_issues': 0},
                'visibility_alert_counts': {'has_issues': 0, 'had_issues': 0},
                'stock_cleared_count': len([a for a in state.cleared_alerts['vendor_product_stock'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_product_stock']),
                'visibility_cleared_count': len([a for a in state.cleared_alerts['vendor_product_visibility'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_product_visibility'])
            }

        business_lines = df.get('business_line', pd.Series()).value_counts().to_dict()

        stock_alerts = {
            'has_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                             if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_NEW and (not codes or a.get('vendor_code') in codes)]),
            'had_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                             if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_PERSISTENT and (not codes or a.get('vendor_code') in codes)])
        }

        visibility_alerts = {
            'has_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                             if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_NEW and (not codes or a.get('vendor_code') in codes)]),
            'had_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                             if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT and (not codes or a.get('vendor_code') in codes)])
        }

        return {
            'type': 'vendor_product',
            'total_vendors': len(codes),
            'business_lines': business_lines,
            'stock_alert_counts': stock_alerts,
            'visibility_alert_counts': visibility_alerts,
            'stock_cleared_count': len([a for a in state.cleared_alerts['vendor_product_stock'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_product_stock']),
            'visibility_cleared_count': len([a for a in state.cleared_alerts['vendor_product_visibility'].values() if a.get('vendor_code') in codes]) if codes else len(state.cleared_alerts['vendor_product_visibility'])
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint for Kubernetes liveness probe"""
    return jsonify({'status': 'healthy', 'timestamp': get_tehran_time().isoformat()}), 200

@app.route('/ready')
def ready():
    """Readiness check endpoint for Kubernetes readiness probe"""
    # Check if the app has essential dependencies ready
    try:
        is_ready = True
        checks = {
            'vendor_codes_initialized': isinstance(state.vendor_codes, set),
            'alerts_initialized': isinstance(state.alerts, dict),
            'locks_initialized': hasattr(state, 'lock')
        }

        if not all(checks.values()):
            is_ready = False

        return jsonify({
            'status': 'ready' if is_ready else 'not_ready',
            'checks': checks,
            'timestamp': get_tehran_time().isoformat()
        }), 200 if is_ready else 503
    except Exception as e:
        return jsonify({
            'status': 'not_ready',
            'error': str(e),
            'timestamp': get_tehran_time().isoformat()
        }), 503

@app.route('/health/background-job')
def background_job_health():
    """
    Health check endpoint for background job monitoring.
    Returns 503 if the background job hasn't sent a heartbeat in >5 minutes.

    This detects if the background fetch job has frozen/died.
    """
    try:
        with TimedLock(state.lock, timeout=30, name="heartbeat_check"):
            last_heartbeat = state.last_heartbeat
            last_fetch_times = state.last_fetch_times.copy()

        now = get_tehran_time()
        seconds_since_heartbeat = (now - last_heartbeat).total_seconds()

        # Background job runs every 180s, so 5 minutes (300s) is a reasonable threshold
        # 300s = 180s interval + 120s buffer for Metabase query time
        is_healthy = seconds_since_heartbeat < 300

        response = {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'background_job': {
                'last_heartbeat': last_heartbeat.isoformat(),
                'seconds_since_heartbeat': round(seconds_since_heartbeat, 1),
                'threshold_seconds': 300,
                'is_alive': is_healthy
            },
            'last_fetch_times': {
                k: v.isoformat() if v else None
                for k, v in last_fetch_times.items()
            },
            'timestamp': now.isoformat()
        }

        if not is_healthy:
            logger.warning(f"⚠️  Background job health check FAILED: {seconds_since_heartbeat:.1f}s since last heartbeat")

        return jsonify(response), 200 if is_healthy else 503

    except Exception as e:
        logger.error(f"❌ Error in background job health check: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': get_tehran_time().isoformat()
        }), 500

@app.route('/api/upload-vendors', methods=['POST'])
def upload_vendors():
    """
    Upload vendor codes and create/update user session.
    This endpoint NEVER crashes - it handles all errors gracefully.

    Returns:
        - session_id: Unique identifier for this user's session
        - count: Number of vendor codes uploaded
        - data: Filtered data for the uploaded vendor codes (from cache)
        - cache_status: Information about data freshness
    """
    try:
        # ========== STEP 1: Parse vendor codes from request ==========
        vendors = []
        parse_error = None

        try:
            if 'file' in request.files:
                file = request.files['file']
                if not file or not file.filename:
                    return jsonify({'error': 'No file selected'}), 400

                # Try to read and parse CSV file
                try:
                    content = file.read().decode('utf-8')
                    vendors = [line.strip() for line in content.split('\n') if line.strip()]
                except UnicodeDecodeError:
                    try:
                        file.seek(0)
                        content = file.read().decode('utf-8-sig')  # Try with BOM
                        vendors = [line.strip() for line in content.split('\n') if line.strip()]
                    except Exception as e:
                        return jsonify({'error': f'Failed to decode file: {str(e)}. Please ensure it is a UTF-8 encoded text file.'}), 400

            elif request.is_json and 'vendors' in request.json:
                vendors = request.json['vendors']
                if not isinstance(vendors, list):
                    return jsonify({'error': 'vendors must be a list'}), 400
            else:
                return jsonify({'error': 'No vendor data provided. Please upload a file or send vendors as JSON.'}), 400

        except Exception as e:
            logger.error(f"Error parsing vendor upload: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'error': f'Failed to parse vendor data: {str(e)}'}), 400

        if not vendors:
            return jsonify({'error': 'No vendor codes found in uploaded data'}), 400

        vendor_codes_set = set(vendors)
        logger.info(f"📤 Parsed {len(vendor_codes_set)} unique vendor codes from upload")

        # ========== STEP 2: Check if initial fetch completed ==========
        if not state.initial_fetch_complete:
            logger.warning("⚠️  Upload received before initial fetch completed")
            return jsonify({
                'error': 'Application is still loading data. Please try again in a few moments.',
                'initial_fetch_complete': False
            }), 503

        # ========== STEP 3: Create user session ==========
        try:
            session_id = create_user_session(vendor_codes_set)
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'error': f'Failed to create session: {str(e)}'}), 500

        # ========== STEP 4: Filter data from cache ==========
        try:
            filtered_data = filter_data_for_session(session_id)
        except Exception as e:
            logger.error(f"Failed to filter data for session {session_id}: {e}")
            logger.error(traceback.format_exc())
            return jsonify({
                'error': f'Failed to filter data: {str(e)}',
                'session_id': session_id,
                'count': len(vendor_codes_set)
            }), 500

        # ========== STEP 4.5: Process alerts for THIS SESSION ONLY ==========
        # CRITICAL FIX: Do NOT update global state - each session is isolated
        # Process alerts and emit ONLY to this session's WebSocket room
        try:
            logger.info(f"📊 Processing alerts for session {session_id[:8]}... (isolated)")
            if not filtered_data['discount_stock'].empty:
                process_discount_stock_alerts(filtered_data['discount_stock'], session_id=session_id)
            if not filtered_data['vendor_status'].empty:
                process_vendor_status_alerts(filtered_data['vendor_status'], session_id=session_id)
                # Emit stats ONLY to this session's room (with session-specific data)
                socketio.emit('stats_update', get_vendor_status_stats(
                    vendor_codes=vendor_codes_set,
                    vendor_status_df=filtered_data['vendor_status']
                ), room=session_id)
            if not filtered_data['vendor_product_status'].empty:
                process_vendor_product_status_alerts(filtered_data['vendor_product_status'], session_id=session_id)
                # Emit stats ONLY to this session's room (with session-specific data)
                socketio.emit('stats_update', get_vendor_product_stats(
                    vendor_codes=vendor_codes_set,
                    vendor_product_status_df=filtered_data['vendor_product_status']
                ), room=session_id)
            logger.info(f"✅ Alerts processed and emitted to session {session_id[:8]}... (isolated)")
        except Exception as e:
            logger.error(f"Failed to process alerts for session {session_id[:8]}...: {e}")
            logger.error(traceback.format_exc())
            # Continue even if alert processing fails

        # ========== STEP 5: Prepare response with ACTUAL DATA ==========
        # CRITICAL: Return data in HTTP response instead of relying on WebSocket
        # WebSocket may not be connected yet, so HTTP response ensures data delivery
        response = {
            'success': True,
            'session_id': session_id,
            'count': len(vendor_codes_set),
            'cache_status': {
                'last_fetch_times': {
                    k: v.isoformat() if v else None
                    for k, v in state.last_fetch_times.items()
                },
                'fetch_errors': state.fetch_errors,
                'data_counts': {
                    'discount_stock': len(filtered_data['discount_stock']),
                    'vendor_status': len(filtered_data['vendor_status']),
                    'vendor_product_status': len(filtered_data['vendor_product_status'])
                }
            },
            # Include actual data in response for immediate frontend rendering
            'data': {
                'discount_stock': filtered_data['discount_stock'].to_dict(orient='records') if not filtered_data['discount_stock'].empty else [],
                'vendor_status': filtered_data['vendor_status'].to_dict(orient='records') if not filtered_data['vendor_status'].empty else [],
                'vendor_product_status': filtered_data['vendor_product_status'].to_dict(orient='records') if not filtered_data['vendor_product_status'].empty else []
            },
            # Include alerts in response for immediate display (FILTERED for this session only)
            'alerts': {
                'discount_stock': [
                    alert for alert in state.alerts['discount_stock'].values()
                    if alert.get('vendor_code') in vendor_codes_set
                ],
                'vendor_status': [
                    alert for alert in state.alerts['vendor_status'].values()
                    if alert.get('vendor_code') in vendor_codes_set
                ],
                'vendor_product_stock': [
                    alert for alert in state.alerts['vendor_product_stock'].values()
                    if alert.get('vendor_code') in vendor_codes_set
                ],
                'vendor_product_visibility': [
                    alert for alert in state.alerts['vendor_product_visibility'].values()
                    if alert.get('vendor_code') in vendor_codes_set
                ]
            },
            # Include stats for summary displays (vendor status and product status)
            'stats': {
                'vendor_status': {
                    'total_vendors': len(vendor_codes_set),
                    'active_vendors': len(filtered_data['vendor_status'][filtered_data['vendor_status']['vendor_status'] == VENDOR_STATUS_ACTIVE]) if not filtered_data['vendor_status'].empty and 'vendor_status' in filtered_data['vendor_status'].columns else 0,
                    'inactive_vendors': len(filtered_data['vendor_status'][filtered_data['vendor_status']['vendor_status'] == VENDOR_STATUS_INACTIVE]) if not filtered_data['vendor_status'].empty and 'vendor_status' in filtered_data['vendor_status'].columns else 0,
                    'active_alerts': len([a for a in state.alerts['vendor_status'].values() if a.get('vendor_code') in vendor_codes_set]),
                    'cleared_alerts': len([a for a in state.cleared_alerts['vendor_status'].values() if a.get('vendor_code') in vendor_codes_set])
                },
                'vendor_product': {
                    'total_vendors': len(vendor_codes_set),
                    'business_lines': filtered_data['vendor_product_status'].get('business_line', pd.Series()).value_counts().to_dict() if not filtered_data['vendor_product_status'].empty else {},
                    'stock_alert_counts': {
                        'has_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                                          if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_NEW and a.get('vendor_code') in vendor_codes_set]),
                        'had_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                                          if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_PERSISTENT and a.get('vendor_code') in vendor_codes_set])
                    },
                    'visibility_alert_counts': {
                        'has_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                                          if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_NEW and a.get('vendor_code') in vendor_codes_set]),
                        'had_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                                          if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT and a.get('vendor_code') in vendor_codes_set])
                    },
                    'stock_cleared_count': len([a for a in state.cleared_alerts['vendor_product_stock'].values() if a.get('vendor_code') in vendor_codes_set]),
                    'visibility_cleared_count': len([a for a in state.cleared_alerts['vendor_product_visibility'].values() if a.get('vendor_code') in vendor_codes_set])
                }
            },
            'message': f'Successfully uploaded {len(vendor_codes_set)} vendor codes and filtered data from cache'
        }

        logger.info(f"✅ Session {session_id[:8]}...: Uploaded {len(vendor_codes_set)} vendors, returned {response['cache_status']['data_counts']} filtered rows")

        return jsonify(response), 200

    except Exception as e:
        # Catch-all error handler - this should never crash the app
        logger.error(f"❌ Unexpected error in upload_vendors: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': 'An unexpected error occurred while processing your upload',
            'details': str(e)
        }), 500

@app.route('/api/get-session-data/<session_id>', methods=['GET'])
def get_session_data(session_id):
    """
    Get filtered data for a specific user session.
    This allows users to retrieve their data without re-uploading.

    Returns:
        - Filtered data for the session's vendor codes
        - Cache status information
    """
    try:
        # Check if session exists
        vendor_codes = get_session_vendor_codes(session_id)
        if not vendor_codes:
            return jsonify({
                'error': 'Session not found or expired. Please upload your vendor codes again.',
                'session_id': session_id
            }), 404

        # Filter data from cache
        try:
            filtered_data = filter_data_for_session(session_id)
        except Exception as e:
            logger.error(f"Failed to filter data for session {session_id}: {e}")
            logger.error(traceback.format_exc())
            return jsonify({
                'error': f'Failed to filter data: {str(e)}',
                'session_id': session_id
            }), 500

        # Prepare response
        response = {
            'success': True,
            'session_id': session_id,
            'vendor_count': len(vendor_codes),
            'data_counts': {
                'discount_stock': len(filtered_data['discount_stock']),
                'vendor_status': len(filtered_data['vendor_status']),
                'vendor_product_status': len(filtered_data['vendor_product_status'])
            },
            'cache_status': {
                'last_fetch_times': {
                    k: v.isoformat() if v else None
                    for k, v in state.last_fetch_times.items()
                },
                'fetch_errors': state.fetch_errors
            },
            # Include actual data in response
            'data': {
                'discount_stock': filtered_data['discount_stock'].to_dict(orient='records') if not filtered_data['discount_stock'].empty else [],
                'vendor_status': filtered_data['vendor_status'].to_dict(orient='records') if not filtered_data['vendor_status'].empty else [],
                'vendor_product_status': filtered_data['vendor_product_status'].to_dict(orient='records') if not filtered_data['vendor_product_status'].empty else []
            }
        }

        logger.info(f"✅ Session {session_id}: Retrieved data with {response['data_counts']} rows")

        return jsonify(response), 200

    except Exception as e:
        logger.error(f"❌ Unexpected error in get_session_data: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': 'An unexpected error occurred while retrieving session data',
            'details': str(e),
            'session_id': session_id
        }), 500

@app.route('/api/session-status/<session_id>', methods=['GET'])
def get_session_status(session_id):
    """
    Check the status of a specific user session.

    Returns:
        - Session status: active, inactive, disconnected, or expired
        - Session metadata: created_at, last_accessed, connected_at, disconnected_at
        - Time remaining until expiry

    Status values:
        - active: Connected and receiving updates
        - disconnected: User closed window/tab but data preserved for 5 minutes
        - expired: Session exceeded 5-minute inactivity timeout and has been removed
    """
    try:
        with TimedLock(state.lock, timeout=30, name="get_session_status"):
            if session_id not in state.user_sessions:
                return jsonify({
                    'error': 'Session not found',
                    'session_id': session_id,
                    'status': 'expired'
                }), 404

            session = state.user_sessions[session_id]
            now = get_tehran_time()
            last_accessed = session.get('last_accessed')
            inactivity_hours = (now - last_accessed).total_seconds() / 3600 if last_accessed else None

            # Check if session is expired (5 minutes = 5/60 hours)
            is_expired = inactivity_hours and inactivity_hours > 5/60

            response = {
                'success': True,
                'session_id': session_id,
                'status': session.get('status', 'active'),
                'vendor_count': len(session.get('vendor_codes', set())),
                'metadata': {
                    'created_at': session['created_at'].isoformat(),
                    'last_accessed': last_accessed.isoformat() if last_accessed else None,
                    'connected_at': session.get('connected_at', {}).isoformat() if session.get('connected_at') else None,
                    'disconnected_at': session.get('disconnected_at', {}).isoformat() if session.get('disconnected_at') else None,
                    'inactivity_hours': round(inactivity_hours, 2) if inactivity_hours else None
                },
                'expiry': {
                    'expires_in_hours': round(5/60 - (inactivity_hours or 0), 2),
                    'will_expire_at': (last_accessed + __import__('datetime').timedelta(hours=5/60)).isoformat() if last_accessed else None,
                    'is_expired': is_expired
                }
            }

            logger.info(f"ℹ️  Session status check: {session_id[:8]}... | Status: {response['status']} | Inactivity: {inactivity_hours:.1f}h")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"❌ Error checking session status: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': 'An error occurred while checking session status',
            'details': str(e),
            'session_id': session_id
        }), 500

@app.route('/api/refresh-data/<session_id>', methods=['GET'])
def refresh_data(session_id):
    """
    Refresh data for an active session.
    Called by frontend every 3 minutes to pull latest data.

    Returns:
        - Fresh filtered data
        - Updated alerts
        - Updated stats
    """
    try:
        # Validate session exists and is active
        if not is_session_valid(session_id):
            return jsonify({
                'error': 'Session not found or expired',
                'session_id': session_id
            }), 404

        # Get vendor codes for this session
        vendor_codes = get_session_vendor_codes(session_id)
        if not vendor_codes:
            return jsonify({
                'error': 'No vendor codes found for session',
                'session_id': session_id
            }), 404

        # Filter fresh data for this session
        filtered_data = filter_data_for_session(session_id)

        # Build response with fresh data
        with TimedLock(state.lock, timeout=30, name="refresh_data"):
            response = {
                'success': True,
                'session_id': session_id,
                'timestamp': get_tehran_time().isoformat(),
                'vendor_count': len(vendor_codes),
                'data': {
                    'discount_stock': filtered_data['discount_stock'].to_dict(orient='records') if not filtered_data['discount_stock'].empty else [],
                    'vendor_status': filtered_data['vendor_status'].to_dict(orient='records') if not filtered_data['vendor_status'].empty else [],
                    'vendor_product_status': filtered_data['vendor_product_status'].to_dict(orient='records') if not filtered_data['vendor_product_status'].empty else []
                },
                'alerts': {
                    'discount_stock': [
                        alert for alert in state.alerts['discount_stock'].values()
                        if alert.get('vendor_code') in vendor_codes
                    ],
                    'vendor_status': [
                        alert for alert in state.alerts['vendor_status'].values()
                        if alert.get('vendor_code') in vendor_codes
                    ],
                    'vendor_product_stock': [
                        alert for alert in state.alerts['vendor_product_stock'].values()
                        if alert.get('vendor_code') in vendor_codes
                    ],
                    'vendor_product_visibility': [
                        alert for alert in state.alerts['vendor_product_visibility'].values()
                        if alert.get('vendor_code') in vendor_codes
                    ]
                },
                'stats': {
                    'vendor_status': {
                        'total_vendors': len(vendor_codes),
                        'active_vendors': len(filtered_data['vendor_status'][filtered_data['vendor_status']['vendor_status'] == VENDOR_STATUS_ACTIVE]) if not filtered_data['vendor_status'].empty and 'vendor_status' in filtered_data['vendor_status'].columns else 0,
                        'inactive_vendors': len(filtered_data['vendor_status'][filtered_data['vendor_status']['vendor_status'] == VENDOR_STATUS_INACTIVE]) if not filtered_data['vendor_status'].empty and 'vendor_status' in filtered_data['vendor_status'].columns else 0,
                        'active_alerts': len([a for a in state.alerts['vendor_status'].values() if a.get('vendor_code') in vendor_codes]),
                        'cleared_alerts': len([a for a in state.cleared_alerts['vendor_status'].values() if a.get('vendor_code') in vendor_codes])
                    },
                    'vendor_product': {
                        'total_vendors': len(vendor_codes),
                        'business_lines': filtered_data['vendor_product_status'].get('business_line', pd.Series()).value_counts().to_dict() if not filtered_data['vendor_product_status'].empty else {},
                        'stock_alert_counts': {
                            'has_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                                              if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_NEW and a.get('vendor_code') in vendor_codes]),
                            'had_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                                              if a.get('alert_type') == ALERT_TYPE_STOCK_ISSUES_PERSISTENT and a.get('vendor_code') in vendor_codes])
                        },
                        'visibility_alert_counts': {
                            'has_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                                              if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_NEW and a.get('vendor_code') in vendor_codes]),
                            'had_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                                              if a.get('alert_type') == ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT and a.get('vendor_code') in vendor_codes])
                        },
                        'stock_cleared_count': len([a for a in state.cleared_alerts['vendor_product_stock'].values() if a.get('vendor_code') in vendor_codes]),
                        'visibility_cleared_count': len([a for a in state.cleared_alerts['vendor_product_visibility'].values() if a.get('vendor_code') in vendor_codes])
                    }
                },
                'data_counts': {
                    'discount_stock': len(filtered_data['discount_stock']),
                    'vendor_status': len(filtered_data['vendor_status']),
                    'vendor_product_status': len(filtered_data['vendor_product_status'])
                }
            }

        logger.info(f"🔄 Refreshed data for session {session_id[:8]}... ({len(vendor_codes)} vendors)")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"❌ Error refreshing data for session {session_id[:8]}...: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': 'Failed to refresh data',
            'details': str(e),
            'session_id': session_id
        }), 500

@app.route('/api/clear-vendors', methods=['POST'])
def clear_vendors():
    try:
        with TimedLock(state.lock, timeout=30, name="clear_vendors"):
            state.vendor_codes.clear()
            state.alerts = {
                'discount_stock': {},
                'vendor_status': {},
                'vendor_product_stock': {},
                'vendor_product_visibility': {}
            }
            state.cleared_alerts = {
                'discount_stock': {},
                'vendor_status': {},
                'vendor_product_stock': {},
                'vendor_product_visibility': {}
            }
            state.previous_vendor_status.clear()
            state.previous_product_status.clear()
        
        socketio.emit('clear_all_alerts')
        logger.info("Cleared all vendor codes and alerts")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error clearing vendors: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-cleared-discount-alerts')
def export_cleared_discount_alerts():
    try:
        with TimedLock(state.lock, timeout=30, name="export_cleared_discount_alerts"):
            alerts = list(state.cleared_alerts['discount_stock'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Cleared Discount Alerts')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'cleared_discount_alerts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting cleared discount alerts: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-cleared-vendor-status-alerts')
def export_cleared_vendor_status_alerts():
    try:
        with TimedLock(state.lock, timeout=30, name="export_cleared_vendor_status_alerts"):
            alerts = list(state.cleared_alerts['vendor_status'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Cleared Vendor Status')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'cleared_vendor_status_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting cleared vendor status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-cleared-stock-alerts')
def export_cleared_stock_alerts():
    try:
        with TimedLock(state.lock, timeout=30, name="export_cleared_stock_alerts"):
            alerts = list(state.cleared_alerts['vendor_product_stock'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Cleared Stock Alerts')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'cleared_stock_alerts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting cleared stock alerts: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-cleared-visibility-alerts')
def export_cleared_visibility_alerts():
    try:
        with TimedLock(state.lock, timeout=30, name="export_cleared_visibility_alerts"):
            alerts = list(state.cleared_alerts['vendor_product_visibility'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Cleared Visibility Alerts')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'cleared_visibility_alerts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting cleared visibility alerts: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-active-vendors')
def export_active_vendors():
    try:
        with TimedLock(state.lock, timeout=30, name="export_active_vendors"):
            df = state.vendor_status_data
            if not df.empty and 'vendor_status' in df.columns:
                active_df = df[df['vendor_status'] == VENDOR_STATUS_ACTIVE]
            else:
                active_df = pd.DataFrame()
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            active_df.to_excel(writer, index=False, sheet_name='Active Vendors')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'active_vendors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting active vendors: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-inactive-vendors')
def export_inactive_vendors():
    try:
        with TimedLock(state.lock, timeout=30, name="export_inactive_vendors"):
            df = state.vendor_status_data
            if not df.empty and 'vendor_status' in df.columns:
                inactive_df = df[df['vendor_status'] == VENDOR_STATUS_INACTIVE]
            else:
                inactive_df = pd.DataFrame()
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            inactive_df.to_excel(writer, index=False, sheet_name='Inactive Vendors')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'inactive_vendors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting inactive vendors: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-stock-issues')
def export_stock_issues():
    try:
        with TimedLock(state.lock, timeout=30, name="export_stock_issues"):
            alerts = list(state.alerts['vendor_product_stock'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Stock Issues')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'stock_issues_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting stock issues: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-visibility-issues')
def export_visibility_issues():
    try:
        with TimedLock(state.lock, timeout=30, name="export_visibility_issues"):
            alerts = list(state.alerts['vendor_product_visibility'].values())
        
        df = pd.DataFrame(alerts)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Visibility Issues')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'visibility_issues_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error exporting visibility issues: {e}")
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket client connection"""
    logger.info(f'🔌 WebSocket client connected: {request.sid[:8]}...')
    emit('connection_response', {'status': 'connected'})

@socketio.on('register_session')
def handle_register_session(data):
    """
    Register a session with the WebSocket connection.
    This allows us to track when a user closes their window.
    Expected data: {'session_id': '<uuid>'}

    CRITICAL: Joins user to a private room for their session to prevent data leaks.
    """
    try:
        from flask_socketio import join_room

        session_id = data.get('session_id')
        if not session_id:
            logger.warning("⚠️  register_session called without session_id")
            emit('session_response', {'status': 'error', 'message': 'session_id required'})
            return

        # Store mapping of WebSocket SID to session_id
        with TimedLock(state.lock, timeout=30, name="handle_register_session"):
            # Mark session as connected (active)
            if session_id in state.user_sessions:
                state.user_sessions[session_id]['status'] = 'active'
                state.user_sessions[session_id]['websocket_sid'] = request.sid
                state.user_sessions[session_id]['connected_at'] = get_tehran_time()

                # CRITICAL: Join user to their private room to prevent cross-user data leaks
                join_room(session_id)

                logger.info(f"📱 Session registered: {session_id[:8]}... | WebSocket: {request.sid[:8]}... | Joined room: {session_id[:8]}...")
                emit('session_response', {'status': 'registered', 'session_id': session_id})
            else:
                logger.warning(f"⚠️  register_session: Unknown session {session_id[:8]}...")
                emit('session_response', {'status': 'error', 'message': 'session not found'})
    except Exception as e:
        logger.error(f"❌ Error registering session: {e}")
        emit('session_response', {'status': 'error', 'message': str(e)})

@socketio.on('disconnect')
def handle_disconnect():
    """
    Handle WebSocket client disconnection.
    This is triggered when a user closes their browser window/tab.
    Marks the associated session as 'disconnected' for graceful handling.
    """
    logger.info(f"🔌 WebSocket client disconnected: {request.sid[:8]}...")

    # Find and mark associated session as disconnected
    with TimedLock(state.lock, timeout=30, name="handle_disconnect"):
        for session_id, session_data in list(state.user_sessions.items()):
            if session_data.get('websocket_sid') == request.sid:
                old_status = session_data.get('status')
                session_data['status'] = 'disconnected'
                session_data['disconnected_at'] = get_tehran_time()
                vendor_count = len(session_data.get('vendor_codes', set()))

                logger.info(
                    f"📴 Session disconnected: {session_id[:8]}... | "
                    f"Previous status: {old_status} | "
                    f"Vendors: {vendor_count} | "
                    f"New status: disconnected | "
                    f"Will expire in 5 minutes"
                )
                break

def start_background_jobs():
    """
    Start the SINGLE centralized background job using eventlet green threads.
    This replaces the old 3 separate jobs to avoid redundant fetches.

    CRITICAL: Uses eventlet.spawn instead of threading.Thread to avoid
    incompatibility between eventlet's monkey-patched cooperative threads
    and OS preemptive threads, which causes the background job to freeze.
    """
    # Use eventlet.spawn to create a green thread compatible with eventlet
    eventlet.spawn(centralized_fetch_job)

    logger.info("✅ Centralized background job started (eventlet green thread - fetches all data + updates sessions)")

if __name__ == '__main__':
    # Perform initial data fetch (BLOCKING - must complete before serving)
    logger.info("=" * 70)
    logger.info("🚀 DEVELOPMENT MODE - Starting initial data fetch...")
    logger.info("=" * 70)
    perform_initial_fetch()

    # Start background jobs for periodic refreshes
    start_background_jobs()

    # Start Flask development server
    socketio.run(app, host=APP_HOST, port=APP_PORT, debug=APP_DEBUG)
