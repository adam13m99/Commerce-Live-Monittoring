"""
Real-Time Vendor Monitoring & Alerting Dashboard
Flask application with WebSocket support for live updates
"""

from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import pandas as pd
from datetime import datetime
import threading
import time
import io
from typing import Dict, List, Set, Optional
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

app = Flask(__name__)
app.config['SECRET_KEY'] = APP_SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins=CORS_ALLOWED_ORIGINS, async_mode=ASYNC_MODE)

# Global state management
class AppState:
    def __init__(self):
        self.vendor_codes: Set[str] = set()
        # Store alerts as dictionaries keyed by unique identifiers
        self.alerts: Dict[str, Dict[str, Dict]] = {
            'discount_stock': {},      # Key: product_id
            'vendor_status': {},       # Key: vendor_code
            'vendor_product_stock': {},     # Key: vendor_code
            'vendor_product_visibility': {} # Key: vendor_code
        }
        # Track cleared alerts
        self.cleared_alerts: Dict[str, Dict[str, Dict]] = {
            'discount_stock': {},
            'vendor_status': {},
            'vendor_product_stock': {},
            'vendor_product_visibility': {}
        }
        self.previous_vendor_status: Dict[str, str] = {}
        self.previous_product_status: Dict[str, Dict] = {}
        self.vendor_status_data: pd.DataFrame = pd.DataFrame()
        self.discount_stock_data: pd.DataFrame = pd.DataFrame()
        self.vendor_product_status_data: pd.DataFrame = pd.DataFrame()
        self.lock = threading.Lock()

state = AppState()

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

# Mock data fetching function (replace with actual mini.py integration)
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

def process_discount_stock_alerts(df: pd.DataFrame):
    """Process discount stock data and generate alerts with key-based updates"""
    if df.empty:
        return
    
    with state.lock:
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
                    cleared_alert['cleared_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cleared_alert['alert_type'] = ALERT_TYPE_DISCOUNTED_ITEM_FIXED
                    cleared_alert['product_stock'] = int(product_stock)
                    cleared_alert['discount_stock'] = discount_stock
                    state.cleared_alerts['discount_stock'][product_id] = cleared_alert

                    # Remove from active alerts
                    del state.alerts['discount_stock'][product_id]

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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                
                socketio.emit('new_alert' if is_new else 'update_alert', {
                    'tab': 'discount_stock',
                    'alert': alert,
                    'is_new': is_new
                })
        
        # Check for alerts that are no longer in the data (removed products)
        for product_id in list(state.alerts['discount_stock'].keys()):
            if product_id not in current_keys:
                cleared_alert = state.alerts['discount_stock'][product_id].copy()
                cleared_alert['status'] = 'cleared'
                cleared_alert['severity'] = SEVERITY_GREEN
                cleared_alert['cleared_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cleared_alert['alert_type'] = ALERT_TYPE_DISCOUNTED_ITEM_FIXED
                state.cleared_alerts['discount_stock'][product_id] = cleared_alert
                del state.alerts['discount_stock'][product_id]
                
                socketio.emit('alert_cleared', {
                    'tab': 'discount_stock',
                    'product_id': product_id,
                    'alert': cleared_alert
                })

def process_vendor_status_alerts(df: pd.DataFrame):
    """Process vendor status data and generate alerts with key-based updates"""
    if df.empty:
        return

    with state.lock:
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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    cleared_alert['cleared_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cleared_alert['alert_type'] = ALERT_TYPE_VENDOR_ACTIVATED
                    cleared_alert['vendor_status'] = current_status
                    state.cleared_alerts['vendor_status'][vendor_code] = cleared_alert
                    del state.alerts['vendor_status'][vendor_code]

                    socketio.emit('alert_cleared', {
                        'tab': 'vendor_status',
                        'vendor_code': vendor_code,
                        'alert': cleared_alert
                    })

            if alert:
                is_new = vendor_code not in state.alerts['vendor_status']
                state.alerts['vendor_status'][vendor_code] = alert

                socketio.emit('new_alert' if is_new else 'update_alert', {
                    'tab': 'vendor_status',
                    'alert': alert,
                    'is_new': is_new
                })

            # Update state
            state.previous_vendor_status[vendor_code] = current_status

def process_vendor_product_status_alerts(df: pd.DataFrame):
    """Process vendor product status data and generate alerts with key-based updates"""
    if df.empty:
        return
    
    with state.lock:
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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    cleared_alert['cleared_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cleared_alert['alert_type'] = ALERT_TYPE_STOCK_ISSUES_FIXED
                    state.cleared_alerts['vendor_product_stock'][key] = cleared_alert
                    del state.alerts['vendor_product_stock'][key]
                    
                    socketio.emit('alert_cleared', {
                        'tab': 'vendor_product_stock',
                        'vendor_code': vendor_code,
                        'alert': cleared_alert
                    })
            
            if stock_alert:
                is_new = key not in state.alerts['vendor_product_stock']
                state.alerts['vendor_product_stock'][key] = stock_alert
                
                socketio.emit('new_alert' if is_new else 'update_alert', {
                    'tab': 'vendor_product_stock',
                    'alert': stock_alert,
                    'is_new': is_new
                })
            
            # Visibility alerts
            visibility_alert = None
            visibility_status = 'cleared'
            
            if (prev_visibility_status == PRODUCT_STATUS_VISIBILITY_GOOD and
                current_visibility_status == PRODUCT_STATUS_VISIBILITY_ISSUE):
                visibility_status = 'active'
                visibility_alert = {
                    'vendor_code': vendor_code,
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
                    cleared_alert['cleared_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cleared_alert['alert_type'] = ALERT_TYPE_VISIBILITY_ISSUES_FIXED
                    state.cleared_alerts['vendor_product_visibility'][key] = cleared_alert
                    del state.alerts['vendor_product_visibility'][key]
                    
                    socketio.emit('alert_cleared', {
                        'tab': 'vendor_product_visibility',
                        'vendor_code': vendor_code,
                        'alert': cleared_alert
                    })
            
            if visibility_alert:
                is_new = key not in state.alerts['vendor_product_visibility']
                state.alerts['vendor_product_visibility'][key] = visibility_alert
                
                socketio.emit('new_alert' if is_new else 'update_alert', {
                    'tab': 'vendor_product_visibility',
                    'alert': visibility_alert,
                    'is_new': is_new
                })
            
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
        with state.lock:
            state.discount_stock_data = df
        process_discount_stock_alerts(df)
        logger.info("✅ Discount stock fetch completed")

        # 2. Fetch vendor status
        logger.info("👥 Fetching vendor status data...")
        df = fetch_vendor_status()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with state.lock:
            state.vendor_status_data = df
        process_vendor_status_alerts(df)
        socketio.emit('stats_update', get_vendor_status_stats())
        logger.info("✅ Vendor status fetch completed")

        # 3. Fetch vendor product status (first time)
        logger.info("📊 Fetching vendor product status data (1/2)...")
        df = fetch_vendor_product_status()
        df = filter_by_vendor_codes(df, 'vendor_code')
        with state.lock:
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
        with state.lock:
            state.vendor_product_status_data = df
        process_vendor_product_status_alerts(df)
        socketio.emit('stats_update', get_vendor_product_stats())
        logger.info("✅ Vendor product status fetch 2/2 completed")

        logger.info("🎉 All immediate fetches completed successfully!")

    except Exception as e:
        logger.error(f"❌ Error in immediate fetch: {e}")

def discount_stock_job():
    """Background job for discount stock monitoring"""
    while True:
        try:
            if state.vendor_codes:
                df = fetch_discount_stock()
                df = filter_by_vendor_codes(df, 'vendor_code')
                with state.lock:
                    state.discount_stock_data = df
                process_discount_stock_alerts(df)
            time.sleep(DISCOUNT_STOCK_JOB_INTERVAL)
        except Exception as e:
            logger.error(f"Error in discount stock job: {e}")
            time.sleep(DISCOUNT_STOCK_JOB_INTERVAL)

def vendor_status_job():
    """Background job for vendor status monitoring"""
    while True:
        try:
            if state.vendor_codes:
                df = fetch_vendor_status()
                df = filter_by_vendor_codes(df, 'vendor_code')
                with state.lock:
                    state.vendor_status_data = df
                process_vendor_status_alerts(df)
                socketio.emit('stats_update', get_vendor_status_stats())
            time.sleep(VENDOR_STATUS_JOB_INTERVAL)
        except Exception as e:
            logger.error(f"Error in vendor status job: {e}")
            time.sleep(VENDOR_STATUS_JOB_INTERVAL)

def vendor_product_status_job():
    """Background job for vendor product status monitoring"""
    while True:
        try:
            if state.vendor_codes:
                df = fetch_vendor_product_status()
                df = filter_by_vendor_codes(df, 'vendor_code')
                with state.lock:
                    state.vendor_product_status_data = df
                process_vendor_product_status_alerts(df)
                socketio.emit('stats_update', get_vendor_product_stats())
            time.sleep(VENDOR_PRODUCT_STATUS_JOB_INTERVAL)
        except Exception as e:
            logger.error(f"Error in vendor product status job: {e}")
            time.sleep(VENDOR_PRODUCT_STATUS_JOB_INTERVAL)

def get_vendor_status_stats() -> Dict:
    """Calculate vendor status statistics"""
    with state.lock:
        df = state.vendor_status_data
        if df.empty:
            return {
                'type': 'vendor_status',
                'total_vendors': len(state.vendor_codes),
                'active_vendors': 0,
                'inactive_vendors': 0,
                'active_alerts': len(state.alerts['vendor_status']),
                'cleared_alerts': len(state.cleared_alerts['vendor_status'])
            }
        
        active_count = len(df[df['vendor_status'] == VENDOR_STATUS_ACTIVE]) if 'vendor_status' in df.columns else 0
        inactive_count = len(df[df['vendor_status'] == VENDOR_STATUS_INACTIVE]) if 'vendor_status' in df.columns else 0
        
        return {
            'type': 'vendor_status',
            'total_vendors': len(state.vendor_codes),
            'active_vendors': active_count,
            'inactive_vendors': inactive_count,
            'active_alerts': len(state.alerts['vendor_status']),
            'cleared_alerts': len(state.cleared_alerts['vendor_status'])
        }

def get_vendor_product_stats() -> Dict:
    """Calculate vendor product status statistics"""
    with state.lock:
        df = state.vendor_product_status_data
        if df.empty:
            return {
                'type': 'vendor_product',
                'total_vendors': len(state.vendor_codes),
                'business_lines': {},
                'stock_alert_counts': {'has_issues': 0, 'had_issues': 0},
                'visibility_alert_counts': {'has_issues': 0, 'had_issues': 0},
                'stock_cleared_count': len(state.cleared_alerts['vendor_product_stock']),
                'visibility_cleared_count': len(state.cleared_alerts['vendor_product_visibility'])
            }
        
        business_lines = df.get('business_line', pd.Series()).value_counts().to_dict()
        
        stock_alerts = {
            'has_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                             if a['alert_type'] == ALERT_TYPE_STOCK_ISSUES_NEW]),
            'had_issues': len([a for a in state.alerts['vendor_product_stock'].values()
                             if a['alert_type'] == ALERT_TYPE_STOCK_ISSUES_PERSISTENT])
        }

        visibility_alerts = {
            'has_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                             if a['alert_type'] == ALERT_TYPE_VISIBILITY_ISSUES_NEW]),
            'had_issues': len([a for a in state.alerts['vendor_product_visibility'].values()
                             if a['alert_type'] == ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT])
        }
        
        return {
            'type': 'vendor_product',
            'total_vendors': len(state.vendor_codes),
            'business_lines': business_lines,
            'stock_alert_counts': stock_alerts,
            'visibility_alert_counts': visibility_alerts,
            'stock_cleared_count': len(state.cleared_alerts['vendor_product_stock']),
            'visibility_cleared_count': len(state.cleared_alerts['vendor_product_visibility'])
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint for Kubernetes liveness probe"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()}), 200

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
            'timestamp': datetime.now().isoformat()
        }), 200 if is_ready else 503
    except Exception as e:
        return jsonify({
            'status': 'not_ready',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 503

@app.route('/api/upload-vendors', methods=['POST'])
def upload_vendors():
    try:
        if 'file' in request.files:
            file = request.files['file']
            content = file.read().decode('utf-8')
            vendors = [line.strip() for line in content.split('\n') if line.strip()]
        elif 'vendors' in request.json:
            vendors = request.json['vendors']
        else:
            return jsonify({'error': 'No vendor data provided'}), 400

        with state.lock:
            state.vendor_codes = set(vendors)

        logger.info(f"Uploaded {len(vendors)} vendor codes")

        # Trigger immediate fetch in background thread
        fetch_thread = threading.Thread(target=run_immediate_fetch, daemon=True)
        fetch_thread.start()
        logger.info("Started immediate fetch thread")

        return jsonify({'success': True, 'count': len(vendors)})
    except Exception as e:
        logger.error(f"Error uploading vendors: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clear-vendors', methods=['POST'])
def clear_vendors():
    try:
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
        with state.lock:
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
    logger.info('Client connected')
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

def start_background_jobs():
    jobs = [
        threading.Thread(target=discount_stock_job, daemon=True),
        threading.Thread(target=vendor_status_job, daemon=True),
        threading.Thread(target=vendor_product_status_job, daemon=True)
    ]
    
    for job in jobs:
        job.start()
    
    logger.info("All background jobs started")

if __name__ == '__main__':
    start_background_jobs()
    socketio.run(app, host=APP_HOST, port=APP_PORT, debug=APP_DEBUG)
