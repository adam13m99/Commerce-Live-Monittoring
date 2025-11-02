import os

METABASE_URL = "https://metabase.tapsifood.cloud"
METABASE_USERNAME = "a.mehmandoost@OFOOD.CLOUD"
METABASE_PASSWORD = "Fff322666@"

METABASE_DATABASE = os.getenv('METABASE_DATABASE', 'data')
METABASE_PAGE_SIZE = int(os.getenv('METABASE_PAGE_SIZE', '5000000'))

QUESTION_ID_DISCOUNT_STOCK = int(os.getenv('QUESTION_ID_DISCOUNT_STOCK', '7179'))
QUESTION_ID_VENDOR_STATUS = int(os.getenv('QUESTION_ID_VENDOR_STATUS', '7163'))
QUESTION_ID_VENDOR_PRODUCT_STATUS = int(os.getenv('QUESTION_ID_VENDOR_PRODUCT_STATUS', '7196'))

APP_SECRET_KEY = os.getenv('APP_SECRET_KEY', 'dc3e0cfbda69b2aa6eb306881e813e443c627c33c3f9e6f0d083f6dae429f722')
APP_HOST = os.getenv('APP_HOST', '0.0.0.0')
APP_PORT = int(os.getenv('APP_PORT', '5000'))
APP_DEBUG = os.getenv('APP_DEBUG', 'False').lower() in ('true', '1', 't')

CORS_ALLOWED_ORIGINS = os.getenv('CORS_ALLOWED_ORIGINS', '*')
ASYNC_MODE = os.getenv('ASYNC_MODE', 'threading')

DISCOUNT_STOCK_JOB_INTERVAL = int(os.getenv('DISCOUNT_STOCK_JOB_INTERVAL', '180'))  # Check every 5 minutes
VENDOR_STATUS_JOB_INTERVAL = int(os.getenv('VENDOR_STATUS_JOB_INTERVAL', '185'))  # Check every 5 minutes
VENDOR_PRODUCT_STATUS_JOB_INTERVAL = int(os.getenv('VENDOR_PRODUCT_STATUS_JOB_INTERVAL', '190'))  # Check every 6 minutes

SEVERITY_CHERRY = 'cherry'
SEVERITY_RED = 'red'
SEVERITY_YELLOW = 'yellow'
SEVERITY_GREEN = 'green'
SEVERITY_LIGHT_GREEN = 'light-green'
SEVERITY_DARK_GREEN = 'dark-green'
SEVERITY_RED_HIGH = 'red-high'
SEVERITY_RED_MEDIUM = 'red-medium'
SEVERITY_RED_LIGHT = 'red-light'

PRIORITY_HIGH = 1  # Cherry alerts (transitions)
PRIORITY_LOW = 2   # Yellow alerts (persistent states)

VENDOR_STATUS_ACTIVE = 'vendor_active_in_shift'
VENDOR_STATUS_INACTIVE = 'vendor_not_active_in_shift'

PRODUCT_STATUS_STOCK_GOOD = 'stock_good'
PRODUCT_STATUS_STOCK_ISSUE = 'vendor_stock_issue'
PRODUCT_STATUS_VISIBILITY_GOOD = 'visibility_good'
PRODUCT_STATUS_VISIBILITY_ISSUE = 'vendor_product_visibility_issue'

ALERT_TYPE_PRODUCT_STOCK_FINISHED = 'Product Stock Finished'
ALERT_TYPE_DISCOUNT_STOCK_NEAR_END = 'Discount Stock Near End'
ALERT_TYPE_VENDOR_DEACTIVATED = 'Vendor_Got_Deactivated'
ALERT_TYPE_VENDOR_NOT_ACTIVE = 'vendor_Started_With_Not_Active_In_Shift'
ALERT_TYPE_VENDOR_ACTIVATED = 'Vendor Got Activated'
ALERT_TYPE_DISCOUNTED_ITEM_FIXED = 'Discounted Item Fixed'
ALERT_TYPE_STOCK_ISSUES_NEW = 'Vendor Has Stock Issues'
ALERT_TYPE_STOCK_ISSUES_PERSISTENT = 'Vendor Had Stock Issues'
ALERT_TYPE_STOCK_ISSUES_FIXED = 'Fixed Vendor Stock Issue'
ALERT_TYPE_VISIBILITY_ISSUES_NEW = 'Vendor Has Visibility Issues'
ALERT_TYPE_VISIBILITY_ISSUES_PERSISTENT = 'Vendor Had Visibility Issues'
ALERT_TYPE_VISIBILITY_ISSUES_FIXED = 'Fixed Vendor Visibility Issue'

DISCOUNT_STOCK_NEAR_END_THRESHOLD = 3  
