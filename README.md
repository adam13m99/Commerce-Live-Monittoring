# X-Commerce Monitoring

Real-time vendor monitoring dashboard for commerce operations. Tracks vendor status, product availability, and discount stock levels with live WebSocket alerts.

---

## Overview

X-Commerce Monitoring is a Flask-based dashboard that provides real-time monitoring of:
- **Discount Stock Levels** - Track available discount inventory
- **Vendor Status** - Monitor vendor online/offline status
- **Vendor Product Status** - Track product availability across vendors

The dashboard fetches data from Metabase, processes alerts based on configurable thresholds, and broadcasts real-time updates to all connected users via WebSockets.

---

## Features

- **Real-time Alerts** - WebSocket-based live notifications
- **Metabase Integration** - Automated data fetching from Metabase queries
- **Severity-based Color Coding** - Visual alerts (cherry, red, yellow, green)
- **Background Jobs** - Automated data refresh at configurable intervals
- **Multi-user Support** - Shared state across all connected users
- **Vault Integration** - Secure credential management
- **Docker Ready** - Containerized deployment
- **CI/CD Pipeline** - GitLab + Kubernetes automation

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    USER BROWSERS                            │
│  (Multiple users connected via WebSocket)                   │
└────────────────┬────────────────────────────────────────────┘
                 │ WebSocket Connection
                 ▼
┌─────────────────────────────────────────────────────────────┐
│            FLASK + SOCKETIO APPLICATION                      │
│  - Serves web interface                                     │
│  - Broadcasts alerts to all connected clients               │
│  - Single worker (shared state)                             │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│            BACKGROUND JOBS (3 threads)                       │
│                                                              │
│  Job 1: Discount Stock      (every 180s)                    │
│  Job 2: Vendor Status        (every 185s)                    │
│  Job 3: Vendor Product Status (every 190s)                   │
│                                                              │
│  Each job:                                                   │
│  1. Fetches data from Metabase                              │
│  2. Processes and stores in global state                    │
│  3. Calculates alerts based on thresholds                   │
│  4. Broadcasts alerts via WebSocket                         │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                    METABASE API                              │
│  Question 7179: Discount Stock Data                         │
│  Question 7163: Vendor Status Data                          │
│  Question 7196: Vendor Product Status Data                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Local Development (Docker)

```bash
# 1. Clone repository
git clone <your-repo-url>
cd x-commerce-monitoring

# 2. Start with Docker Compose
docker-compose up -d

# 3. Open browser
http://localhost:5000

# 4. Check health
curl http://localhost:5000/health
```

### Local Development (Python - Windows)

```bash
# Note: Windows cannot run Gunicorn (Unix-only)
# For quick testing, create a test file:

# test_windows.py
from app import app, socketio, start_background_jobs

if __name__ == '__main__':
    start_background_jobs()
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# Run it
python test_windows.py
```

**Important:** For production-like testing on Windows, use `docker-compose up -d`

---

## Prerequisites

### Development
- Docker & Docker Compose
- Python 3.11+ (for local development)
- Access to Metabase instance

### Production
- GitLab account with CI/CD enabled
- Kubernetes cluster
- HashiCorp Vault (for secrets)
- GitLab Runner with `prd` tag

---

## Configuration

All configuration is managed via environment variables (injected from Vault in production).

### Required Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `METABASE_URL` | Metabase instance URL | `https://metabase.ofood.cloud` |
| `METABASE_USERNAME` | Metabase username | Required |
| `METABASE_PASSWORD` | Metabase password | Required |
| `APP_SECRET_KEY` | Flask secret key | Required |

### Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_PORT` | Application port | `5000` |
| `APP_DEBUG` | Debug mode | `False` |
| `QUESTION_ID_DISCOUNT_STOCK` | Metabase question ID | `7179` |
| `QUESTION_ID_VENDOR_STATUS` | Metabase question ID | `7163` |
| `QUESTION_ID_VENDOR_PRODUCT_STATUS` | Metabase question ID | `7196` |
| `DISCOUNT_STOCK_JOB_INTERVAL` | Job interval (seconds) | `180` |
| `VENDOR_STATUS_JOB_INTERVAL` | Job interval (seconds) | `185` |
| `VENDOR_PRODUCT_STATUS_JOB_INTERVAL` | Job interval (seconds) | `190` |

See `config.py` for complete list of configuration options.

---

## Project Structure

```
x-commerce-monitoring/
├── app.py                      # Main Flask application
├── mini.py                     # Metabase integration
├── config.py                   # Configuration (Vault pattern)
├── run_production.py           # Production entry point
├── Dockerfile                  # Container build instructions
├── docker-compose.yml          # Local development orchestration
├── .gitlab-ci.yml              # CI/CD pipeline
├── requirements.txt            # Python dependencies
├── templates/
│   └── index.html              # Dashboard UI
├── static/
│   ├── styles.css              # Dashboard styles
│   └── app.js                  # WebSocket client
└── docs/
    ├── README.md               # This file
    ├── DEPLOYMENT-WORKFLOW.md  # Deployment guide
    ├── DEPLOYMENT-FILES-EXPLAINED.md
    └── HOW-TO-RUN.md           # Quick run guide
```

---

## Alert Severity Levels

The dashboard uses color-coded severity levels:

| Severity | Color | Description |
|----------|-------|-------------|
| `cherry` | Dark Red | Critical - Immediate action required |
| `red` | Red | High priority |
| `red-high` | Bright Red | Very high priority |
| `yellow` | Yellow | Warning |
| `green` | Green | Normal |
| `light-green` | Light Green | Good |
| `dark-green` | Dark Green | Excellent |

Severity thresholds are configurable in `config.py`.

---

## Deployment

### Tag-Based Deployment Workflow

This project uses **tag-based deployment**. Deployments only happen when you create production tags.

#### Step 1: Develop and Test

```bash
# Make changes
vim app.py

# Test locally
docker-compose up -d
curl http://localhost:5000/health

# Commit changes
git add .
git commit -m "Add new feature"
git push origin main
```

**Note:** Pushing to `main` does NOT trigger deployment.

#### Step 2: Create Production Tag

```bash
# Create tag (format: v{major}.{minor}.{patch}-prd)
git tag v1.0.0-prd

# Push tag (this triggers CI/CD pipeline)
git push origin v1.0.0-prd
```

#### Step 3: Monitor Deployment

```
GitLab Pipeline:
  Stage 1: docker_build
    - Builds Docker image
    - Pushes to GitLab registry

  Stage 2: init_ci
    - Triggers external Kubernetes deployment
    - DevOps system handles deployment

Production:
  - Pulls image from registry
  - Injects Vault secrets
  - Deploys to namespace: x-commerce-monitoring
  - Runs health checks
```

### Tag Naming Convention

**Pattern:** `/^v\d{1,9}\.\d{1,9}\.\d{1,9}-[p][r][d]$/`

**Valid Examples:**
- `v1.0.0-prd` - Initial release
- `v1.0.1-prd` - Bug fix
- `v1.1.0-prd` - New feature
- `v2.0.0-prd` - Breaking change

**Invalid Examples:**
- `v1.0.0-prod` - Must be `-prd` not `-prod`
- `v1.0.0` - Missing `-prd` suffix
- `1.0.0-prd` - Missing `v` prefix

See [DEPLOYMENT-WORKFLOW.md](DEPLOYMENT-WORKFLOW.md) for detailed deployment guide.

---

## Security

### Secrets Management

All secrets are managed via **HashiCorp Vault**:

```bash
# DevOps team stores secrets in Vault:
vault kv put secret/x-commerce-monitoring/app \
  METABASE_USERNAME="your-username" \
  METABASE_PASSWORD="your-password" \
  APP_SECRET_KEY="your-secret-key"

# Secrets are injected at runtime in Kubernetes
# Application receives them via os.getenv()
```

### Important Security Notes

- **Never commit secrets** to version control
- All sensitive values use placeholders in `config.py`
- Real credentials only in Vault
- Container runs as non-root user (UID 1000)
- Health checks don't expose sensitive data

---

## Health Checks

### Liveness Probe
```bash
curl http://localhost:5000/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-27T10:30:00"
}
```

### Readiness Probe
```bash
curl http://localhost:5000/ready
```

**Response:**
```json
{
  "status": "ready",
  "checks": {
    "vendor_codes_initialized": true,
    "alerts_initialized": true,
    "locks_initialized": true
  },
  "timestamp": "2025-01-27T10:30:00"
}
```

---

## Testing

### Local Testing with Docker

```bash
# Build and run
docker-compose up -d

# Check logs
docker-compose logs -f

# Test endpoints
curl http://localhost:5000/health
curl http://localhost:5000/ready

# Test WebSocket (open in browser)
http://localhost:5000

# Stop
docker-compose down
```

### Testing Background Jobs

Background jobs run automatically and log their execution:

```bash
# Watch logs for job execution
docker-compose logs -f vendor-monitoring

# You should see:
# INFO - Fetching discount stock data...
# INFO - Discount stock data updated: X vendors
# INFO - Fetching vendor status data...
# INFO - Vendor status updated: Y vendors
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard UI |
| `/health` | GET | Liveness probe |
| `/ready` | GET | Readiness probe |
| `/api/discount-stock` | GET | Get discount stock data |
| `/api/vendor-status` | GET | Get vendor status data |
| `/api/vendor-product-status` | GET | Get vendor product status |
| `/api/vendor-codes` | GET | Get list of vendor codes |

### WebSocket Events

**Client → Server:**
- `connect` - Client connects to dashboard

**Server → Client:**
- `vendor_alert` - Broadcast vendor alert to all clients
- `vendor_product_alert` - Broadcast vendor product alert
- `discount_stock_update` - Broadcast discount stock update

---

## Troubleshooting

### Issue: WebSocket not connecting

**Solution:**
```bash
# Check if application is running
curl http://localhost:5000/health

# Check browser console for errors
# Open browser DevTools → Console

# Verify WebSocket URL in app.js
# Should match your deployment URL
```

### Issue: No data showing in dashboard

**Solution:**
```bash
# Check if background jobs are running
docker-compose logs -f | grep "Fetching"

# Verify Metabase credentials
# Check environment variables are set correctly

# Test Metabase connection manually
curl -u "username:password" https://metabase.ofood.cloud/api/session
```

### Issue: Pipeline not triggering

**Solution:**
```bash
# Verify tag format
git tag -l "*-prd"

# Should show: v1.0.0-prd (not v1.0.0-prod or v1.0.0)

# Delete wrong tag
git tag -d v1.0.0-prod
git push origin :refs/tags/v1.0.0-prod

# Create correct tag
git tag v1.0.0-prd
git push origin v1.0.0-prd
```

### Issue: Container fails health checks

**Solution:**
```bash
# Check container logs
docker-compose logs vendor-monitoring

# Common issues:
# 1. Metabase credentials incorrect
# 2. Metabase URL unreachable
# 3. Port 5000 already in use

# Test health endpoint manually
docker exec vendor-monitoring curl http://localhost:5000/health
```

---

## Documentation

- [DEPLOYMENT-WORKFLOW.md](DEPLOYMENT-WORKFLOW.md) - Complete deployment guide
- [DEPLOYMENT-FILES-EXPLAINED.md](DEPLOYMENT-FILES-EXPLAINED.md) - Understanding deployment files
- [HOW-TO-RUN.md](HOW-TO-RUN.md) - Quick start guide
- [DEPLOYMENT-READINESS.md](DEPLOYMENT-READINESS.md) - Production readiness checklist

---

## Contributing

### Development Workflow

```bash
# 1. Create feature branch
git checkout -b feature/new-alert-type

# 2. Make changes
vim app.py

# 3. Test locally
docker-compose up -d

# 4. Commit changes
git add .
git commit -m "feat: Add new alert type for XYZ"

# 5. Push to GitLab
git push origin feature/new-alert-type

# 6. Create Merge Request
# (via GitLab UI)

# 7. After merge to main, deploy to production
git checkout main
git pull origin main
git tag v1.1.0-prd
git push origin v1.1.0-prd
```

### Coding Standards

- Python code follows PEP 8
- Use type hints where applicable
- Add docstrings to all functions
- Update documentation when adding features
- Test changes locally before pushing

---

## Version History

### v1.0.0-prd (Initial Release)
- Real-time vendor monitoring dashboard
- Metabase integration for data fetching
- WebSocket-based live alerts
- Three background jobs (discount stock, vendor status, vendor product status)
- Docker containerization
- GitLab CI/CD pipeline
- Vault integration for secrets

---

## License

Internal project - oFood Commerce Team

---

## Team

**Development Team:** Commerce Monitoring Team
**DevOps Team:** Infrastructure Team
**Maintained By:** Commerce Operations

---

## Support

### Internal Support

- **GitLab Issues:** Create issue in project repository
- **DevOps Support:** Contact Infrastructure Team
- **Metabase Issues:** Contact Data Team

### Quick Links

- GitLab Repository: `<your-gitlab-url>`
- Metabase Instance: `https://metabase.ofood.cloud`
- Production Dashboard: `<your-production-url>`
- Kubernetes Namespace: `x-commerce-monitoring`

---

## Performance

### Optimizations

- **Single Worker:** Uses 1 Gunicorn worker for shared in-memory state
- **Eventlet Worker:** Async worker class for WebSocket support
- **Efficient Polling:** Staggered background job intervals (180s, 185s, 190s)
- **Metabase Pagination:** Large page size (5M) to minimize API calls
- **WebSocket Broadcasting:** Single broadcast reaches all connected clients

### Resource Requirements

**Development:**
- CPU: 1 core
- Memory: 512 MB
- Storage: 100 MB

**Production:**
- CPU: 2 cores (recommended)
- Memory: 2 GB (recommended)
- Storage: 500 MB

---

## Future Enhancements

- [ ] Historical data tracking and charts
- [ ] Email/Slack notifications
- [ ] User authentication and role-based access
- [ ] Configurable alert thresholds via UI
- [ ] Export data to CSV/Excel
- [ ] Mobile-responsive design improvements
- [ ] Alert acknowledgment system

---

**Last Updated:** 2025-10-27
**Version:** 1.0.0
**Status:** Production Ready 
