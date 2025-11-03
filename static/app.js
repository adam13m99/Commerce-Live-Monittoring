// WebSocket Connection
const socket = io();

// State Management
const state = {
    currentTab: 'discount-stock',
    currentSubTab: 'stock',
    vendorCodes: [],
    sessionId: null,  // Session ID for WebSocket room isolation
    refreshTimer: null,  // Timer for periodic data refresh
    alerts: {
        discount_stock: {},
        vendor_status: {},
        vendor_product_stock: {},
        vendor_product_visibility: {}
    },
    clearedAlerts: {
        discount_stock: {},
        vendor_status: {},
        vendor_product_stock: {},
        vendor_product_visibility: {}
    },
    filters: {},
    sortConfig: {},
    totalAlerts: {
        discount_stock: 0,
        vendor_status: 0,
        product_status: 0
    }
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeTabs();
    initializeSubTabs();
    initializeVendorUpload();
    initializeWebSocket();
    initializeThemeToggle();
    initializeViewToggles();
    showConnectionStatus('connected');
    setTimeout(addFilteringHeaders, 100);
});

// Stop auto-refresh when user closes/leaves page
window.addEventListener('beforeunload', () => {
    stopAutoRefresh();
});

// Theme Toggle
function initializeThemeToggle() {
    const themeToggle = document.getElementById('theme-toggle');
    const themeIcon = document.querySelector('.theme-icon');

    // Load saved theme from localStorage or default to light
    const savedTheme = localStorage.getItem('theme') || 'light';
    applyTheme(savedTheme);

    themeToggle.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
        const newTheme = currentTheme === 'light' ? 'dark' : 'light';
        applyTheme(newTheme);
        localStorage.setItem('theme', newTheme);
    });
}

function applyTheme(theme) {
    const themeIcon = document.querySelector('.theme-icon');

    if (theme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
        themeIcon.textContent = '☀️';
    } else {
        document.documentElement.setAttribute('data-theme', 'light');
        themeIcon.textContent = '🌙';
    }
}

// View Toggle (Alerts / Fixed)
function initializeViewToggles() {
    const toggleButtons = document.querySelectorAll('.toggle-btn');

    toggleButtons.forEach(button => {
        button.addEventListener('click', () => {
            const view = button.getAttribute('data-view');
            const tabContent = button.closest('.tab-content');

            // Update button states
            tabContent.querySelectorAll('.toggle-btn').forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');

            // Toggle views
            if (view === 'alerts') {
                tabContent.querySelectorAll('.alerts-view').forEach(el => el.style.display = '');
                tabContent.querySelectorAll('.fixed-view').forEach(el => el.style.display = 'none');
            } else {
                tabContent.querySelectorAll('.alerts-view').forEach(el => el.style.display = 'none');
                tabContent.querySelectorAll('.fixed-view').forEach(el => el.style.display = 'block');
            }
        });
    });
}

// Tab Navigation
function initializeTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetTab = button.getAttribute('data-tab');

            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));

            button.classList.add('active');
            document.getElementById(targetTab).classList.add('active');

            state.currentTab = targetTab;
        });
    });
}

// Sub-tab Navigation
function initializeSubTabs() {
    const subTabButtons = document.querySelectorAll('.sub-tab-btn');
    const subTabContents = document.querySelectorAll('.sub-tab-content');

    subTabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetSubTab = button.getAttribute('data-subtab');
            
            subTabButtons.forEach(btn => btn.classList.remove('active'));
            subTabContents.forEach(content => content.classList.remove('active'));
            
            button.classList.add('active');
            document.getElementById(targetSubTab).classList.add('active');
            
            state.currentSubTab = targetSubTab;
            updateProductStatsDisplay();
        });
    });
}

// Add filtering headers to tables
function addFilteringHeaders() {
    const tables = [
        'discount-stock-table', 'discount-stock-cleared-table',
        'vendor-status-table', 'vendor-status-cleared-table',
        'stock-table', 'stock-cleared-table',
        'visibility-table', 'visibility-cleared-table'
    ];
    
    tables.forEach(tableId => {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        const thead = table.querySelector('thead');
        const headerRow = thead.querySelector('tr');
        
        // Check if filter row already exists
        if (thead.querySelector('.filter-row')) return;
        
        const filterRow = document.createElement('tr');
        filterRow.classList.add('filter-row');
        
        Array.from(headerRow.children).forEach((th, index) => {
            const filterTh = document.createElement('th');
            const columnName = th.textContent.trim();
            
            // Add filter input based on column type
            const filterInput = createFilterInput(columnName, tableId, index);
            filterTh.appendChild(filterInput);
            
            // Add sort buttons
            const sortDiv = document.createElement('div');
            sortDiv.classList.add('sort-controls');
            sortDiv.innerHTML = `
                <button class="sort-btn" data-direction="asc" title="Sort Ascending">▲</button>
                <button class="sort-btn" data-direction="desc" title="Sort Descending">▼</button>
            `;
            sortDiv.querySelectorAll('.sort-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    sortTable(tableId, index, btn.getAttribute('data-direction'));
                });
            });
            filterTh.appendChild(sortDiv);
            
            filterRow.appendChild(filterTh);
        });
        
        thead.appendChild(filterRow);
    });
}

function createFilterInput(columnName, tableId, columnIndex) {
    const container = document.createElement('div');
    container.classList.add('filter-container');
    
    // Determine filter type based on column name
    const lowerName = columnName.toLowerCase();
    
    // Date columns
    if (lowerName.includes('time') || lowerName.includes('discount start') || lowerName.includes('discount end') || lowerName.includes('cleared')) {
        const input = document.createElement('input');
        input.type = 'date';
        input.classList.add('filter-input');
        input.addEventListener('change', () => applyFilters(tableId));
        container.appendChild(input);
    }
    // Limited value columns (dropdowns)
    else if (lowerName.includes('alert type') || lowerName.includes('business line') || lowerName.includes('vendor status')) {
        const select = document.createElement('select');
        select.classList.add('filter-select');
        select.innerHTML = '<option value="">All</option>';
        select.addEventListener('change', () => applyFilters(tableId));
        container.appendChild(select);
        
        // Populate options dynamically
        setTimeout(() => populateSelectOptions(tableId, columnIndex, select), 100);
    }
    // Numerical columns (NOT vendor_code)
    else if ((lowerName.includes('discount stock') || lowerName.includes('product stock') || 
             lowerName.includes('ratio') || lowerName.includes('headers') || 
             lowerName.includes('issues') || lowerName.includes('rate')) && 
             !lowerName.includes('vendor code')) {
        const operatorSelect = document.createElement('select');
        operatorSelect.classList.add('filter-operator');
        operatorSelect.innerHTML = `
            <option value="">-</option>
            <option value="=">=</option>
            <option value=">">></option>
            <option value=">=">>=</option>
            <option value="<"><</option>
            <option value="<="><=</option>
        `;
        
        const valueInput = document.createElement('input');
        valueInput.type = 'text';
        valueInput.classList.add('filter-value');
        valueInput.placeholder = 'Value';
        
        operatorSelect.addEventListener('change', () => applyFilters(tableId));
        valueInput.addEventListener('input', () => applyFilters(tableId));
        
        container.appendChild(operatorSelect);
        container.appendChild(valueInput);
    }
    // String columns (including vendor_code)
    else {
        const input = document.createElement('input');
        input.type = 'text';
        input.classList.add('filter-input');
        input.placeholder = 'Contains...';
        input.addEventListener('input', () => applyFilters(tableId));
        container.appendChild(input);
    }
    
    return container;
}

function populateSelectOptions(tableId, columnIndex, select) {
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');
    const values = new Set();
    
    Array.from(tbody.rows).forEach(row => {
        if (row.cells[columnIndex]) {
            const value = row.cells[columnIndex].textContent.trim();
            if (value && value !== 'N/A') {
                values.add(value);
            }
        }
    });
    
    Array.from(values).sort().forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
    });
}

function applyFilters(tableId) {
    const table = document.getElementById(tableId);
    const filterRow = table.querySelector('.filter-row');
    const tbody = table.querySelector('tbody');
    
    if (!filterRow) return;
    
    const filters = [];
    Array.from(filterRow.children).forEach((th, index) => {
        const filterInput = th.querySelector('.filter-input');
        const filterSelect = th.querySelector('.filter-select');
        const filterOperator = th.querySelector('.filter-operator');
        const filterValue = th.querySelector('.filter-value');
        
        if (filterInput && filterInput.value) {
            filters.push({index, type: 'contains', value: filterInput.value.toLowerCase()});
        } else if (filterSelect && filterSelect.value) {
            filters.push({index, type: 'exact', value: filterSelect.value});
        } else if (filterOperator && filterOperator.value && filterValue && filterValue.value) {
            filters.push({
                index, 
                type: 'numerical', 
                operator: filterOperator.value,
                value: parseFloat(filterValue.value)
            });
        }
    });
    
    // Apply filters to rows
    Array.from(tbody.rows).forEach(row => {
        let show = true;
        
        filters.forEach(filter => {
            if (!row.cells[filter.index]) {
                show = false;
                return;
            }
            
            const cellValue = row.cells[filter.index].textContent.trim();
            
            if (filter.type === 'contains') {
                if (!cellValue.toLowerCase().includes(filter.value)) {
                    show = false;
                }
            } else if (filter.type === 'exact') {
                if (cellValue !== filter.value) {
                    show = false;
                }
            } else if (filter.type === 'numerical') {
                // Remove % sign if present
                const cleanValue = cellValue.replace('%', '');
                const numValue = parseFloat(cleanValue);
                if (isNaN(numValue)) {
                    show = false;
                } else {
                    switch (filter.operator) {
                        case '=':
                            if (numValue !== filter.value) show = false;
                            break;
                        case '>':
                            if (numValue <= filter.value) show = false;
                            break;
                        case '>=':
                            if (numValue < filter.value) show = false;
                            break;
                        case '<':
                            if (numValue >= filter.value) show = false;
                            break;
                        case '<=':
                            if (numValue > filter.value) show = false;
                            break;
                    }
                }
            }
        });
        
        row.style.display = show ? '' : 'none';
    });
}

function sortTable(tableId, columnIndex, direction) {
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.rows);
    
    const sortedRows = rows.sort((a, b) => {
        const aValue = a.cells[columnIndex].textContent.trim();
        const bValue = b.cells[columnIndex].textContent.trim();
        
        // Try to parse as number (remove % if present)
        const aClean = aValue.replace('%', '');
        const bClean = bValue.replace('%', '');
        const aNum = parseFloat(aClean);
        const bNum = parseFloat(bClean);
        
        let comparison = 0;
        
        // Check if both are valid numbers
        if (!isNaN(aNum) && !isNaN(bNum)) {
            comparison = aNum - bNum;
        }
        // Check if both are dates
        else if (aValue.match(/\d{4}-\d{2}-\d{2}/) && bValue.match(/\d{4}-\d{2}-\d{2}/)) {
            const aDate = new Date(aValue);
            const bDate = new Date(bValue);
            comparison = aDate - bDate;
        }
        // Otherwise compare as strings
        else {
            comparison = aValue.localeCompare(bValue);
        }
        
        return direction === 'asc' ? comparison : -comparison;
    });
    
    // Clear tbody and append sorted rows
    tbody.innerHTML = '';
    sortedRows.forEach(row => tbody.appendChild(row));
}

// Button State Management
function lockUploadButton() {
    const uploadLabel = document.querySelector('label[for="vendor-upload"]');
    const clearButton = document.getElementById('clear-vendors');

    if (uploadLabel) {
        uploadLabel.style.opacity = '0.5';
        uploadLabel.style.cursor = 'not-allowed';
        uploadLabel.style.pointerEvents = 'none';
        uploadLabel.setAttribute('title', 'Please clear vendor codes first before uploading new ones');
    }

    if (clearButton) {
        clearButton.style.opacity = '1';
        clearButton.style.cursor = 'pointer';
        clearButton.style.pointerEvents = 'auto';
        clearButton.disabled = false;
    }
}

function unlockUploadButton() {
    const uploadLabel = document.querySelector('label[for="vendor-upload"]');
    const clearButton = document.getElementById('clear-vendors');

    if (uploadLabel) {
        uploadLabel.style.opacity = '1';
        uploadLabel.style.cursor = 'pointer';
        uploadLabel.style.pointerEvents = 'auto';
        uploadLabel.setAttribute('title', 'Click to upload vendor codes file');
    }

    if (clearButton) {
        clearButton.style.opacity = '0.5';
        clearButton.style.cursor = 'not-allowed';
        clearButton.style.pointerEvents = 'none';
        clearButton.disabled = true;
    }
}

// Vendor Upload
function initializeVendorUpload() {
    const uploadInput = document.getElementById('vendor-upload');
    const clearButton = document.getElementById('clear-vendors');

    // Initialize button states on page load
    unlockUploadButton();

    uploadInput.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('/api/upload-vendors', {
                method: 'POST',
                body: formData
            });

            const result = await response.json();
            console.log('Upload response:', result);  // DEBUG

            // Handle 409 Conflict - active session already exists
            if (response.status === 409) {
                console.warn('Upload rejected: Active session exists', result);  // DEBUG
                showNotification('⚠️ You already have vendor codes loaded. Please click "Clear vendor codes" button first.', 'error');
                lockUploadButton();
                uploadInput.value = '';
                return;
            }

            if (result.success) {
                showNotification(`✅ Uploaded ${result.count} vendor codes. Monitoring started!`, 'success');
                state.vendorCodes = result.count;

                // Store session ID for WebSocket registration
                if (result.session_id) {
                    state.sessionId = result.session_id;
                    console.log('Session ID:', result.session_id);  // DEBUG
                    // Register session with WebSocket
                    if (socket && socket.connected) {
                        socket.emit('register_session', { session_id: result.session_id });
                        console.log('Registered session with WebSocket');  // DEBUG
                    }
                }

                // Process initial alerts from HTTP response
                console.log('Processing alerts from HTTP response...');  // DEBUG
                console.log('Alerts object:', result.alerts);  // DEBUG
                if (result.alerts) {
                    // Discount stock alerts
                    if (result.alerts.discount_stock && result.alerts.discount_stock.length > 0) {
                        console.log(`Processing ${result.alerts.discount_stock.length} discount stock alerts`);  // DEBUG
                        result.alerts.discount_stock.forEach(alert => {
                            handleNewAlert({ tab: 'discount_stock', alert: alert, is_new: true });
                        });
                    }
                    // Vendor status alerts
                    if (result.alerts.vendor_status && result.alerts.vendor_status.length > 0) {
                        console.log(`Processing ${result.alerts.vendor_status.length} vendor status alerts`);  // DEBUG
                        result.alerts.vendor_status.forEach(alert => {
                            handleNewAlert({ tab: 'vendor_status', alert: alert, is_new: true });
                        });
                    }
                    // Stock alerts
                    if (result.alerts.vendor_product_stock && result.alerts.vendor_product_stock.length > 0) {
                        console.log(`Processing ${result.alerts.vendor_product_stock.length} stock alerts`);  // DEBUG
                        result.alerts.vendor_product_stock.forEach(alert => {
                            handleNewAlert({ tab: 'vendor_product_stock', alert: alert, is_new: true });
                        });
                    }
                    // Visibility alerts
                    if (result.alerts.vendor_product_visibility && result.alerts.vendor_product_visibility.length > 0) {
                        console.log(`Processing ${result.alerts.vendor_product_visibility.length} visibility alerts`);  // DEBUG
                        result.alerts.vendor_product_visibility.forEach(alert => {
                            handleNewAlert({ tab: 'vendor_product_visibility', alert: alert, is_new: true });
                        });
                    }
                }

                // Update stats if provided
                console.log('Processing stats...');  // DEBUG
                if (result.stats) {
                    console.log('Stats:', result.stats);  // DEBUG
                    if (result.stats.vendor_status) {
                        handleStatsUpdate({ type: 'vendor_status', ...result.stats.vendor_status });
                    }
                    if (result.stats.vendor_product) {
                        handleStatsUpdate({ type: 'vendor_product', ...result.stats.vendor_product });
                    }
                }

                // Set total alerts from actual data counts
                if (result.cache_status && result.cache_status.data_counts) {
                    console.log('Data counts:', result.cache_status.data_counts);  // DEBUG
                    state.totalAlerts.discount_stock = result.cache_status.data_counts.discount_stock;
                    state.totalAlerts.vendor_status = result.cache_status.data_counts.vendor_status;
                    state.totalAlerts.product_status = result.cache_status.data_counts.vendor_product_status;
                    console.log('Total alerts updated:', state.totalAlerts);  // DEBUG
                }

                console.log('✅ Upload processing complete');  // DEBUG

                // Lock the upload button after successful upload
                lockUploadButton();

                // Start auto-refresh timer (every 3 minutes)
                startAutoRefresh();
            } else {
                console.error('Upload failed:', result);  // DEBUG
                showNotification('Error uploading vendors', 'error');
            }
        } catch (error) {
            console.error('Error uploading vendors:', error);
            showNotification('Error uploading vendors', 'error');
        }

        uploadInput.value = '';
    });

    clearButton.addEventListener('click', async () => {
        if (!confirm('⚠️ WARNING: Clearing vendor codes will stop all monitoring and remove all alerts. Are you sure?')) {
            return;
        }

        // Disable clear button during the request
        clearButton.disabled = true;
        clearButton.style.opacity = '0.5';

        try {
            const sessionId = state.sessionId;
            if (!sessionId) {
                showNotification('⚠️ No active session to clear', 'error');
                clearButton.disabled = false;
                clearButton.style.opacity = '1';
                return;
            }

            const response = await fetch(`/api/clear-vendors/${sessionId}`, {
                method: 'POST'
            });

            const result = await response.json();

            if (result.success) {
                // Stop auto-refresh timer
                stopAutoRefresh();

                // Clear state
                state.sessionId = null;
                state.vendorCodes = 0;

                clearAllTables();
                clearAllStats();
                showNotification('🛑 Cleared all vendor codes. Monitoring stopped.', 'success');

                // Unlock the upload button after clearing
                unlockUploadButton();
            } else {
                console.error('Clear failed:', result);  // DEBUG
                showNotification('Error clearing vendor codes', 'error');
                clearButton.disabled = false;
                clearButton.style.opacity = '1';
            }
        } catch (error) {
            console.error('Error clearing vendors:', error);
            showNotification('Error clearing vendors', 'error');
            clearButton.disabled = false;
            clearButton.style.opacity = '1';
        }
    });
}

// Auto-refresh functions
function startAutoRefresh() {
    // Clear any existing timer
    stopAutoRefresh();

    console.log('🔄 Starting auto-refresh (every 3 minutes)...');

    // Refresh every 3 minutes (180,000 milliseconds)
    state.refreshTimer = setInterval(async () => {
        if (!state.sessionId) {
            console.warn('No session ID - stopping auto-refresh');
            stopAutoRefresh();
            return;
        }

        console.log('⏰ Auto-refresh triggered - fetching fresh data...');
        await refreshData();
    }, 180000);  // 3 minutes
}

function stopAutoRefresh() {
    if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
        console.log('🛑 Auto-refresh stopped');
    }
}

async function refreshData() {
    if (!state.sessionId) {
        console.error('Cannot refresh: no session ID');
        return;
    }

    try {
        console.log(`📥 Fetching fresh data for session ${state.sessionId}...`);

        const response = await fetch(`/api/refresh-data/${state.sessionId}`);
        const result = await response.json();

        if (result.success) {
            console.log('✅ Received fresh data:', result);
            processRefreshData(result);
            showNotification('🔄 Data refreshed successfully', 'success');
        } else {
            console.error('Refresh failed:', result);
            if (response.status === 404) {
                // Session expired
                showNotification('⚠️ Session expired. Please upload vendors again.', 'warning');
                stopAutoRefresh();
                state.sessionId = null;
            }
        }
    } catch (error) {
        console.error('Error refreshing data:', error);
        showNotification('⚠️ Failed to refresh data', 'error');
    }
}

function processRefreshData(data) {
    console.log('🔄 Processing refreshed data...');

    // Process new/updated alerts
    if (data.alerts) {
        // Discount stock alerts
        if (data.alerts.discount_stock) {
            data.alerts.discount_stock.forEach(alert => {
                handleNewAlert({ tab: 'discount_stock', alert: alert, is_new: false });
            });
        }

        // Vendor status alerts
        if (data.alerts.vendor_status) {
            data.alerts.vendor_status.forEach(alert => {
                handleNewAlert({ tab: 'vendor_status', alert: alert, is_new: false });
            });
        }

        // Stock alerts
        if (data.alerts.vendor_product_stock) {
            data.alerts.vendor_product_stock.forEach(alert => {
                handleNewAlert({ tab: 'vendor_product_stock', alert: alert, is_new: false });
            });
        }

        // Visibility alerts
        if (data.alerts.vendor_product_visibility) {
            data.alerts.vendor_product_visibility.forEach(alert => {
                handleNewAlert({ tab: 'vendor_product_visibility', alert: alert, is_new: false });
            });
        }
    }

    // Update stats
    if (data.stats) {
        if (data.stats.vendor_status) {
            handleStatsUpdate({ type: 'vendor_status', ...data.stats.vendor_status });
        }
        if (data.stats.vendor_product) {
            handleStatsUpdate({ type: 'vendor_product', ...data.stats.vendor_product });
        }
    }

    console.log('✅ Refresh processing complete');
}

// WebSocket Event Handlers
function initializeWebSocket() {
    socket.on('connect', () => {
        console.log('Connected to server');
        showConnectionStatus('connected');
    });

    socket.on('disconnect', () => {
        console.log('Disconnected from server');
        showConnectionStatus('disconnected');
    });

    socket.on('new_alert', (data) => {
        handleNewAlert(data);
    });

    socket.on('update_alert', (data) => {
        handleUpdateAlert(data);
    });

    socket.on('alert_cleared', (data) => {
        handleClearedAlert(data);
    });

    socket.on('stats_update', (data) => {
        handleStatsUpdate(data);
    });

    socket.on('clear_all_alerts', () => {
        clearAllTables();
        clearAllStats();
    });
}

// Handle New Alert
function handleNewAlert(data) {
    const { tab, alert, is_new } = data;
    
    // Store alert in state
    const key = alert.product_id || alert.vendor_code;
    if (tab === 'discount_stock') {
        state.alerts.discount_stock[key] = alert;
        addOrUpdateAlertRow('discount-stock-table', alert, [
            'time', 'alert_type', 'vendor_code', 'vendor_name', 
            'discount_stock', 'product_stock', 'product_discount_ratio',
            'product_header_name', 'product_name', 'discount_start_at', 'discount_end_at'
        ]);
        updateProgressBar('discount');
    } else if (tab === 'vendor_status') {
        state.alerts.vendor_status[key] = alert;
        addOrUpdateAlertRow('vendor-status-table', alert, [
            'time', 'alert_type', 'vendor_code', 'vendor_name', 'vendor_status'
        ]);
        updateProgressBar('vendor-status');
    } else if (tab === 'vendor_product_stock') {
        state.alerts.vendor_product_stock[key] = alert;
        addOrUpdateAlertRow('stock-table', alert, [
            'time', 'alert_type', 'vendor_code', 'vendor_name', 
            'business_line', 'total_p_headers', 'stock_issues', 'stock_rate'
        ]);
        updateProgressBar('product-status');
    } else if (tab === 'vendor_product_visibility') {
        state.alerts.vendor_product_visibility[key] = alert;
        addOrUpdateAlertRow('visibility-table', alert, [
            'time', 'alert_type', 'vendor_code', 'vendor_name', 
            'business_line', 'total_p_headers', 'visibility_issues', 'visibility_rate'
        ]);
        updateProgressBar('product-status');
    }
    
    if (is_new) {
        showNotification(`New alert: ${alert.alert_type}`, 'info');
    }
    
    // Refresh select options if needed
    setTimeout(() => {
        const tableId = tab === 'discount_stock' ? 'discount-stock-table' :
                       tab === 'vendor_status' ? 'vendor-status-table' :
                       tab === 'vendor_product_stock' ? 'stock-table' : 'visibility-table';
        refreshSelectOptions(tableId);
    }, 100);
}

function handleUpdateAlert(data) {
    handleNewAlert(data);
}

function handleClearedAlert(data) {
    const { tab, product_id, vendor_code, alert } = data;
    const key = product_id || vendor_code;
    
    // Add to cleared alerts state
    if (tab === 'discount_stock') {
        state.clearedAlerts.discount_stock[key] = alert;
        addClearedAlertRow('discount-stock-cleared-table', alert, [
            'cleared_at', 'alert_type', 'vendor_code', 'vendor_name', 
            'discount_stock', 'product_stock', 'product_discount_ratio',
            'product_header_name', 'product_name', 'discount_start_at', 'discount_end_at'
        ]);
        updateProgressBar('discount');
    } else if (tab === 'vendor_status') {
        state.clearedAlerts.vendor_status[key] = alert;
        addClearedAlertRow('vendor-status-cleared-table', alert, [
            'cleared_at', 'alert_type', 'vendor_code', 'vendor_name', 'vendor_status'
        ]);
        updateProgressBar('vendor-status');
    } else if (tab === 'vendor_product_stock') {
        state.clearedAlerts.vendor_product_stock[key] = alert;
        addClearedAlertRow('stock-cleared-table', alert, [
            'cleared_at', 'alert_type', 'vendor_code', 'vendor_name', 
            'business_line', 'total_p_headers', 'stock_issues', 'stock_rate'
        ]);
        updateProgressBar('product-status');
    } else if (tab === 'vendor_product_visibility') {
        state.clearedAlerts.vendor_product_visibility[key] = alert;
        addClearedAlertRow('visibility-cleared-table', alert, [
            'cleared_at', 'alert_type', 'vendor_code', 'vendor_name', 
            'business_line', 'total_p_headers', 'visibility_issues', 'visibility_rate'
        ]);
        updateProgressBar('product-status');
    }
    
    // Remove from active table
    const activeTableId = tab === 'discount_stock' ? 'discount-stock-table' :
                          tab === 'vendor_status' ? 'vendor-status-table' :
                          tab === 'vendor_product_stock' ? 'stock-table' : 'visibility-table';
    
    const table = document.getElementById(activeTableId);
    const tbody = table.querySelector('tbody');
    const row = Array.from(tbody.rows).find(r => r.dataset.key === key);
    if (row) {
        row.remove();
    }
    
    showNotification(`🎉 Alert cleared: ${alert.alert_type}`, 'success');
}

function addClearedAlertRow(tableId, alert, columns) {
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');

    const row = tbody.insertRow(0);
    row.dataset.key = alert.product_id || alert.vendor_code;

    // Apply appropriate severity class
    const severityClass = alert.severity ? `severity-${alert.severity}` : 'severity-green';
    row.classList.add(severityClass, 'new-alert');

    columns.forEach((column) => {
        const cell = row.insertCell();
        let value = alert[column] || 'N/A';
        // Add % to product_discount_ratio
        if (column === 'product_discount_ratio' && value !== 'N/A') {
            value = value + '%';
        }
        cell.textContent = value;
    });

    setTimeout(() => {
        row.classList.remove('new-alert');
    }, 1000);

    // Refresh filter options
    setTimeout(() => refreshSelectOptions(tableId), 100);
}

function updateProgressBar(type) {
    let cleared = 0;
    let total = state.totalAlerts[type] || 1;
    
    if (type === 'discount') {
        cleared = Object.keys(state.clearedAlerts.discount_stock).length;
        const active = Object.keys(state.alerts.discount_stock).length;
        total = Math.max(cleared + active, total);
        
        updateProgressBarUI('discount-progress', cleared, total);
    } else if (type === 'vendor-status') {
        cleared = Object.keys(state.clearedAlerts.vendor_status).length;
        const active = Object.keys(state.alerts.vendor_status).length;
        total = Math.max(cleared + active, total);
        
        updateProgressBarUI('vendor-status-progress', cleared, total);
    } else if (type === 'product-status') {
        cleared = Object.keys(state.clearedAlerts.vendor_product_stock).length + 
                  Object.keys(state.clearedAlerts.vendor_product_visibility).length;
        const active = Object.keys(state.alerts.vendor_product_stock).length + 
                       Object.keys(state.alerts.vendor_product_visibility).length;
        total = Math.max(cleared + active, total);
        
        updateProgressBarUI('product-status-progress', cleared, total);
    }
}

function updateProgressBarUI(prefix, cleared, total) {
    const percentage = total > 0 ? Math.round((cleared / total) * 100) : 0;
    const fill = document.getElementById(`${prefix}-fill`);
    const text = document.getElementById(`${prefix}-text`);
    
    if (fill) {
        fill.style.width = `${percentage}%`;
        fill.textContent = `${percentage}%`;
    }
    if (text) {
        text.textContent = `${cleared} cleared of ${total}`;
    }
}

function refreshSelectOptions(tableId) {
    const table = document.getElementById(tableId);
    const filterRow = table.querySelector('.filter-row');
    if (!filterRow) return;
    
    Array.from(filterRow.children).forEach((th, index) => {
        const select = th.querySelector('.filter-select');
        if (select) {
            const currentValue = select.value;
            const values = new Set();
            
            const tbody = table.querySelector('tbody');
            Array.from(tbody.rows).forEach(row => {
                if (row.cells[index]) {
                    const value = row.cells[index].textContent.trim();
                    if (value && value !== 'N/A') {
                        values.add(value);
                    }
                }
            });
            
            // Rebuild options
            select.innerHTML = '<option value="">All</option>';
            Array.from(values).sort().forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });
            
            // Restore previous value if it still exists
            if (currentValue && values.has(currentValue)) {
                select.value = currentValue;
            }
        }
    });
}

// Add or Update Alert Row
function addOrUpdateAlertRow(tableId, alert, columns) {
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');
    const key = alert.product_id || alert.vendor_code;

    // Check if row already exists
    let row = Array.from(tbody.rows).find(r => r.dataset.key === key);

    if (!row) {
        row = tbody.insertRow(0);
        row.dataset.key = key;
        row.classList.add('new-alert');

        columns.forEach(() => {
            row.insertCell();
        });

        setTimeout(() => {
            row.classList.remove('new-alert');
        }, 1000);
    }

    // Update cell values
    columns.forEach((column, index) => {
        let value = alert[column] || 'N/A';
        // Add % to product_discount_ratio
        if (column === 'product_discount_ratio' && value !== 'N/A') {
            value = value + '%';
        }
        row.cells[index].textContent = value;
    });

    // Apply severity coloring
    row.classList.remove('severity-red', 'severity-yellow', 'severity-green', 'severity-cherry',
                        'severity-red-light', 'severity-red-medium', 'severity-red-high',
                        'severity-light-green', 'severity-dark-green');
    if (alert.severity) {
        row.classList.add(`severity-${alert.severity}`);
    }
}

// Handle Stats Update
function handleStatsUpdate(data) {
    if (data.type === 'vendor_status') {
        updateVendorStatusStats(data);
    } else if (data.type === 'vendor_product') {
        updateVendorProductStats(data);
    }
}

// Update Vendor Status Stats
function updateVendorStatusStats(data) {
    document.getElementById('total-vendors').textContent = data.total_vendors;
    document.getElementById('active-vendors').textContent = data.active_vendors;
    document.getElementById('inactive-vendors').textContent = data.inactive_vendors;
}

// Update Vendor Product Stats
function updateVendorProductStats(data) {
    document.getElementById('product-total-vendors').textContent = data.total_vendors;
    
    const businessLinesDiv = document.getElementById('business-lines');
    businessLinesDiv.innerHTML = '';
    for (const [line, count] of Object.entries(data.business_lines)) {
        const lineDiv = document.createElement('div');
        lineDiv.innerHTML = `<span>${line}</span><span>${count}</span>`;
        businessLinesDiv.appendChild(lineDiv);
    }
    
    state.currentStats = data;
    updateProductStatsDisplay();
}

// Update Product Stats Display
function updateProductStatsDisplay() {
    if (!state.currentStats) return;
    
    const alertCountsDiv = document.getElementById('alert-counts');
    const alertCountsTitle = document.getElementById('alert-counts-title');
    const exportBtn = document.getElementById('export-product-btn');
    
    alertCountsDiv.innerHTML = '';
    
    if (state.currentSubTab === 'stock') {
        alertCountsTitle.textContent = 'Count Alert Types Stock';
        exportBtn.textContent = 'Export Excel Report of Stock Issues';
        exportBtn.onclick = exportStockIssues;
        
        const counts = state.currentStats.stock_alert_counts;
        const hasIssuesDiv = document.createElement('div');
        hasIssuesDiv.innerHTML = `<span>Vendor Has Stock Issues</span><span>${counts.has_issues}</span>`;
        alertCountsDiv.appendChild(hasIssuesDiv);
        
        const hadIssuesDiv = document.createElement('div');
        hadIssuesDiv.innerHTML = `<span>Vendor Had Stock Issues</span><span>${counts.had_issues}</span>`;
        alertCountsDiv.appendChild(hadIssuesDiv);
        
        const clearedDiv = document.createElement('div');
        clearedDiv.innerHTML = `<span>Cleared Stock Alerts</span><span>${state.currentStats.stock_cleared_count || 0}</span>`;
        clearedDiv.style.fontWeight = 'bold';
        clearedDiv.style.color = '#28a745';
        alertCountsDiv.appendChild(clearedDiv);
    } else {
        alertCountsTitle.textContent = 'Count Alert Types Visibility';
        exportBtn.textContent = 'Export Excel Report of Visibility Issues';
        exportBtn.onclick = exportVisibilityIssues;
        
        const counts = state.currentStats.visibility_alert_counts;
        const hasIssuesDiv = document.createElement('div');
        hasIssuesDiv.innerHTML = `<span>Vendor Has Visibility Issues</span><span>${counts.has_issues}</span>`;
        alertCountsDiv.appendChild(hasIssuesDiv);
        
        const hadIssuesDiv = document.createElement('div');
        hadIssuesDiv.innerHTML = `<span>Vendor Had Visibility Issues</span><span>${counts.had_issues}</span>`;
        alertCountsDiv.appendChild(hadIssuesDiv);
        
        const clearedDiv = document.createElement('div');
        clearedDiv.innerHTML = `<span>Cleared Visibility Alerts</span><span>${state.currentStats.visibility_cleared_count || 0}</span>`;
        clearedDiv.style.fontWeight = 'bold';
        clearedDiv.style.color = '#28a745';
        alertCountsDiv.appendChild(clearedDiv);
    }
}

// Export Functions
async function exportActiveVendors() {
    downloadFile('/api/export-active-vendors', 'active_vendors.xlsx');
}

async function exportInactiveVendors() {
    downloadFile('/api/export-inactive-vendors', 'inactive_vendors.xlsx');
}

async function exportStockIssues() {
    downloadFile('/api/export-stock-issues', 'stock_issues.xlsx');
}

async function exportVisibilityIssues() {
    downloadFile('/api/export-visibility-issues', 'visibility_issues.xlsx');
}

async function downloadFile(url, filename) {
    try {
        const response = await fetch(url);
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        document.body.removeChild(a);
        showNotification(`Exported ${filename}`, 'success');
    } catch (error) {
        console.error(`Error exporting ${filename}:`, error);
        showNotification(`Error exporting ${filename}`, 'error');
    }
}

// Utility Functions
function clearAllTables() {
    const tables = [
        'discount-stock-table', 'discount-stock-cleared-table',
        'vendor-status-table', 'vendor-status-cleared-table',
        'stock-table', 'stock-cleared-table',
        'visibility-table', 'visibility-cleared-table'
    ];
    
    tables.forEach(tableId => {
        const table = document.getElementById(tableId);
        if (table) {
            const tbody = table.querySelector('tbody');
            tbody.innerHTML = '';
        }
    });
    
    state.alerts = {
        discount_stock: {},
        vendor_status: {},
        vendor_product_stock: {},
        vendor_product_visibility: {}
    };
    
    state.clearedAlerts = {
        discount_stock: {},
        vendor_status: {},
        vendor_product_stock: {},
        vendor_product_visibility: {}
    };
    
    // Reset progress bars
    updateProgressBar('discount');
    updateProgressBar('vendor-status');
    updateProgressBar('product-status');
}

function clearAllStats() {
    document.getElementById('total-vendors').textContent = '0';
    document.getElementById('active-vendors').textContent = '0';
    document.getElementById('inactive-vendors').textContent = '0';
    document.getElementById('product-total-vendors').textContent = '0';
    document.getElementById('business-lines').innerHTML = '';
    document.getElementById('alert-counts').innerHTML = '';
}

function showNotification(message, type) {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 20px;
        border-radius: 6px;
        color: white;
        font-weight: 500;
        z-index: 2000;
        animation: slideIn 0.3s ease-out;
        max-width: 400px;
    `;
    
    switch(type) {
        case 'success':
            notification.style.backgroundColor = '#28a745';
            break;
        case 'error':
            notification.style.backgroundColor = '#dc3545';
            break;
        case 'info':
            notification.style.backgroundColor = '#17a2b8';
            break;
    }
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease-in';
        setTimeout(() => {
            if (notification.parentNode) {
                document.body.removeChild(notification);
            }
        }, 300);
    }, 3000);
}

function showConnectionStatus(status) {
    let statusElement = document.querySelector('.connection-status');
    
    if (!statusElement) {
        statusElement = document.createElement('div');
        statusElement.className = 'connection-status';
        document.body.appendChild(statusElement);
    }
    
    statusElement.className = `connection-status ${status}`;
    statusElement.textContent = status === 'connected' ? '● Connected' : '● Disconnected';
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
    
    .filter-row th {
        padding: 5px !important;
        background-color: #f0f0f0;
    }

    [data-theme="dark"] .filter-row th {
        background-color: #161b22;
        border-bottom: 2px solid #30363d;
    }

    .filter-container {
        display: flex;
        gap: 3px;
        align-items: center;
        flex-wrap: wrap;
    }

    .filter-input, .filter-select, .filter-operator, .filter-value {
        font-size: 11px;
        padding: 3px;
        border: 1px solid #ddd;
        border-radius: 3px;
        background-color: #fff;
        color: #333;
    }

    [data-theme="dark"] .filter-input,
    [data-theme="dark"] .filter-select,
    [data-theme="dark"] .filter-operator,
    [data-theme="dark"] .filter-value {
        background-color: #0d1117;
        border-color: #30363d;
        color: #e6edf3;
    }
    
    .filter-input, .filter-select {
        width: 100%;
        min-width: 80px;
    }
    
    .filter-operator {
        width: 45px;
        flex-shrink: 0;
    }
    
    .filter-value {
        width: 60px;
        flex-shrink: 0;
    }
    
    .sort-controls {
        display: flex;
        gap: 2px;
        justify-content: center;
        margin-top: 3px;
    }
    
    .sort-btn {
        font-size: 10px;
        padding: 2px 6px;
        background: #fff;
        border: 1px solid #ddd;
        cursor: pointer;
        border-radius: 3px;
        color: #333;
    }

    [data-theme="dark"] .sort-btn {
        background: #161b22;
        border-color: #30363d;
        color: #e6edf3;
    }

    .sort-btn:hover {
        background: #007bff;
        color: white;
    }

    [data-theme="dark"] .sort-btn:hover {
        background: #4a9eff;
        color: white;
    }
`;
document.head.appendChild(style);
