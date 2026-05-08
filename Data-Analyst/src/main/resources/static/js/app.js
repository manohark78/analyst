/**
 * Main Application Controller
 * Manages state, UI rendering, and user interactions.
 */
(function () {
    'use strict';

    // === STATE ===
    const state = {
        activeDataset: null,      // { id, name, tableName, columns, rowCount }
        datasets: [],              // All uploaded datasets
        conversations: [],         // All conversations
        activeConversation: null,  // { id, title, messages }
        isStreaming: false,
        streamController: null,
        theme: localStorage.getItem('theme') || 'dark'
    };

    // === DOM REFS ===
    const $ = id => document.getElementById(id);
    const userInput = $('user-input');
    const sendBtn = $('send-btn');
    const chatArea = $('chat-area');
    const messagesContainer = $('messages-container');
    const welcomeScreen = $('welcome-screen');
    const fileUpload = $('file-upload');
    const uploadZone = $('upload-zone');
    const datasetList = $('dataset-list');
    const historyList = $('history-list');
    const headerDataset = $('header-dataset');
    const statusDot = $('status-dot');
    const statusText = $('status-text');
    const previewBtn = $('preview-btn');
    const schemaBtn = $('schema-btn');
    const previewModal = $('preview-modal');
    const schemaModal = $('schema-modal');
    const previewClose = $('preview-close');
    const schemaClose = $('schema-close');
    const previewTable = $('preview-table');
    const schemaBody = $('schema-body');
    const themeToggle = $('theme-toggle');
    const newChatBtn = $('new-chat-btn');
    const sidebarToggle = $('sidebar-toggle');
    const sidebar = $('sidebar');

    // === INIT ===
    async function init() {
        applyTheme(state.theme);
        bindEvents();
        await checkLlmHealth();
        await loadDatasets();
        await loadConversations();
        setInterval(checkLlmHealth, 15000); // Poll every 15s
    }

    // === THEME ===
    function applyTheme(theme) {
        document.body.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        state.theme = theme;
    }

    // === HEALTH CHECK ===
    async function checkLlmHealth() {
        try {
            const health = await API.checkHealth();
            if (health.llmRunning) {
                statusDot.classList.add('online');
                statusText.textContent = 'LLM Online';
            } else {
                statusDot.classList.remove('online');
                statusText.textContent = 'LLM Offline';
            }
        } catch {
            statusDot.classList.remove('online');
            statusText.textContent = 'Server Down';
        }
    }

    // === EVENT BINDINGS ===
    function bindEvents() {
        // Input
        userInput.addEventListener('input', handleInputResize);
        userInput.addEventListener('keydown', handleInputKeydown);
        sendBtn.addEventListener('click', sendMessage);

        // File upload
        fileUpload.addEventListener('change', handleFileUpload);
        uploadZone.addEventListener('dragover', e => { e.preventDefault(); uploadZone.classList.add('dragover'); });
        uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('dragover'));
        uploadZone.addEventListener('drop', handleFileDrop);

        // Modals
        previewBtn.addEventListener('click', showDataPreview);
        schemaBtn.addEventListener('click', showSchema);
        previewClose.addEventListener('click', () => previewModal.classList.remove('visible'));
        schemaClose.addEventListener('click', () => schemaModal.classList.remove('visible'));
        previewModal.addEventListener('click', e => { if (e.target === previewModal) previewModal.classList.remove('visible'); });
        schemaModal.addEventListener('click', e => { if (e.target === schemaModal) schemaModal.classList.remove('visible'); });

        // Theme toggle
        themeToggle.addEventListener('click', () => applyTheme(state.theme === 'dark' ? 'light' : 'dark'));

        // New chat
        newChatBtn.addEventListener('click', startNewChat);

        // Sidebar toggle (mobile)
        if (sidebarToggle) {
            sidebarToggle.addEventListener('click', () => sidebar.classList.toggle('open'));
        }

        // Suggestion cards
        document.querySelectorAll('.suggestion-card').forEach(card => {
            card.addEventListener('click', () => {
                userInput.value = card.dataset.prompt;
                handleInputResize();
                sendMessage();
            });
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', e => {
            if (e.ctrlKey && e.key === 'p') { e.preventDefault(); showDataPreview(); }
            if (e.ctrlKey && e.key === 'i') { e.preventDefault(); showSchema(); }
            if (e.ctrlKey && e.key === 'n') { e.preventDefault(); startNewChat(); }
        });
    }

    // === INPUT HANDLING ===
    function handleInputResize() {
        userInput.style.height = 'auto';
        userInput.style.height = Math.min(userInput.scrollHeight, 180) + 'px';
        sendBtn.disabled = !userInput.value.trim() || state.isStreaming;
    }

    function handleInputKeydown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            if (!sendBtn.disabled) sendMessage();
        }
    }

    // === SEND MESSAGE ===
    async function sendMessage() {
        const text = userInput.value.trim();
        if (!text || state.isStreaming) return;

        // Hide welcome screen
        if (welcomeScreen) welcomeScreen.style.display = 'none';

        // Auto-create conversation if needed
        if (!state.activeConversation) {
            const title = text.substring(0, 40) + (text.length > 40 ? '...' : '');
            const conv = await API.createConversation(title, state.activeDataset?.id);
            state.activeConversation = conv;
            await loadConversations();
        }

        // Render user message
        appendMessage('user', text);
        userInput.value = '';
        userInput.style.height = 'auto';
        sendBtn.disabled = true;

        // Render AI message placeholder
        const aiMsg = appendMessage('ai', '');
        const bodyDiv = aiMsg.querySelector('.msg-body');
        bodyDiv.innerHTML = '<div class="typing-indicator"><span></span><span></span><span></span></div>';

        state.isStreaming = true;
        let fullResponse = '';
        let sqlPreview = '';

        state.streamController = API.streamChat(
            text,
            state.activeConversation?.id,
            state.activeDataset?.id,
            {
                onToken(token) {
                    if (bodyDiv.querySelector('.typing-indicator')) {
                        bodyDiv.innerHTML = '';
                        bodyDiv.classList.add('streaming-cursor');
                    }
                    fullResponse += token;
                    bodyDiv.innerHTML = renderMarkdown(fullResponse);
                    chatArea.scrollTop = chatArea.scrollHeight;
                },
                onSql(sql) {
                    sqlPreview = sql;
                    const sqlDiv = document.createElement('div');
                    sqlDiv.className = 'sql-preview';
                    sqlDiv.textContent = sql;
                    bodyDiv.appendChild(sqlDiv);
                },
                onChart(data) {
                    renderChart(bodyDiv, data);
                },
                onDone() {
                    bodyDiv.classList.remove('streaming-cursor');
                    state.isStreaming = false;
                    sendBtn.disabled = !userInput.value.trim();
                    chatArea.scrollTop = chatArea.scrollHeight;

                    // Auto-rename conversation based on first message
                    if (state.activeConversation?.messages?.length === 0) {
                        const title = text.substring(0, 40);
                        API.renameConversation(state.activeConversation.id, title).catch(() => {});
                    }
                },
                onError(err) {
                    bodyDiv.classList.remove('streaming-cursor');
                    bodyDiv.innerHTML = `<span style="color: var(--error)">❌ Error: ${err.message || 'Connection failed'}</span>`;
                    state.isStreaming = false;
                    sendBtn.disabled = !userInput.value.trim();
                }
            }
        );
    }

    // === MESSAGE RENDERING ===
    function appendMessage(role, content) {
        const div = document.createElement('div');
        div.className = `message ${role}-msg`;
        div.innerHTML = `
            <div class="msg-avatar">${role === 'user' ? 'U' : 'AI'}</div>
            <div class="msg-body">${content ? renderMarkdown(content) : ''}</div>
        `;
        messagesContainer.appendChild(div);
        chatArea.scrollTop = chatArea.scrollHeight;
        return div;
    }

    // === MARKDOWN RENDERING (lightweight) ===
    function renderMarkdown(text) {
        if (!text) return '';
        let html = text
            // Code blocks
            .replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>')
            // Inline code
            .replace(/`([^`]+)`/g, '<code>$1</code>')
            // Bold
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            // Italic
            .replace(/\*(.+?)\*/g, '<em>$1</em>')
            // Headers
            .replace(/^### (.+)$/gm, '<h4>$1</h4>')
            .replace(/^## (.+)$/gm, '<h3>$1</h3>')
            .replace(/^# (.+)$/gm, '<h2>$1</h2>')
            // Tables (basic)
            .replace(/\|(.+)\|\n\|[-| ]+\|\n([\s\S]*?)(?=\n\n|\n$|$)/g, (match, header, body) => {
                const heads = header.split('|').map(h => `<th>${h.trim()}</th>`).join('');
                const rows = body.trim().split('\n').map(row => {
                    const cells = row.split('|').filter(c => c.trim()).map(c => `<td>${c.trim()}</td>`).join('');
                    return `<tr>${cells}</tr>`;
                }).join('');
                return `<table><thead><tr>${heads}</tr></thead><tbody>${rows}</tbody></table>`;
            })
            // Lists
            .replace(/^- (.+)$/gm, '<li>$1</li>')
            .replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>')
            // Paragraphs
            .replace(/\n\n/g, '</p><p>')
            .replace(/\n/g, '<br>');

        return `<p>${html}</p>`;
    }

    // === CHART RENDERING ===
    function renderChart(container, data) {
        if (!data || data.length === 0) return;

        const chartDiv = document.createElement('div');
        chartDiv.className = 'chart-container';

        const keys = Object.keys(data[0]);
        const labelKey = keys[0];
        const valueKey = keys[1] || keys[0];

        const labels = data.map(d => d[labelKey]);
        const values = data.map(d => d[valueKey]);

        // Simple bar chart using CSS
        const maxVal = Math.max(...values);
        let barsHtml = '';
        data.forEach((d, i) => {
            const pct = (values[i] / maxVal * 100).toFixed(1);
            barsHtml += `
                <div style="display:flex;align-items:center;gap:10px;margin:6px 0;">
                    <span style="width:120px;font-size:12px;text-align:right;color:var(--text-secondary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${labels[i]}</span>
                    <div style="flex:1;background:var(--bg-tertiary);border-radius:4px;overflow:hidden;">
                        <div style="height:24px;width:${pct}%;background:linear-gradient(90deg,var(--accent),#8b5cf6);border-radius:4px;transition:width 0.5s ease;"></div>
                    </div>
                    <span style="width:60px;font-size:12px;color:var(--text-primary);font-weight:500;">${Number(values[i]).toLocaleString()}</span>
                </div>
            `;
        });

        chartDiv.innerHTML = `<div style="font-size:12px;font-weight:600;color:var(--text-secondary);margin-bottom:12px;text-transform:uppercase;">${valueKey} by ${labelKey}</div>${barsHtml}`;
        container.appendChild(chartDiv);
    }

    // === FILE UPLOAD ===
    async function handleFileUpload(e) {
        const file = e.target.files[0];
        if (file) await uploadFile(file);
        fileUpload.value = '';
    }

    function handleFileDrop(e) {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
        const file = e.dataTransfer.files[0];
        if (file) uploadFile(file);
    }

    async function uploadFile(file) {
        const validExts = ['.csv', '.xlsx', '.xls'];
        const ext = '.' + file.name.split('.').pop().toLowerCase();
        if (!validExts.includes(ext)) {
            showToast('Unsupported file type. Use CSV or Excel.', 'error');
            return;
        }

        uploadZone.classList.add('uploading');
        uploadZone.querySelector('span').textContent = 'Uploading...';

        try {
            const datasets = await API.uploadFile(file);
            state.datasets.push(...datasets);
            renderDatasetList();

            // Auto-select first dataset
            if (datasets.length > 0) {
                selectDataset(datasets[0]);
            }

            showToast(`Uploaded ${datasets.length} dataset(s) from ${file.name}`, 'success');
        } catch (err) {
            showToast('Upload failed: ' + err.message, 'error');
        } finally {
            uploadZone.classList.remove('uploading');
            uploadZone.querySelector('span').textContent = 'Drop CSV/Excel or click';
        }
    }

    // === DATASET MANAGEMENT ===
    async function loadDatasets() {
        try {
            state.datasets = await API.listDatasets();
            renderDatasetList();
        } catch { /* ignore on first load */ }
    }

    function renderDatasetList() {
        datasetList.innerHTML = '';
        if (state.datasets.length === 0) return;

        state.datasets.forEach(ds => {
            const item = document.createElement('div');
            item.className = `list-item ${state.activeDataset?.id === ds.id ? 'active' : ''}`;
            item.innerHTML = `
                <svg class="item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                    <polyline points="14 2 14 8 20 8"/>
                </svg>
                <span class="item-label" title="${ds.name}">${ds.name}</span>
                <div class="item-actions">
                    <button class="item-action-btn" title="Remove" data-id="${ds.id}">×</button>
                </div>
            `;
            item.addEventListener('click', (e) => {
                if (e.target.closest('.item-action-btn')) return;
                selectDataset(ds);
            });
            item.querySelector('.item-action-btn').addEventListener('click', async () => {
                await API.deleteDataset(ds.id);
                state.datasets = state.datasets.filter(d => d.id !== ds.id);
                if (state.activeDataset?.id === ds.id) {
                    state.activeDataset = null;
                    updateHeaderDataset();
                }
                renderDatasetList();
                showToast('Dataset removed', 'info');
            });
            datasetList.appendChild(item);
        });
    }

    function selectDataset(ds) {
        state.activeDataset = ds;
        renderDatasetList();
        updateHeaderDataset();
        previewBtn.disabled = false;
        schemaBtn.disabled = false;
    }

    function updateHeaderDataset() {
        const label = headerDataset.querySelector('.header-label');
        if (state.activeDataset) {
            label.textContent = `${state.activeDataset.name} (${state.activeDataset.rowCount?.toLocaleString()} rows)`;
            label.classList.add('has-data');
        } else {
            label.textContent = 'No dataset selected';
            label.classList.remove('has-data');
            previewBtn.disabled = true;
            schemaBtn.disabled = true;
        }
    }

    // === CONVERSATION MANAGEMENT ===
    async function loadConversations() {
        try {
            state.conversations = await API.listConversations();
            renderHistoryList();
        } catch { /* ignore */ }
    }

    function renderHistoryList() {
        historyList.innerHTML = '';
        state.conversations.forEach(conv => {
            const item = document.createElement('div');
            item.className = `list-item ${state.activeConversation?.id === conv.id ? 'active' : ''}`;
            item.innerHTML = `
                <svg class="item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>
                </svg>
                <span class="item-label" title="${conv.title}">${conv.title}</span>
                <div class="item-actions">
                    <button class="item-action-btn" title="Delete">×</button>
                </div>
            `;
            item.addEventListener('click', (e) => {
                if (e.target.closest('.item-action-btn')) return;
                loadConversation(conv.id);
            });
            item.querySelector('.item-action-btn').addEventListener('click', async () => {
                await API.deleteConversation(conv.id);
                if (state.activeConversation?.id === conv.id) {
                    startNewChat();
                }
                await loadConversations();
                showToast('Chat deleted', 'info');
            });
            historyList.appendChild(item);
        });
    }

    async function loadConversation(id) {
        try {
            const conv = await API.getConversation(id);
            state.activeConversation = conv;
            renderHistoryList();

            // Clear and render messages
            messagesContainer.innerHTML = '';
            if (welcomeScreen) welcomeScreen.style.display = 'none';

            if (conv.messages) {
                conv.messages.forEach(msg => {
                    appendMessage(msg.role === 'user' ? 'user' : 'ai', msg.content);
                });
            }
        } catch (err) {
            showToast('Failed to load conversation', 'error');
        }
    }

    function startNewChat() {
        state.activeConversation = null;
        messagesContainer.innerHTML = '';
        if (welcomeScreen) welcomeScreen.style.display = 'flex';
        renderHistoryList();
        userInput.focus();
    }

    // === DATA PREVIEW MODAL ===
    async function showDataPreview() {
        if (!state.activeDataset) return;

        previewModal.classList.add('visible');
        $('preview-modal-title').textContent = `Preview: ${state.activeDataset.name}`;
        previewTable.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Loading...</div>';

        try {
            const data = await API.previewData(state.activeDataset.tableName);
            if (data.length === 0) {
                previewTable.innerHTML = '<div style="padding:20px;text-align:center">No data found.</div>';
                return;
            }

            const headers = Object.keys(data[0]);
            let html = '<table><thead><tr>';
            headers.forEach(h => html += `<th>${h}</th>`);
            html += '</tr></thead><tbody>';
            data.forEach(row => {
                html += '<tr>';
                headers.forEach(h => html += `<td>${row[h] ?? ''}</td>`);
                html += '</tr>';
            });
            html += '</tbody></table>';
            previewTable.innerHTML = html;
        } catch (err) {
            previewTable.innerHTML = `<div style="padding:20px;color:var(--error)">Failed to load: ${err.message}</div>`;
        }
    }

    // === SCHEMA MODAL ===
    function showSchema() {
        if (!state.activeDataset) return;
        schemaModal.classList.add('visible');

        const ds = state.activeDataset;
        let html = `
            <div class="schema-stats">
                <div class="schema-stat"><span class="stat-value">${ds.rowCount?.toLocaleString()}</span><span class="stat-label">Rows</span></div>
                <div class="schema-stat"><span class="stat-value">${ds.columnCount}</span><span class="stat-label">Columns</span></div>
                <div class="schema-stat"><span class="stat-value">${ds.tableName}</span><span class="stat-label">Table</span></div>
            </div>
            <table class="schema-table">
                <thead><tr><th>#</th><th>Column</th><th>Type</th></tr></thead>
                <tbody>
        `;
        ds.columns.forEach((col, i) => {
            html += `<tr><td>${i + 1}</td><td class="col-name">${col.name}</td><td class="col-type">${col.type}</td></tr>`;
        });
        html += '</tbody></table>';
        schemaBody.innerHTML = html;
    }

    // === TOAST NOTIFICATIONS ===
    function showToast(message, type = 'info') {
        const container = $('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateY(10px)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    // === START ===
    document.addEventListener('DOMContentLoaded', init);
})();
