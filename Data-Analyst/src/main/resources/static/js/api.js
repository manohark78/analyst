/**
 * API Client — All backend communication.
 * Supports file upload, SSE streaming, conversations, and health checks.
 */
const API = {
    // === Health ===
    async checkHealth() {
        try {
            const res = await fetch('/api/health');
            return await res.json();
        } catch {
            return { status: 'DOWN', llmRunning: false };
        }
    },

    // === Datasets ===
    async uploadFile(file) {
        const formData = new FormData();
        formData.append('file', file);
        const res = await fetch('/api/datasets/upload', { method: 'POST', body: formData });
        if (!res.ok) throw new Error((await res.json()).error || 'Upload failed');
        return await res.json();
    },

    async listDatasets() {
        const res = await fetch('/api/datasets');
        return await res.json();
    },

    async deleteDataset(id) {
        await fetch(`/api/datasets/${id}`, { method: 'DELETE' });
    },

    // === Data Preview ===
    async previewData(tableName, limit = 50) {
        const res = await fetch(`/api/data/preview?tableName=${encodeURIComponent(tableName)}&limit=${limit}`);
        return await res.json();
    },

    // === Conversations ===
    async createConversation(title, datasetId) {
        const res = await fetch('/api/conversations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title, datasetId })
        });
        return await res.json();
    },

    async listConversations() {
        const res = await fetch('/api/conversations');
        return await res.json();
    },

    async getConversation(id) {
        const res = await fetch(`/api/conversations/${id}`);
        return await res.json();
    },

    async deleteConversation(id) {
        await fetch(`/api/conversations/${id}`, { method: 'DELETE' });
    },

    async renameConversation(id, title) {
        await fetch(`/api/conversations/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title })
        });
    },

    // === Chat (SSE Streaming) ===
    streamChat(message, conversationId, datasetId, { onToken, onSql, onChart, onTable, onDone, onError }) {
        const controller = new AbortController();

        fetch('/api/chat/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message, conversationId, datasetId }),
            signal: controller.signal
        }).then(async (response) => {
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop(); // Keep incomplete line in buffer

                for (const line of lines) {
                    let content = line;
                    if (line.startsWith('data:')) {
                        content = line.substring(5);
                    }

                    // Check for embedded SQL
                    const sqlMatch = content.match(/<sql>([\s\S]*?)<\/sql>/);
                    if (sqlMatch) {
                        onSql(sqlMatch[1].trim());
                        content = content.replace(/<sql>[\s\S]*?<\/sql>/, '');
                    }

                    // Check for embedded chart data
                    const chartMatch = content.match(/<chart>([\s\S]*?)<\/chart>/);
                    if (chartMatch) {
                        try {
                            onChart(JSON.parse(chartMatch[1].trim()));
                        } catch(e) { /* ignore parse errors */ }
                        content = content.replace(/<chart>[\s\S]*?<\/chart>/, '');
                    }

                    // Check for embedded table data
                    const tableMatch = content.match(/<table_data>([\s\S]*?)<\/table_data>/);
                    if (tableMatch) {
                        try {
                            if (onTable) onTable(JSON.parse(tableMatch[1].trim()));
                        } catch(e) { console.error("Table parse error", e); }
                        content = content.replace(/<table_data>[\s\S]*?<\/table_data>/, '');
                    }

                    if (content) {
                        onToken(content);
                    }
                }
            }

            onDone();
        }).catch(err => {
            if (err.name !== 'AbortError') {
                onError(err);
            }
        });

        return controller; // Return controller for cancellation
    }
};
