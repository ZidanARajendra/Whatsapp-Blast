const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { Client, LocalAuth } = require('whatsapp-web.js');
const QRCode = require('qrcode');

const app = express();
const PORT = 3000;
const upload = multer({ dest: 'uploads/' });

// SSE clients storage
let sseClients = [];

// WhatsApp client with persistent session (scan QR only once)
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-accelerated-2d-canvas',
            '--disable-extensions',
            '--disable-background-timer-throttling',
            '--disable-renderer-backgrounding',
            '--disable-field-trial-config',
            '--no-first-run',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',
            '--aggressive-cache-discard',
            '--max_old_space_size=512'
        ]
    }
});

// Keep track of current state
let qrCodeDataUrl = null;
let isAuthenticated = false;
let isSending = false;

// When QR code is generated, convert to data URL and notify clients
client.on('qr', async (qr) => {
    qrCodeDataUrl = await QRCode.toDataURL(qr);
    isAuthenticated = false;
    notifyAll({ type: 'qr', data: qrCodeDataUrl });
});

// Ready event – authenticated
client.on('ready', () => {
    isAuthenticated = true;
    qrCodeDataUrl = null;
    notifyAll({ type: 'status', message: 'WhatsApp connected and ready!' });
});

// Handle disconnects
client.on('disconnected', (reason) => {
    isAuthenticated = false;
    notifyAll({ type: 'status', message: 'Disconnected: ' + reason });
    client.initialize();
});

// Initialize the WhatsApp client
client.initialize();

// Serve static files
app.use(express.static('public'));
app.use(express.json());

// SSE endpoint
app.get('/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    sseClients.push(res);

    // Send current state on connection
    if (isAuthenticated) {
        res.write(`data: ${JSON.stringify({ type: 'status', message: 'Connected' })}\n\n`);
    } else if (qrCodeDataUrl) {
        res.write(`data: ${JSON.stringify({ type: 'qr', data: qrCodeDataUrl })}\n\n`);
    }

    req.on('close', () => {
        sseClients = sseClients.filter(client => client !== res);
    });
});

// Notify all connected browsers
function notifyAll(data) {
    sseClients.forEach(client => {
        client.write(`data: ${JSON.stringify(data)}\n\n`);
    });
}

// Send status updates during blast
function sendStatus(message, progress = null) {
    notifyAll({ type: 'blast-status', message, progress });
}

// API: check auth status
app.get('/api/status', (req, res) => {
    res.json({ authenticated: isAuthenticated });
});

// API: send blast
app.post('/api/send', upload.single('csv'), async (req, res) => {
    if (isSending) {
        return res.status(400).json({ error: 'A sending process is already running.' });
    }
    if (!req.file) {
        return res.status(400).json({ error: 'CSV file is required.' });
    }
    if (!req.body.message || typeof req.body.message !== 'string') {
        return res.status(400).json({ error: 'Message template is required.' });
    }

    const interval = parseInt(req.body.interval, 10) || 5; // seconds
    const messageTemplate = req.body.message;
    const results = [];
    const csvPath = req.file.path;

    // Parse CSV
    try {
        await new Promise((resolve, reject) => {
            fs.createReadStream(csvPath)
                .pipe(csv())
                .on('data', (row) => results.push(row))
                .on('end', resolve)
                .on('error', reject);
        });
        fs.unlinkSync(csvPath); // clean up upload
    } catch (err) {
        fs.unlinkSync(csvPath);
        return res.status(400).json({ error: 'Invalid CSV file.' });
    }

    if (results.length === 0) {
        return res.status(400).json({ error: 'CSV file is empty.' });
    }

    // Basic validation: must have a column with phone number (case-insensitive)
    const headers = Object.keys(results[0]).map(h => h.toLowerCase());
    const phoneColumn = headers.find(h => h === 'phone' || h === 'number' || h === 'phonenumber' || h === 'mobile');
    if (!phoneColumn) {
        return res.status(400).json({ error: 'CSV must have a column named "phone", "number", "mobile" or similar.' });
    }

    res.json({ message: 'Blast started', total: results.length });
    isSending = true;

    // Process rows
    let sent = 0;
    let failed = 0;
    for (let i = 0; i < results.length; i++) {
        const row = results[i];
        let phone = row[Object.keys(row).find(k => k.toLowerCase() === phoneColumn)]?.toString().trim();
        if (!phone) {
            failed++;
            sendStatus(`Row ${i+1}: missing phone number.`, { sent, failed, total: results.length });
            continue;
        }

        // Format number: remove everything except digits, ensure it starts with country code
        phone = phone.replace(/\D/g, '');   // keep only digits
        if (!phone.startsWith('62') && !phone.startsWith('1') /* etc. */) {
            // If you want to auto-prepend a default country code, do it here
            // But your CSV already has 62, so this is fine as-is.
        }
        const chatId = `${phone}@c.us`;   // do NOT add '+'

        // Personalize message
        let personalized = messageTemplate;
        for (const key of Object.keys(row)) {
            const placeholder = `{{${key}}}`;
            personalized = personalized.replace(new RegExp(placeholder, 'g'), row[key]?.toString() || '');
        }

        // Send message
        try {
            await client.sendMessage(chatId, personalized);
            sent++;
            sendStatus(`Sent to ${phone}`, { sent, failed, total: results.length });
        } catch (err) {
            failed++;
            sendStatus(`Failed for ${phone}: ${err.message}`, { sent, failed, total: results.length });
        }

        // Interval between messages (except after the last one)
        if (i < results.length - 1) {
            await new Promise(resolve => setTimeout(resolve, interval * 1000));
        }
    }

    isSending = false;
    sendStatus(`Blast finished: ${sent} sent, ${failed} failed.`, { sent, failed, total: results.length });
});

app.listen(PORT, () => {
    console.log(`WhatsApp Blast app listening on http://localhost:${PORT}`);
});