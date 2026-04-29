const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const { parse } = require('csv-parse');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  allowEIO3: true,
});

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "50mb" }));

// Serve static files — check both ./public and ./ (in case index.html is in same dir)
const publicDir = path.join(__dirname, 'public');
const rootDir = __dirname;
if (fs.existsSync(publicDir)) {
  app.use(express.static(publicDir));
} else {
  app.use(express.static(rootDir));
}

// Explicit fallback for GET / so "Cannot GET /" never appears
app.get('/', (req, res) => {
  const candidates = [
    path.join(__dirname, 'public', 'index.html'),
    path.join(__dirname, 'index.html'),
  ];
  const found = candidates.find(f => fs.existsSync(f));
  if (found) return res.sendFile(found);
  res.send(`
    <h2 style="font-family:monospace;padding:20px">⚠️ index.html not found</h2>
    <p style="font-family:monospace;padding:0 20px">
      Make sure <b>index.html</b> is inside a <b>public/</b> folder next to server.js<br><br>
      Expected: <code>${candidates[0]}</code>
    </p>
  `);
});

// Multer for CSV uploads (memory storage)
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Only CSV files are allowed'));
    }
  },
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB max
});

// ─── WhatsApp Client State ────────────────────────────────────────────────────
let waClient = null;
let waStatus = 'disconnected'; // disconnected | qr | ready | auth_failure
let currentQR = null;
let blastRunning = false;

function initWhatsApp(socketId) {
  if (waClient) {
    try { waClient.destroy(); } catch (_) {}
  }

  waStatus = 'initializing';
  io.emit('wa_status', { status: waStatus });

  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: './wa_session' }),
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
      ],
    },
  });

  waClient.on('qr', async (qr) => {
    waStatus = 'qr';
    currentQR = await qrcode.toDataURL(qr);
    io.emit('wa_qr', { qr: currentQR });
    io.emit('wa_status', { status: waStatus });
    console.log('[WhatsApp] QR code generated');
  });

  waClient.on('authenticated', () => {
    waStatus = 'authenticated';
    currentQR = null;
    io.emit('wa_status', { status: waStatus });
    console.log('[WhatsApp] Authenticated!');
  });

  waClient.on('ready', () => {
    waStatus = 'ready';
    io.emit('wa_status', { status: waStatus });
    console.log('[WhatsApp] Client ready!');
  });

  waClient.on('auth_failure', (msg) => {
    waStatus = 'auth_failure';
    io.emit('wa_status', { status: waStatus, message: msg });
    console.error('[WhatsApp] Auth failure:', msg);
  });

  waClient.on('disconnected', (reason) => {
    waStatus = 'disconnected';
    io.emit('wa_status', { status: waStatus, reason });
    console.log('[WhatsApp] Disconnected:', reason);
    waClient = null;
  });

  waClient.initialize().catch((err) => {
    waStatus = 'error';
    io.emit('wa_status', { status: 'error', message: err.message });
    console.error('[WhatsApp] Init error:', err.message);
  });
}

// ─── REST API ─────────────────────────────────────────────────────────────────

// Status
app.get('/api/status', (req, res) => {
  res.json({ status: waStatus, qr: currentQR, blastRunning });
});

// Connect WhatsApp
app.post('/api/connect', (req, res) => {
  if (waStatus === 'ready') {
    return res.json({ ok: true, status: 'ready', message: 'Already connected' });
  }
  initWhatsApp();
  res.json({ ok: true, status: waStatus, message: 'Initializing WhatsApp...' });
});

// Disconnect
app.post('/api/disconnect', async (req, res) => {
  if (waClient) {
    try {
      await waClient.logout();
      await waClient.destroy();
    } catch (_) {}
    waClient = null;
  }
  // Remove session
  try { fs.rmSync('./wa_session', { recursive: true, force: true }); } catch (_) {}
  waStatus = 'disconnected';
  io.emit('wa_status', { status: waStatus });
  res.json({ ok: true });
});

// Upload & Parse CSV
app.post('/api/parse-csv', upload.single('csv'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No CSV file uploaded' });

  const results = [];
  const stream = require('stream');
  const bufferStream = new stream.PassThrough();
  bufferStream.end(req.file.buffer);

  bufferStream
    .pipe(parse({ columns: true, skip_empty_lines: true, trim: true }))
    .on('data', (row) => results.push(row))
    .on('error', (err) => res.status(400).json({ error: 'CSV parse error: ' + err.message }))
    .on('end', () => {
      if (results.length === 0) return res.status(400).json({ error: 'CSV is empty' });
      const columns = Object.keys(results[0]);
      res.json({ ok: true, rows: results, columns, count: results.length });
    });
});

// Send Blast
app.post('/api/send-blast', async (req, res) => {
  if (waStatus !== 'ready') {
    return res.status(400).json({ error: 'WhatsApp is not connected. Please scan the QR code first.' });
  }
  if (blastRunning) {
    return res.status(400).json({ error: 'A blast is already running' });
  }

  const { rows, template, phoneColumn, delayMs = 2000 } = req.body;

  if (!rows || !template || !phoneColumn) {
    return res.status(400).json({ error: 'Missing rows, template, or phoneColumn' });
  }

  blastRunning = true;
  res.json({ ok: true, total: rows.length, message: 'Blast started' });

  // Run blast asynchronously
  (async () => {
    const results = [];
    io.emit('blast_start', { total: rows.length });

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      let phone = (row[phoneColumn] || '').replace(/\D/g, '');

      // Normalize phone number
      if (!phone) {
        results.push({ row: i + 1, phone: 'N/A', status: 'skipped', reason: 'Empty phone' });
        io.emit('blast_progress', { index: i, total: rows.length, phone: 'N/A', status: 'skipped', results });
        continue;
      }

      // Build personalized message
      let message = template;
      Object.keys(row).forEach((key) => {
        message = message.replace(new RegExp(`{{${key}}}`, 'g'), row[key] || '');
      });

      try {
        // Format: countrycode+number@c.us (assume intl format)
        const chatId = `${phone}@c.us`;
        await waClient.sendMessage(chatId, message);
        results.push({ row: i + 1, phone, status: 'sent' });
        io.emit('blast_progress', { index: i, total: rows.length, phone, status: 'sent', results });
        console.log(`[Blast] ✓ Sent to ${phone}`);
      } catch (err) {
        const reason = err.message || 'Unknown error';
        results.push({ row: i + 1, phone, status: 'failed', reason });
        io.emit('blast_progress', { index: i, total: rows.length, phone, status: 'failed', reason, results });
        console.error(`[Blast] ✗ Failed to ${phone}: ${reason}`);
      }

      // Rate limit delay (avoid WA ban)
      if (i < rows.length - 1) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }

    blastRunning = false;
    const sent = results.filter((r) => r.status === 'sent').length;
    const failed = results.filter((r) => r.status === 'failed').length;
    const skipped = results.filter((r) => r.status === 'skipped').length;
    io.emit('blast_done', { total: rows.length, sent, failed, skipped, results });
    console.log(`[Blast] Done — Sent: ${sent}, Failed: ${failed}, Skipped: ${skipped}`);
  })();
});

// Cancel blast (best-effort)
app.post('/api/cancel-blast', (req, res) => {
  blastRunning = false;
  io.emit('blast_cancelled', {});
  res.json({ ok: true });
});

// ─── Socket.IO ───────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('[Socket] Client connected:', socket.id);
  // Send current state immediately on connect
  socket.emit('wa_status', { status: waStatus });
  if (currentQR) socket.emit('wa_qr', { qr: currentQR });
  socket.on('disconnect', () => console.log('[Socket] Client disconnected:', socket.id));
});

// ─── Start Server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 WhatsApp Blast Server running at http://localhost:${PORT}`);
  console.log('📱 Open the URL above in your browser to get started\n');
});
