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
const { randomUUID } = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  allowEIO3: true,
});

app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '50mb' }));

// Static files
const publicDir = path.join(__dirname, 'public');
if (fs.existsSync(publicDir)) app.use(express.static(publicDir));
else app.use(express.static(__dirname));

app.get('/', (req, res) => {
  const candidates = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')];
  const found = candidates.find(f => fs.existsSync(f));
  if (found) return res.sendFile(found);
  res.send('<h2>index.html not found</h2>');
});

// Multer
const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) cb(null, true);
    else cb(new Error('Only CSV files are allowed'));
  },
  limits: { fileSize: 10 * 1024 * 1024 },
});

// Multi-session state: sessionId -> { client, status, qr, blastRunning, socketIds }
const sessions = new Map();

function getSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { client: null, status: 'disconnected', qr: null, blastRunning: false, socketIds: new Set() });
  }
  return sessions.get(sessionId);
}

function emitToSession(sessionId, event, data) {
  io.to('session:' + sessionId).emit(event, data);
}

function getSessionId(req) {
  return req.headers['x-session-id'] || req.query.sessionId;
}

function initWhatsApp(sessionId) {
  const sess = getSession(sessionId);
  if (sess.client) { try { sess.client.destroy(); } catch (_) {} sess.client = null; }

  sess.status = 'initializing';
  emitToSession(sessionId, 'wa_status', { status: 'initializing' });

  const client = new Client({
    authStrategy: new LocalAuth({ clientId: sessionId, dataPath: './wa_sessions' }),
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--no-first-run', '--no-zygote', '--single-process'],
    },
  });

  sess.client = client;

  client.on('qr', async (qr) => {
    sess.status = 'qr';
    sess.qr = await qrcode.toDataURL(qr);
    emitToSession(sessionId, 'wa_qr', { qr: sess.qr });
    emitToSession(sessionId, 'wa_status', { status: 'qr' });
  });

  client.on('authenticated', () => {
    sess.status = 'authenticated'; sess.qr = null;
    emitToSession(sessionId, 'wa_status', { status: 'authenticated' });
  });

  client.on('ready', () => {
    sess.status = 'ready'; sess.qr = null;
    emitToSession(sessionId, 'wa_status', { status: 'ready' });
    console.log('[' + sessionId.slice(0,8) + '] WhatsApp Ready');
  });

  client.on('auth_failure', (msg) => {
    sess.status = 'auth_failure';
    emitToSession(sessionId, 'wa_status', { status: 'auth_failure', message: msg });
  });

  client.on('disconnected', (reason) => {
    sess.status = 'disconnected'; sess.client = null;
    emitToSession(sessionId, 'wa_status', { status: 'disconnected', reason });
  });

  client.initialize().catch((err) => {
    sess.status = 'error';
    emitToSession(sessionId, 'wa_status', { status: 'error', message: err.message });
    console.error('[' + sessionId.slice(0,8) + '] Init error:', err.message);
  });
}

// API Routes
app.post('/api/new-session', (req, res) => {
  const sessionId = randomUUID();
  getSession(sessionId);
  console.log('[New session] ' + sessionId.slice(0,8));
  res.json({ ok: true, sessionId });
});

app.get('/api/status', (req, res) => {
  const sessionId = getSessionId(req);
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });
  const sess = getSession(sessionId);
  res.json({ status: sess.status, qr: sess.qr, blastRunning: sess.blastRunning });
});

app.post('/api/connect', (req, res) => {
  const sessionId = getSessionId(req);
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });
  const sess = getSession(sessionId);
  if (sess.status === 'ready') return res.json({ ok: true, status: 'ready' });
  initWhatsApp(sessionId);
  res.json({ ok: true, status: 'initializing' });
});

app.post('/api/disconnect', async (req, res) => {
  const sessionId = getSessionId(req);
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });
  const sess = getSession(sessionId);
  if (sess.client) {
    try { await sess.client.logout(); } catch (_) {}
    try { await sess.client.destroy(); } catch (_) {}
    sess.client = null;
  }
  try { fs.rmSync(path.join('./wa_sessions', sessionId), { recursive: true, force: true }); } catch (_) {}
  sess.status = 'disconnected'; sess.qr = null;
  emitToSession(sessionId, 'wa_status', { status: 'disconnected' });
  res.json({ ok: true });
});

app.post('/api/parse-csv', upload.single('csv'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No CSV file uploaded' });
  const results = [];
  const { PassThrough } = require('stream');
  const s = new PassThrough();
  s.end(req.file.buffer);
  s.pipe(parse({ columns: true, skip_empty_lines: true, trim: true }))
    .on('data', row => results.push(row))
    .on('error', err => res.status(400).json({ error: 'CSV parse error: ' + err.message }))
    .on('end', () => {
      if (!results.length) return res.status(400).json({ error: 'CSV is empty' });
      res.json({ ok: true, rows: results, columns: Object.keys(results[0]), count: results.length });
    });
});

app.post('/api/send-blast', async (req, res) => {
  const sessionId = getSessionId(req);
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });
  const sess = getSession(sessionId);
  if (sess.status !== 'ready') return res.status(400).json({ error: 'WhatsApp is not connected.' });
  if (sess.blastRunning) return res.status(400).json({ error: 'Blast already running' });

  const { rows, template, phoneColumn, delayMs = 2000 } = req.body;
  if (!rows || !template || !phoneColumn) return res.status(400).json({ error: 'Missing required fields' });

  sess.blastRunning = true;
  res.json({ ok: true, total: rows.length });

  (async () => {
    const results = [];
    emitToSession(sessionId, 'blast_start', { total: rows.length });

    for (let i = 0; i < rows.length; i++) {
      if (!sess.blastRunning) break;
      const row = rows[i];
      const phone = (row[phoneColumn] || '').replace(/\D/g, '');

      if (!phone) {
        results.push({ row: i+1, phone: 'N/A', status: 'skipped', reason: 'Empty phone' });
        emitToSession(sessionId, 'blast_progress', { index: i, total: rows.length, phone: 'N/A', status: 'skipped', results });
        continue;
      }

      let message = template;
      Object.keys(row).forEach(k => { message = message.replace(new RegExp('{{' + k + '}}', 'g'), row[k] || ''); });

      try {
        await sess.client.sendMessage(phone + '@c.us', message);
        results.push({ row: i+1, phone, status: 'sent' });
        emitToSession(sessionId, 'blast_progress', { index: i, total: rows.length, phone, status: 'sent', results });
      } catch (err) {
        results.push({ row: i+1, phone, status: 'failed', reason: err.message });
        emitToSession(sessionId, 'blast_progress', { index: i, total: rows.length, phone, status: 'failed', reason: err.message, results });
      }

      if (i < rows.length - 1) await new Promise(r => setTimeout(r, delayMs));
    }

    sess.blastRunning = false;
    const sent = results.filter(r => r.status === 'sent').length;
    const failed = results.filter(r => r.status === 'failed').length;
    const skipped = results.filter(r => r.status === 'skipped').length;
    emitToSession(sessionId, 'blast_done', { total: rows.length, sent, failed, skipped, results });
  })();
});

app.post('/api/cancel-blast', (req, res) => {
  const sessionId = getSessionId(req);
  if (!sessionId) return res.status(400).json({ error: 'Missing sessionId' });
  const sess = getSession(sessionId);
  sess.blastRunning = false;
  res.json({ ok: true });
});

// Cleanup stale sessions hourly
setInterval(() => {
  for (const [id, sess] of sessions.entries()) {
    if (sess.socketIds.size === 0 && sess.status === 'disconnected') {
      sessions.delete(id);
    }
  }
}, 60 * 60 * 1000);

// Socket.IO
io.on('connection', (socket) => {
  let mySessionId = null;

  socket.on('join_session', (sessionId) => {
    if (!sessionId) return;
    mySessionId = sessionId;
    socket.join('session:' + sessionId);
    const sess = getSession(sessionId);
    sess.socketIds.add(socket.id);
    socket.emit('wa_status', { status: sess.status });
    if (sess.qr) socket.emit('wa_qr', { qr: sess.qr });
  });

  socket.on('disconnect', () => {
    if (mySessionId) {
      const sess = sessions.get(mySessionId);
      if (sess) sess.socketIds.delete(socket.id);
    }
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log('\n🚀 WA Blast running at http://localhost:' + PORT);
  console.log('👥 Multi-session: each browser gets its own WhatsApp\n');
});
