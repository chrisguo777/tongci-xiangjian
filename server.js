const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
// Longer ping window so a phone with a locked screen is not marked offline instantly.
const io = new Server(server, {
  cors: { origin: false },
  pingInterval: 20000,
  pingTimeout: 25000
});

const rooms = new Map();
const MAX_ROOMS = 2000;
const TTL = 12 * 60 * 60 * 1000;

app.use(express.static(path.join(__dirname, 'public'), {
  // index.html must never be cached, or a deploy leaves old clients talking to a new server.
  setHeaders(res, filePath) {
    if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache');
  }
}));
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

function code() { let s; do { s = crypto.randomBytes(4).toString('hex').slice(0, 6).toUpperCase(); } while (rooms.has(s)); return s; }
function cleanName(raw) { return String(raw || '').trim().slice(0, 16) || '玩家'; }
function cleanWord(raw) { return String(raw || '').normalize('NFC').trim().replace(/\s+/g, ' ').slice(0, 30); }
function same(a, b) { return a.normalize('NFKC').toLocaleLowerCase() === b.normalize('NFKC').toLocaleLowerCase(); }

function roomState(room) {
  return {
    code: room.code,
    round: room.round,
    phase: room.phase,
    players: room.players.map(p => ({ id: p.id, name: p.name, connected: !!p.socketId, submitted: !!room.submissions[p.id] })),
    history: room.history,
    winner: room.winner
  };
}
function emitRoom(room) { room.updated = Date.now(); io.to(room.code).emit('state', roomState(room)); }
function reply(cb, payload) { if (typeof cb === 'function') cb(payload); }
function findPlayer(room, token) { return room.players.find(p => p.token === token); }
// The acting player must own this socket; stops stale sockets from mutating a room.
function actor(socket) {
  const room = rooms.get(socket.data && socket.data.code);
  if (!room) return {};
  const player = room.players.find(p => p.id === socket.data.id && p.socketId === socket.id);
  return player ? { room, player } : { room };
}
function phaseAfterPopulationChange(room) {
  if (room.phase === 'playing' && room.players.length < 2) return 'waiting';
  if (room.phase === 'waiting' && room.players.length === 2) return 'playing';
  return room.phase;
}

io.on('connection', socket => {
  socket.on('create', ({ name } = {}, cb) => {
    if (rooms.size >= MAX_ROOMS) return reply(cb, { ok: false, error: '服务器房间已满，请稍后重试' });
    const id = crypto.randomUUID(), token = crypto.randomBytes(24).toString('hex');
    const room = {
      code: code(),
      players: [{ id, token, name: cleanName(name), socketId: socket.id }],
      round: 1, phase: 'waiting', submissions: {}, history: [], winner: false, updated: Date.now()
    };
    rooms.set(room.code, room);
    socket.join(room.code);
    socket.data = { code: room.code, id };
    reply(cb, { ok: true, code: room.code, token, id });
    emitRoom(room);
  });

  socket.on('join', ({ code: rawCode, name, token } = {}, cb) => {
    const room = rooms.get(String(rawCode || '').trim().toUpperCase());
    if (!room) return reply(cb, { ok: false, error: '房间不存在或已过期' });
    let player = token && findPlayer(room, token);
    if (!player) {
      if (room.players.length >= 2) return reply(cb, { ok: false, error: '房间已有两名玩家' });
      const id = crypto.randomUUID();
      token = crypto.randomBytes(24).toString('hex');
      player = { id, token, name: cleanName(name), socketId: null };
      room.players.push(player);
    } else if (name) {
      player.name = cleanName(name);
    }
    // One live socket per player: a second tab takes over and the first is told why.
    if (player.socketId && player.socketId !== socket.id) {
      const previous = io.sockets.sockets.get(player.socketId);
      if (previous) { previous.leave(room.code); previous.emit('session-replaced'); previous.data = {}; }
    }
    player.socketId = socket.id;
    socket.join(room.code);
    socket.data = { code: room.code, id: player.id };
    room.phase = phaseAfterPopulationChange(room);
    reply(cb, { ok: true, code: room.code, token: player.token, id: player.id, name: player.name });
    emitRoom(room);
  });

  socket.on('submit', ({ word, round } = {}, cb) => {
    const { room, player } = actor(socket);
    if (!room) return reply(cb, { ok: false, error: '请重新进入房间' });
    if (!player) return reply(cb, { ok: false, error: '请重新进入房间' });
    if (room.phase !== 'playing') return reply(cb, { ok: false, error: '本轮暂时无法提交' });
    // A submit queued before a round change must not land in the new round.
    if (round !== undefined && Number(round) !== room.round) return reply(cb, { ok: false, error: '轮次已更新，请重新输入' });
    const value = cleanWord(word);
    if (!value) return reply(cb, { ok: false, error: '请输入一个词' });
    if (room.submissions[player.id]) return reply(cb, { ok: true, already: true });
    room.submissions[player.id] = value;
    reply(cb, { ok: true });
    if (room.players.length === 2 && room.players.every(p => room.submissions[p.id])) {
      const words = room.players.map(p => room.submissions[p.id]);
      const match = same(words[0], words[1]);
      room.history.push({ round: room.round, words, match });
      room.winner = match;
      room.phase = match ? 'finished' : 'reveal';
    }
    emitRoom(room);
  });

  socket.on('next', ({ round } = {}, cb) => {
    const { room, player } = actor(socket);
    if (!room || !player) return reply(cb, { ok: false, error: '请重新进入房间' });
    // Idempotent: whoever clicks second (or double-clicks) gets ok, not an error.
    if (room.phase === 'playing' && (round === undefined || room.round > Number(round))) return reply(cb, { ok: true });
    if (room.phase !== 'reveal') return reply(cb, { ok: false, error: '现在无法进入下一轮' });
    room.round++;
    room.submissions = {};
    room.phase = 'playing';
    reply(cb, { ok: true });
    emitRoom(room);
  });

  socket.on('restart', (_arg, cb) => {
    const { room, player } = actor(socket);
    if (!room || !player) return reply(cb, { ok: false, error: '请重新进入房间' });
    room.round = 1;
    room.submissions = {};
    room.history = [];
    room.winner = false;
    room.phase = room.players.length === 2 ? 'playing' : 'waiting';
    reply(cb, { ok: true });
    emitRoom(room);
  });

  // Explicitly give up the slot so the other person can invite somebody else.
  socket.on('leave', (_arg, cb) => {
    const { room, player } = actor(socket);
    if (!room || !player) return reply(cb, { ok: true });
    room.players = room.players.filter(p => p.id !== player.id);
    delete room.submissions[player.id];
    socket.leave(room.code);
    socket.data = {};
    if (!room.players.length) { rooms.delete(room.code); return reply(cb, { ok: true }); }
    room.phase = phaseAfterPopulationChange(room);
    reply(cb, { ok: true });
    emitRoom(room);
  });

  socket.on('disconnect', () => {
    const room = rooms.get(socket.data && socket.data.code);
    if (!room) return;
    const p = room.players.find(p => p.id === socket.data.id);
    // Only clear if this exact socket still owns the player: a refresh reconnects first.
    if (p && p.socketId === socket.id) { p.socketId = null; emitRoom(room); }
  });
});

setInterval(() => { const now = Date.now(); for (const [id, room] of rooms) { if (now - room.updated > TTL) rooms.delete(id); } }, 15 * 60 * 1000).unref();

const port = process.env.PORT || 3000;
if (require.main === module) {
  server.listen(port, () => console.log('Tongci game ready on ' + port));
}

module.exports = { app, server, io, rooms };
