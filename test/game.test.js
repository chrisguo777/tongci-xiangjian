const test = require('node:test');
const assert = require('node:assert');
const { io } = require('socket.io-client');
const { server, rooms } = require('../server');

let url;

test.before(async () => {
  await new Promise(r => server.listen(0, r));
  url = 'http://127.0.0.1:' + server.address().port;
});
test.after(() => server.close());

const clients = [];
function client() {
  const s = io(url, { transports: ['websocket'], forceNew: true });
  s.on('state', st => { s.latest = st; });   // 缓存最近一次状态，避免测试里错过已广播的事件
  clients.push(s);
  return s;
}
function ask(sock, event, payload) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout on ' + event)), 4000);
    sock.emit(event, payload, res => { clearTimeout(t); resolve(res); });
  });
}
// Waits for a state matching a predicate; resolves at once if the last one already matches.
function waitState(sock, predicate) {
  if (sock.latest && predicate(sock.latest)) return Promise.resolve(sock.latest);
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout waiting for state')), 4000);
    const on = s => { if (predicate(s)) { clearTimeout(t); sock.off('state', on); resolve(s); } };
    sock.on('state', on);
  });
}
test.after(() => clients.forEach(c => c.close()));

async function pair(nameA = '小A', nameB = '小B') {
  const a = client(), b = client();
  const ra = await ask(a, 'create', { name: nameA });
  assert.ok(ra.ok && /^[0-9A-F]{6}$/.test(ra.code), 'room code created');
  const rb = await ask(b, 'join', { code: ra.code, name: nameB });
  assert.ok(rb.ok, 'second player joined');
  const st = await waitState(a, s => s.players.length === 2);
  assert.equal(st.phase, 'playing');
  return { a, b, ra, rb };
}

test('两人加入后进入 playing，房间码不区分大小写', async () => {
  const a = client(), b = client();
  const ra = await ask(a, 'create', { name: 'A' });
  const rb = await ask(b, 'join', { code: ra.code.toLowerCase(), name: 'B' });
  assert.ok(rb.ok);
});

test('只有双方提交后才揭晓，提前不泄露对方词', async () => {
  const { a, b } = await pair();
  const after = waitState(b, s => s.players.some(p => p.submitted));
  assert.ok((await ask(a, 'submit', { word: '苹果' })).ok);
  const seen = await after;
  assert.equal(seen.phase, 'playing', '单方提交后仍在 playing');
  assert.equal(seen.history.length, 0, '未提交完不产生历史，看不到对方的词');
  assert.equal(JSON.stringify(seen).includes('苹果'), false, 'state 中不含对方答案');

  const revealed = waitState(a, s => s.phase === 'reveal');
  await ask(b, 'submit', { word: '香蕉' });
  const st = await revealed;
  assert.deepEqual(st.history[0].words.sort(), ['苹果', '香蕉'].sort());
  assert.equal(st.history[0].match, false);
});

test('同词即结束并带上轮数', async () => {
  const { a, b } = await pair();
  await ask(a, 'submit', { word: '海' });
  const fin = waitState(a, s => s.phase === 'finished');
  await ask(b, 'submit', { word: '海' });
  const st = await fin;
  assert.equal(st.winner, true);
  assert.equal(st.round, 1);
  assert.equal(st.history[0].match, true);
});

test('大小写与首尾空白忽略后算同词', async () => {
  const { a, b } = await pair();
  await ask(a, 'submit', { word: '  Cat ' });
  const fin = waitState(a, s => s.phase === 'finished');
  await ask(b, 'submit', { word: 'cat' });
  assert.equal((await fin).winner, true);
});

test('重复提交被拒绝，且不会覆盖已提交的词', async () => {
  const { a, b } = await pair();
  await ask(a, 'submit', { word: '第一个' });
  const second = await ask(a, 'submit', { word: '第二个' });
  assert.ok(second.already, '重复提交返回 already');
  const revealed = waitState(a, s => s.phase === 'reveal');
  await ask(b, 'submit', { word: '别的' });
  const st = await revealed;
  assert.ok(st.history[0].words.includes('第一个'));
  assert.ok(!st.history[0].words.includes('第二个'));
});

test('空词和纯空白被拒绝', async () => {
  const { a } = await pair();
  assert.equal((await ask(a, 'submit', { word: '   ' })).ok, false);
  assert.equal((await ask(a, 'submit', {})).ok, false);
});

test('两人同时提交也只产生一条历史', async () => {
  const { a, b } = await pair();
  const revealed = waitState(a, s => s.phase === 'reveal');
  await Promise.all([ask(a, 'submit', { word: '左' }), ask(b, 'submit', { word: '右' })]);
  const st = await revealed;
  assert.equal(st.history.length, 1);
});

test('next 幂等：双方都点也只进一轮', async () => {
  const { a, b } = await pair();
  const revealed = waitState(a, s => s.phase === 'reveal');
  await Promise.all([ask(a, 'submit', { word: '山' }), ask(b, 'submit', { word: '水' })]);
  const rev = await revealed;
  const r1 = await ask(a, 'next', { round: rev.round });
  const r2 = await ask(b, 'next', { round: rev.round });
  const r3 = await ask(b, 'next', { round: rev.round });
  assert.ok(r1.ok && r2.ok && r3.ok, '重复点击不报错');
  const st = await waitState(a, s => s.phase === 'playing');
  assert.equal(st.round, 2, '只前进一轮');
  assert.equal(Object.keys(st.history).length, 1);
});

test('轮次过期的提交被拒绝（抢在换轮前点提交）', async () => {
  const { a, b } = await pair();
  const revealed = waitState(a, s => s.phase === 'reveal');
  await Promise.all([ask(a, 'submit', { word: '天' }), ask(b, 'submit', { word: '地' })]);
  await revealed;
  await ask(a, 'next', { round: 1 });
  await waitState(a, s => s.round === 2);
  const stale = await ask(a, 'submit', { word: '旧的', round: 1 });
  assert.equal(stale.ok, false, '旧轮次的提交被拒绝');
});

test('房间最多两人，第三人被拒绝', async () => {
  const { ra } = await pair();
  const c = client();
  const res = await ask(c, 'join', { code: ra.code, name: 'C' });
  assert.equal(res.ok, false);
  assert.match(res.error, /两名玩家/);
});

test('不存在的房间码被拒绝', async () => {
  const c = client();
  assert.equal((await ask(c, 'join', { code: 'ZZZZZZ' })).ok, false);
});

test('刷新/重连：带 token 回来保留座位、轮次和历史', async () => {
  const { a, b, ra, rb } = await pair('甲', '乙');
  await ask(a, 'submit', { word: '猫' });
  const revealed = waitState(a, s => s.phase === 'reveal');
  await ask(b, 'submit', { word: '狗' });
  await revealed;
  await ask(a, 'next', { round: 1 });
  await waitState(a, s => s.round === 2);

  b.close();                                   // 模拟断线
  await waitState(a, s => s.players.some(p => !p.connected));

  const b2 = client();                         // 模拟刷新后带 token 回来
  const back = await ask(b2, 'join', { code: ra.code, token: rb.token, name: '乙' });
  assert.ok(back.ok, '带 token 可以回到房间');
  assert.equal(back.id, rb.id, '还是同一个玩家，不占新座位');
  const st = await waitState(b2, s => s.players.every(p => p.connected));
  assert.equal(st.players.length, 2, '没有变成三个人');
  assert.equal(st.round, 2, '轮次保持一致');
  assert.equal(st.history.length, 1, '历史保留');
  assert.equal(st.phase, 'playing');
});

test('断线期间房间不被第三人占位，但回来后可继续游玩', async () => {
  const { a, b, ra, rb } = await pair();
  b.close();
  await waitState(a, s => s.players.some(p => !p.connected));
  const c = client();
  assert.equal((await ask(c, 'join', { code: ra.code, name: '陌生人' })).ok, false, '断线的座位不被抢占');
  const b2 = client();
  await ask(b2, 'join', { code: ra.code, token: rb.token });
  const fin = waitState(a, s => s.phase === 'finished');
  await ask(a, 'submit', { word: '同' });
  await ask(b2, 'submit', { word: '同' });
  assert.equal((await fin).winner, true);
});

test('同一玩家开第二个标签页会顶替前一个', async () => {
  const { a, ra, rb, b } = await pair();
  const replaced = new Promise(r => b.once('session-replaced', r));
  const b2 = client();
  assert.ok((await ask(b2, 'join', { code: ra.code, token: rb.token })).ok);
  await replaced;                              // 旧标签页收到通知
  const st = await waitState(a, s => s.players.length === 2);
  assert.equal(st.players.length, 2, '不会多出玩家');
});

test('重新开始会清空历史、回到第一轮', async () => {
  const { a, b } = await pair();
  await ask(a, 'submit', { word: '同' });
  const fin = waitState(a, s => s.phase === 'finished');
  await ask(b, 'submit', { word: '同' });
  await fin;
  const restarted = waitState(b, s => s.phase === 'playing');
  assert.ok((await ask(a, 'restart')).ok);
  const st = await restarted;
  assert.equal(st.round, 1);
  assert.equal(st.history.length, 0);
  assert.equal(st.winner, false);
  assert.ok(st.players.every(p => !p.submitted), '提交状态已清空');
});

test('退出房间后座位空出，新玩家可以加入', async () => {
  const { a, b, ra } = await pair();
  assert.ok((await ask(b, 'leave')).ok);
  const st = await waitState(a, s => s.players.length === 1);
  assert.equal(st.phase, 'waiting');
  const c = client();
  assert.ok((await ask(c, 'join', { code: ra.code, name: '新人' })).ok);
  assert.equal((await waitState(a, s => s.players.length === 2)).phase, 'playing');
});

test('未加入房间的 socket 不能操作', async () => {
  const c = client();
  await new Promise(r => c.on('connect', r));
  assert.equal((await ask(c, 'submit', { word: 'x' })).ok, false);
  assert.equal((await ask(c, 'next', {})).ok, false);
  assert.equal((await ask(c, 'restart')).ok, false);
});

test('等待对方时不能提交（phase=waiting）', async () => {
  const a = client();
  const ra = await ask(a, 'create', { name: '独自' });
  assert.equal((await ask(a, 'submit', { word: '词' })).ok, false);
  assert.ok(rooms.has(ra.code));
});

test('昵称与词长度被截断', async () => {
  const a = client(), b = client();
  const ra = await ask(a, 'create', { name: 'x'.repeat(50) });
  await ask(b, 'join', { code: ra.code, name: 'B' });
  const st = await waitState(a, s => s.players.length === 2);
  assert.equal(st.players[0].name.length, 16);
  await ask(a, 'submit', { word: '词'.repeat(60) });
  const revealed = waitState(a, s => s.phase === 'reveal');
  await ask(b, 'submit', { word: '别的' });
  assert.equal((await revealed).history[0].words[0].length, 30);
});

test('/health 可用', async () => {
  const res = await fetch(url + '/health');
  assert.equal(res.status, 200);
  assert.equal((await res.json()).ok, true);
});

test('首页可访问且不缓存', async () => {
  const res = await fetch(url + '/');
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('cache-control'), 'no-cache');
  assert.match(await res.text(), /同词/);
});
