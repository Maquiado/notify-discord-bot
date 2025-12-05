const { db } = require('./firebase');
const { Client, GatewayIntentBits, Partials } = require('discord.js');

const token = process.env.DISCORD_BOT_TOKEN || '';
const channelId = process.env.DISCORD_CHANNEL_ID || '';

const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.DirectMessages], partials: [Partials.Channel] });

const cache = new Map();

function setCache(uid, data) {
  cache.set(uid, { data, ts: Date.now() });
}

function getCache(uid) {
  const it = cache.get(uid);
  if (!it) return null;
  if (Date.now() - it.ts > 5 * 60 * 1000) { cache.delete(uid); return null; }
  return it.data;
}

async function fetchDiscordAndPrefs(uids) {
  const res = {};
  const missing = [];
  uids.forEach((uid) => { const c = getCache(uid); if (c) res[uid] = c; else missing.push(uid); });
  if (missing.length) {
    const mm = missing.slice(0, 10);
    const discSnap = await db.collection('discord').where('uid', 'in', mm).select('uid', 'discordUserId', 'discordUsername').get();
    const notifSnap = await db.collection('notificacoes').where('uid', 'in', mm).select('uid', 'notifyReadyCheck', 'notifyResult').get();
    const discMap = {}; discSnap.forEach((d) => discMap[d.get('uid')] = { discordUserId: d.get('discordUserId') || null, discordUsername: d.get('discordUsername') || null });
    const notifMap = {}; notifSnap.forEach((n) => notifMap[n.get('uid')] = { notifyReadyCheck: !!n.get('notifyReadyCheck'), notifyResult: !!n.get('notifyResult') });
    mm.forEach((uid) => { const v = Object.assign({}, discMap[uid] || {}, notifMap[uid] || {}); res[uid] = v; setCache(uid, v); });
  }
  return res;
}

async function dmOrChannel(uid, prefs, content) {
  const dId = prefs.discordUserId || '';
  if (dId) {
    try { const user = await client.users.fetch(dId); if (user) { await user.send(content); return; } } catch (_) {}
  }
  if (!channelId) return;
  try { const ch = await client.channels.fetch(channelId); if (ch && ch.isTextBased()) await ch.send(content); } catch (_) {}
}

function formatReadyMessage(data) {
  const lista = (Array.isArray(data.jogadores) ? data.jogadores : []).map((p) => `• ${p.nome || p.uid || 'Jogador'} (${p.rolePrincipal || 'Preencher'})`).join('\n');
  return `Ready Check aberto!\n${lista}`;
}

function formatResultMessage(partida) {
  const v = partida.vencedor || 'N/A';
  const t1 = (partida.time1 && Array.isArray(partida.time1.jogadores)) ? partida.time1.jogadores : [];
  const t2 = (partida.time2 && Array.isArray(partida.time2.jogadores)) ? partida.time2.jogadores : [];
  const n = (arr) => arr.map((j) => j.nome || j.uid || '').filter(Boolean).join(', ');
  return `Resultado disponível: vencedor ${v}\nTime 1: ${n(t1)}\nTime 2: ${n(t2)}`;
}

function startReadyNotify() {
  db.collection('aguardandoPartidas').where('status', '==', 'pending').onSnapshot(async (snap) => {
    snap.docChanges().forEach(async (chg) => {
      if (chg.type !== 'added') return;
      const data = chg.doc.data();
      const uids = Array.isArray(data.uids) ? data.uids : [];
      const prefsMap = await fetchDiscordAndPrefs(uids);
      const msg = formatReadyMessage(data);
      await Promise.all(uids.map((uid) => { const prefs = prefsMap[uid] || {}; if (!prefs.notifyReadyCheck) return Promise.resolve(); return dmOrChannel(uid, prefs, msg); }));
    });
  }, (err) => { console.error('ready notify listener error', err); });
}

function startResultNotify() {
  db.collection('Historico').where('vencedor', '!=', 'N/A').orderBy('vencedor').onSnapshot(async (snap) => {
    snap.docChanges().forEach(async (chg) => {
      if (chg.type !== 'added' && chg.type !== 'modified') return;
      const partida = chg.doc.data();
      const uids = Array.isArray(partida.uids) ? partida.uids : [];
      const prefsMap = await fetchDiscordAndPrefs(uids);
      const msg = formatResultMessage(partida);
      await Promise.all(uids.map((uid) => { const prefs = prefsMap[uid] || {}; if (!prefs.notifyResult) return Promise.resolve(); return dmOrChannel(uid, prefs, msg); }));
    });
  }, (err) => { console.error('result notify listener error', err); });
}

client.once('ready', () => { startReadyNotify(); startResultNotify(); });
client.login(token).catch((e) => console.error('discord login error', e));
