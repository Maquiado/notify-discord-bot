import { db } from './firestore.js';

async function getDiscordIdForUid(uid) {
  const snap = await db.collection('discord').doc(String(uid)).get();
  const d = snap.exists ? (snap.data() || {}) : {};
  return d.discordUserId || null;
}

async function getNotifyPrefs(uid) {
  const snap = await db.collection('notificacoes').doc(String(uid)).get();
  const d = snap.exists ? (snap.data() || {}) : {};
  return { notifyReadyCheck: !!d.notifyReadyCheck, notifyResult: !!d.notifyResult, notifyMvp: !!d.notifyMvp };
}

export async function buildMentions(uids, type) {
  const arr = Array.isArray(uids) ? uids : [];
  const ids = await Promise.all(arr.map(async (u) => {
    const prefs = await getNotifyPrefs(u);
    if (type === 'ready' && !prefs.notifyReadyCheck) return null;
    if (type === 'result' && !prefs.notifyResult) return null;
    if (type === 'mvp' && !prefs.notifyMvp) return null;
    const id = await getDiscordIdForUid(u);
    return id || null;
  }));
  const toks = ids.filter(Boolean).map(id => `<@${id}>`);
  return toks.join(' ');
}

export async function notifyReadyCheck(client, channelId, data) {
  const ch = await client.channels.fetch(channelId);
  const uids = Array.isArray(data.uids) ? data.uids : [];
  const mentions = await buildMentions(uids, 'ready');
  const content = mentions ? `${mentions} Ready Check iniciado` : 'Ready Check iniciado';
  await ch.send({ content });
}

export async function notifyResult(client, channelId, historico) {
  const ch = await client.channels.fetch(channelId);
  const uids = Array.isArray(historico.uids) ? historico.uids : [];
  const mentions = await buildMentions(uids, 'result');
  const vencedor = historico.vencedor || 'N/A';
  const content = mentions ? `${mentions} Resultado: ${vencedor}` : `Resultado: ${vencedor}`;
  await ch.send({ content });
}

export async function notifyMvp(client, channelId, historico, mvpUid) {
  const ch = await client.channels.fetch(channelId);
  const mentions = await buildMentions([mvpUid], 'mvp');
  const content = mentions ? `${mentions} MVP da partida` : 'MVP da partida';
  await ch.send({ content });
}
