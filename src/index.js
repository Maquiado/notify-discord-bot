import { Client, GatewayIntentBits, PermissionsBitField } from 'discord.js';
import { db } from './firestore.js';
import { notifyReadyCheck, notifyResult } from './notify.js';

const token = process.env.DISCORD_TOKEN || '';
const channelId = process.env.DISCORD_CHANNEL_ID || '';
const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent] });
const guildId = process.env.DISCORD_GUILD_ID || '';
const notifiedReady = new Set();

async function finalizeMatch(partidaId, vencedorSide) {
  const ref = db.collection('Historico').doc(String(partidaId));
  const snap = await ref.get();
  if (!snap.exists) return false;
  const d = snap.data() || {};
  const teams = d.teams || {};
  const blue = teams.blue || {};
  const red = teams.red || {};
  const slots = ['top', 'jg', 'mid', 'adc', 'sup'];
  const toUids = (obj) => slots.map(s => String(((obj[s] || {}).uid) || '')).filter(u => !!u);
  let blueUids = toUids(blue);
  let redUids = toUids(red);
  if ((!blueUids.length || !redUids.length) && d.time1 && d.time2) {
    const t1 = Array.isArray(d.time1.jogadores) ? d.time1.jogadores : [];
    const t2 = Array.isArray(d.time2.jogadores) ? d.time2.jogadores : [];
    if (!blueUids.length) blueUids = t1.map(j => String(j.uid || '')).filter(Boolean);
    if (!redUids.length) redUids = t2.map(j => String(j.uid || '')).filter(Boolean);
  }
  let side = String(vencedorSide || '').toLowerCase();
  if (side.includes('azul')) side = 'blue';
  if (side.includes('vermelho')) side = 'red';
  const vencedorStr = side === 'blue' ? 'Time Azul' : 'Time Vermelho';
  const vencedores = side === 'blue' ? blueUids : redUids;
  const perdedores = side === 'blue' ? redUids : blueUids;
  const applyForUid = async (uid, won) => {
    const invRef = db.collection('invocador').doc(String(uid));
    const perfRef = db.collection('perfil').doc(String(uid));
    const invSnap = await invRef.get();
    const perfSnap = await perfRef.get();
    const inv = invSnap.exists ? (invSnap.data() || {}) : {};
    const perf = perfSnap.exists ? (perfSnap.data() || {}) : {};
    const xpBase = won ? 70 : 30;
    const lumBase = won ? 40 : 20;
    const curWin = Number(inv.winStreak || 0);
    const curLoss = Number(inv.lossStreak || 0);
    const nextWin = won ? curWin + 1 : 0;
    const nextLoss = won ? 0 : curLoss + 1;
    let factor = 1.0;
    if (won && nextWin >= 3) factor = 1.10;
    if (!won && nextLoss >= 3) factor = 0.95;
    const xpGain = Math.round(xpBase * factor);
    const lumGain = Math.round(lumBase * factor);
    const payloadInv = { xp: Number(inv.xp || 0) + xpGain, vitorias: won ? Number(inv.vitorias || 0) + 1 : Number(inv.vitorias || 0), derrotas: won ? Number(inv.derrotas || 0) : Number(inv.derrotas || 0) + 1, winStreak: nextWin, lossStreak: nextLoss };
    const payloadPerf = { lumens: Number(perf.lumens || 0) + lumGain };
    await invRef.set(payloadInv, { merge: true });
    await perfRef.set(payloadPerf, { merge: true });
  };
  await Promise.all(vencedores.map(u => applyForUid(u, true)));
  await Promise.all(perdedores.map(u => applyForUid(u, false)));
  await ref.set({ statuspartida: 'concluida', fase: 'concluida', vencedor: vencedorStr }, { merge: true });
  const d2 = (await ref.get()).data() || {};
  if (channelId) await notifyResult(client, channelId, d2);
  return true;
}

const onClientReady = async () => {
  if (channelId) {
    const q1 = db.collection('aguardandoPartidas').where('status', '==', 'pending');
    const q2 = db.collection('aguardandoPartidas').where('status', '==', 'readyCheck');
    q1.onSnapshot((snap) => { snap.docChanges().forEach(c => { if (c.type === 'added') { const id = c.doc.id; const data = c.doc.data(); if (!notifiedReady.has(id)) { notifiedReady.add(id); notifyReadyCheck(client, channelId, data); } } }); });
    q2.onSnapshot((snap) => { snap.docChanges().forEach(c => { if (c.type === 'added') { const id = c.doc.id; const data = c.doc.data(); if (!notifiedReady.has(id)) { notifiedReady.add(id); notifyReadyCheck(client, channelId, data); } } }); });
  }
  const cmd = {
    name: 'vencedor',
    description: 'Força o vencedor de uma partida',
    options: [
      { name: 'uidpartida', description: 'ID da partida', type: 3, required: true },
      { name: 'lado', description: 'Lado vencedor', type: 3, required: true, choices: [ { name: 'azul', value: 'azul' }, { name: 'vermelho', value: 'vermelho' } ] }
    ]
  };
  try {
    if (guildId) {
      const guild = await client.guilds.fetch(guildId);
      await guild.commands.create(cmd);
    } else if (client.application) {
      await client.application.commands.create(cmd);
    }
  } catch (_) {}
};
client.once('ready', onClientReady);
client.once('clientReady', onClientReady);

client.on('messageCreate', async (msg) => {
  if (!msg.content) return;
  const t = msg.content.trim();
  if (t.startsWith('!finalizar')) {
    const parts = t.split(/\s+/);
    const partidaId = parts[1] || '';
    const side = parts[2] || '';
    if (partidaId && side) { await finalizeMatch(partidaId, side); msg.react('✅'); }
  }
});

client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName !== 'vencedor') return;
  const isAdmin = interaction.memberPermissions?.has(PermissionsBitField.Flags.Administrator);
  if (!isAdmin) {
    await interaction.reply({ content: 'Sem permissão para usar este comando.', ephemeral: true });
    return;
  }
  const partidaId = interaction.options.getString('uidpartida');
  const lado = interaction.options.getString('lado');
  let side = String(lado||'').toLowerCase();
  if (side.includes('azul')) side = 'blue';
  if (side.includes('vermelho')) side = 'red';
  if (!['blue','red'].includes(side)) {
    await interaction.reply({ content: 'Lado inválido. Use azul ou vermelho.', ephemeral: true });
    return;
  }
  await finalizeMatch(partidaId, side);
  await interaction.reply({ content: `Partida ${partidaId} finalizada com vencedor: ${side==='blue'?'Time Azul':'Time Vermelho'}.`, ephemeral: true });
});

client.login(token);
