require('dotenv').config()
const admin = require('firebase-admin')
const http = require('http')
const https = require('https')
const { URL } = require('url')
const {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  SlashCommandBuilder,
  REST,
  Routes,
  ChannelType,
  AttachmentBuilder,
  MessageFlags
} = require('discord.js')
const path = require('path')
const fs = require('fs')
const ga = require('./ga')
const emojiCache = {}

function parseServiceAccount() {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON
  if (!raw) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON não definido')
  let s = String(raw).trim()
  if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith('"') && s.endsWith('"'))) {
    s = s.slice(1, -1)
  }
  const obj = JSON.parse(s)
  if (obj.private_key) {
    obj.private_key = obj.private_key.replace(/\\n/g, '\n').replace(/\r/g, '')
  }
  return obj
}
let serviceAccount
try {
  serviceAccount = parseServiceAccount()
} catch (e) {
  console.error('Erro ao interpretar FIREBASE_SERVICE_ACCOUNT_JSON:', e && e.message ? e.message : e)
  process.exit(1)
}
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) })
const db = admin.firestore()
const botStartTime = admin.firestore.Timestamp.now()

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers, GatewayIntentBits.GuildMessages, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel]
})
let initDone = false
const discordJsVersion = (()=>{ try { const v = require('discord.js/package.json').version || '14.0.0'; return v } catch { return '14.0.0' } })()
const discordJsMajor = parseInt(String(discordJsVersion).split('.')[0] || '14', 10)

const token = process.env.DISCORD_TOKEN
if (!token) {
  console.error('DISCORD_TOKEN não definido')
  process.exit(1)
}

const RESULTS_CHANNEL_ID = process.env.DISCORD_RESULTS_CHANNEL_ID || '1444182835975028848'

const commands = [
  new SlashCommandBuilder().setName('fila').setDescription('Entrar/sair da fila'),
  new SlashCommandBuilder().setName('perfil').setDescription('Visualizar seu perfil básico'),
  new SlashCommandBuilder().setName('channels').setDescription('Listar canais da guild configurada'),
  new SlashCommandBuilder().setName('pendente').setDescription('Visualizar sua partida pendente'),
  new SlashCommandBuilder().setName('linkuid').setDescription('Vincular seu Discord ao UID do site').addStringOption((o)=>o.setName('uid').setDescription('Seu UID do site').setRequired(true)),
  new SlashCommandBuilder().setName('linkcode').setDescription('Gerar código para vincular pelo site'),
  new SlashCommandBuilder()
    .setName('resultado')
    .setDescription('Registrar resultado de partida (detecta pelo print)')
    .addStringOption((o)=> o.setName('match_id').setDescription('ID da partida (historicoPartidas)').setRequired(true))
    .addAttachmentOption((o)=> o.setName('imagem').setDescription('Print da partida').setRequired(true)),
  new SlashCommandBuilder()
    .setName('corrigirresultado')
    .setDescription('Corrigir resultado da partida')
    .addStringOption((o)=> o.setName('match_id').setDescription('ID da partida (historicoPartidas)').setRequired(true))
    .addStringOption((o)=> o.setName('vencedor').setDescription('Lado vencedor').setRequired(true)
      .addChoices(
        { name: 'Time Azul', value: 'azul' },
        { name: 'Time Vermelho', value: 'vermelho' }
      ))
    .addAttachmentOption((o)=> o.setName('imagem').setDescription('Print da partida').setRequired(true))
    .addStringOption((o)=> o.setName('motivo').setDescription('Motivo da correção').setRequired(false)),
  new SlashCommandBuilder()
    .setName('cleanupready')
    .setDescription('Apagar Ready Checks antigos ou todos')
    .addIntegerOption((o) => o.setName('age').setDescription('Idade mínima em minutos').setRequired(false))
    .addBooleanOption((o) => o.setName('all').setDescription('Apagar todos os Ready Checks').setRequired(false)),
  new SlashCommandBuilder().setName('readylist').setDescription('Listar Ready Checks ativos'),
  new SlashCommandBuilder().setName('clearqueue').setDescription('Remover sua entrada da fila'),
  new SlashCommandBuilder().setName('maketestmatch').setDescription('Criar partida de teste e retornar match_id'),
].map((c) => c.toJSON())

const Tesseract = require('tesseract.js')

function normalizeText(s) {
  return String(s||'')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-z0-9#\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function normalizeName(s){
  return String(s||'')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g,'')
    .replace(/\s+/g, '')
    .trim()
}

async function safeReply(interaction, data){
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data)
    } else {
      return await interaction.reply({ ...(typeof data === 'object' ? data : { content: String(data||'') }), flags: MessageFlags.Ephemeral })
    }
  } catch (e) {
    try {
      if (!interaction.deferred && !interaction.replied) { await interaction.deferReply({ flags: MessageFlags.Ephemeral }) }
      return await interaction.editReply(data)
    } catch {}
  }
}

function buildOcrDemoEmbed(attUrl, teams, uniqueMatched, ocrMs, textLen){
  const matchedNorm = new Set(uniqueMatched.map(n => normalizeName(n)))
  const fmt = (name) => `${matchedNorm.has(normalizeName(name)) ? '✅' : '❌'} ${name}`
  const azul = (teams.t1 || []).slice(0,10).map(fmt).join('\n') || '-'
  const vermelho = (teams.t2 || []).slice(0,10).map(fmt).join('\n') || '-'
  return new EmbedBuilder()
    .setTitle('Leitura do Print (OCR)')
    .addFields(
      { name: 'Tempo OCR', value: `${ocrMs} ms`, inline: true },
      { name: 'Texto extraído', value: `${textLen} chars`, inline: true },
      { name: 'Conferências', value: `${uniqueMatched.length}`, inline: true }
    )
    .addFields(
      { name: 'Time Azul (reconhecidos)', value: azul, inline: true },
      { name: 'Time Vermelho (reconhecidos)', value: vermelho, inline: true }
    )
    .setImage(attUrl)
    .setColor(0x5865F2)
    .setThumbnail('attachment://lollogo.png')
}

async function sendTemporaryChannelMessage(channel, payload, ms = 60 * 1000) {
  try {
    const m = await channel.send(payload)
    setTimeout(async () => {
      try { const msg = await channel.messages.fetch(m.id).catch(()=>null); if (msg && msg.deletable) await msg.delete().catch(()=>{}) } catch {}
    }, ms)
    return m
  } catch {}
}

async function sendTemporaryDmMessage(discordId, payload, ms = 60 * 1000) {
  try {
    const user = await client.users.fetch(discordId)
    const dm = await user.createDM()
    const m = await dm.send(payload)
    setTimeout(async () => {
      try { const msg = await dm.messages.fetch(m.id).catch(()=>null); if (msg && msg.deletable) await msg.delete().catch(()=>{}) } catch {}
    }, ms)
    return m
  } catch {}
}

function tokenizeTextNorm(textNorm){
  try { return String(textNorm||'').split(' ').filter(Boolean) } catch { return [] }
}

function nameMatches(textNorm, tokens, nn){
  if (!nn || nn.length < 3) return false
  if (textNorm.includes(nn)) return true
  for (const tk of tokens) {
    const t0 = tk.replace(/\d+/g,'')
    if (!t0) continue
    if (t0.includes(nn)) return true
    if (nn.includes(t0) && t0.length >= 3) return true
  }
  return false
}

async function ocrTextFromUrl(url) {
  try {
    const timeoutMs = Number(process.env.OCR_TIMEOUT_MS || '8000')
    const p = Tesseract.recognize(url, 'eng')
    const t = new Promise((_, reject) => setTimeout(() => reject(new Error('ocr_timeout')), timeoutMs))
    const res = await Promise.race([p, t])
    return res && res.data && res.data.text ? res.data.text : ''
  } catch { return '' }
}

function brandAssets() {
  const files = []
  const logoPath = path.join(__dirname, 'img', 'lollogo.png')
  const bgPath = path.join(__dirname, 'img', 'background.png')
  if (fs.existsSync(logoPath)) files.push(new AttachmentBuilder(logoPath, { name: 'lollogo.png' }))
  if (fs.existsSync(bgPath)) files.push(new AttachmentBuilder(bgPath, { name: 'background.png' }))
  return files
}

function actionAsset(kind) {
  const fname = kind === 'accept' ? 'aceitar.png' : 'recusar.png'
  const p = path.join(__dirname, 'img', fname)
  if (fs.existsSync(p)) return new AttachmentBuilder(p, { name: fname })
  return null
}

function emojiFor(guildId, key) {
  const m = emojiCache[guildId] || {}
  return m[key] || ''
}

async function ensureGuildEmojisForChannel(ch) {
  try {
    if (!ch || !ch.guild) return {}
    const guild = ch.guild
    const names = ['top','jg','mid','adc','sup','ferro','bronze','prata','ouro','platina','esmeralda','diamante','mestre','graomestre','desafiante','mvp']
    const existing = await guild.emojis.fetch().catch(()=>new Map())
    const map = emojiCache[guild.id] || {}
    for (const name of names) {
      if (map[name]) continue
      const found = [...existing.values()].find(e => e.name === name)
      if (found) { map[name] = `<:${found.name}:${found.id}>`; continue }
      const p = path.join(__dirname, 'img', `${name}.png`)
      if (fs.existsSync(p)) {
        try { const created = await guild.emojis.create({ attachment: p, name }); map[name] = `<:${created.name}:${created.id}>` } catch {}
      }
    }
    emojiCache[guild.id] = map
    return map
  } catch { return {} }
}

function normalizeLane(s){
  const n = String(s||'').toLowerCase()
  if (n.includes('top')) return 'top'
  if (n.includes('caç') || n.includes('jung') || n.includes('jg')) return 'jg'
  if (n.includes('mid') || n.includes('meio')) return 'mid'
  if (n.includes('adc') || n.includes('atir')) return 'adc'
  if (n.includes('sup') || n.includes('suporte')) return 'sup'
  return ''
}

function normalizeElo(s){
  const n = String(s||'').toLowerCase()
  if (n.includes('ferro')) return 'ferro'
  if (n.includes('bronze')) return 'bronze'
  if (n.includes('prata')) return 'prata'
  if (n.includes('ouro')) return 'ouro'
  if (n.includes('plat')) return 'platina'
  if (n.includes('esmer')) return 'esmeralda'
  if (n.includes('diam')) return 'diamante'
  if (n.includes('mestre')) return 'mestre'
  if (n.includes('grão') || n.includes('grao')) return 'graomestre'
  if (n.includes('desaf')) return 'desafiante'
  return ''
}

async function formatTeamsMentionsFromHistorico(matchId) {
  try {
    const ref = db.collection('Historico').doc(matchId)
    const snap = await ref.get()
    if (!snap.exists) return { blueStr: '-', redStr: '-' }
    const d = snap.data() || {}
    const blue = d.time1?.jogadores || d.times?.time1?.jogadores || []
    const red = d.time2?.jogadores || d.times?.time2?.jogadores || []
    async function enrich(list){
      const out = []
      for (const p of Array.isArray(list) ? list : []) {
        const id = p.discordUserId || (p.uid ? await resolveDiscordIdByUid(p.uid) : null)
        out.push({ ...p, discordUserId: id || p.discordUserId || null })
      }
      return out
    }
    const be = await enrich(blue)
    const re = await enrich(red)
    return { blueStr: formatPlayersResult(be, ''), redStr: formatPlayersResult(re, '') }
  } catch { return { blueStr: '-', redStr: '-' } }
}

async function getMatchPlayerNames(matchId){
  try {
    const ref = db.collection('Historico').doc(matchId)
    const snap = await ref.get()
    if (!snap.exists) return []
    const d = snap.data() || {}
    const t1 = (d.time1 && d.time1.jogadores) ? d.time1.jogadores : []
    const t2 = (d.time2 && d.time2.jogadores) ? d.time2.jogadores : []
    const names = []
    t1.forEach(j => { if (j && j.nome) names.push(String(j.nome)) })
    t2.forEach(j => { if (j && j.nome) names.push(String(j.nome)) })
    return names
  } catch { return [] }
}

async function getMatchTeamNames(matchId){
  try {
    const ref = db.collection('Historico').doc(matchId)
    const snap = await ref.get()
    if (!snap.exists) return { t1: [], t2: [], time1Name: 'Time Azul', time2Name: 'Time Vermelho' }
    const d = snap.data() || {}
    const t1 = (d.time1 && d.time1.jogadores) ? d.time1.jogadores : []
    const t2 = (d.time2 && d.time2.jogadores) ? d.time2.jogadores : []
    return {
      t1: t1.map(j => String(j.nome||'')),
      t2: t2.map(j => String(j.nome||'')),
      time1Name: d.time1?.nome || 'Time Azul',
      time2Name: d.time2?.nome || 'Time Vermelho'
    }
  } catch { return { t1: [], t2: [], time1Name: 'Time Azul', time2Name: 'Time Vermelho' } }
}

function detectWinnerFromText(text, team1Names, team2Names){
  const norm = normalizeText(text)
  function count(names){ let c = 0; for (const n of names) { const nn = normalizeName(n); if (nn && nn.length >= 3 && norm.includes(nn)) c++ } return c }
  const c1 = count(team1Names)
  const c2 = count(team2Names)
  if (c1 === 0 && c2 === 0) return null
  if (c1 > c2) return 'time1'
  if (c2 > c1) return 'time2'
  return null
}

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(token)
  const guildId = process.env.DISCORD_GUILD_ID
  if (guildId) {
    await rest.put(
      Routes.applicationGuildCommands(client.application.id, guildId),
      { body: commands }
    )
  } else {
    await rest.put(Routes.applicationCommands(client.application.id), { body: commands })
  }
}

function queueDoc(uid) {
  return db.collection('queuee').doc(uid)
}

function userDoc(uid) {
  return db.collection('users').doc(uid)
}

async function resolveUidByDiscordId(discordId) {
  try {
    const q = await db.collection('users').where('discordUserId', '==', discordId).limit(1).get()
    if (q.empty) return null
    return q.docs[0].id
  } catch { return null }
}

async function getAnnounceChannel() {
  const id = process.env.DISCORD_ANNOUNCE_CHANNEL_ID
  if (id) {
    try {
      const ch = await client.channels.fetch(id)
      if (ch && ch.type === ChannelType.GuildText) return ch
    } catch {}
  }
  const guild = client.guilds.cache.first()
  if (!guild) return null
  const channels = await guild.channels.fetch()
  const byName = channels.find(
    (c) => c && c.type === ChannelType.GuildText && ['geral', 'matchmaking'].includes(c.name.toLowerCase())
  )
  if (byName) return byName
  const anyText = channels.find((c) => c && c.type === ChannelType.GuildText)
  return anyText || null
}

async function getOngoingChannel() {
  const id = process.env.DISCORD_ONGOING_CHANNEL_ID
  if (id) {
    try { const ch = await client.channels.fetch(id); if (ch && ch.type === ChannelType.GuildText) return ch } catch {}
  }
  return await getAnnounceChannel()
}

// Canal de Ready Check configurável via variável de ambiente
// Usa `DISCORD_READY_CHANNEL_ID` e faz fallback para o canal de anúncios.
async function getReadyChannel() {
  const id = process.env.DISCORD_READY_CHANNEL_ID || '1442962746537676831'
  if (id) {
    try { const ch = await client.channels.fetch(id); if (ch && ch.type === ChannelType.GuildText) return ch } catch {}
  }
  return await getAnnounceChannel()
}

async function getQueueChannel() {
  const id = process.env.DISCORD_QUEUE_CHANNEL_ID || '1442962948703391764'
  if (id) {
    try { const ch = await client.channels.fetch(id); if (ch && ch.type === ChannelType.GuildText) return ch } catch {}
  }
  return await getAnnounceChannel()
}

async function onClientReady() {
  if (initDone) return
  initDone = true
  try { console.log('[boot]', new Date().toISOString(), 'ready', { user: client.user?.tag }) } catch {}
  try { ga.trackEvent('bot_ready', { tag: client.user?.tag || '' }) } catch {}
  try {
    await registerCommands()
    console.log('[boot]', new Date().toISOString(), 'commands registered')
  } catch (e) {
    console.error('Falha ao registrar comandos', e)
  }
  try { await publishDiscordConfig() } catch {}
  try { await ensurePinnedHelpMessages() } catch {}
  setupMatchListeners()
  try { console.log('[boot]', new Date().toISOString(), 'listeners started') } catch {}
  startOAuthServer()
}
client.once('clientReady', onClientReady)

client.on('interactionCreate', async (interaction) => {
  try {
    try { const name = interaction.isChatInputCommand() ? interaction.commandName : (interaction.isButton() ? interaction.customId : 'other'); console.log('[interaction]', new Date().toISOString(), { type: interaction.type, name, userId: interaction.user?.id }) } catch {}
    try { if (interaction.isChatInputCommand()) { ga.trackEvent('bot_command', { command: interaction.commandName }, interaction.user?.id) } else if (interaction.isButton()) { ga.trackEvent('bot_button', { id: interaction.customId }, interaction.user?.id) } } catch {}
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === 'fila') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode antes de usar a fila.', flags: MessageFlags.Ephemeral }); return }
        const snap = await isInQueue(uid)
        const row = new ActionRowBuilder()
        const btnJoin = new ButtonBuilder().setCustomId(`queue_confirm_join:${uid}`).setLabel('Entrar na Fila').setStyle(ButtonStyle.Primary)
        const btnLeave = new ButtonBuilder().setCustomId(`queue_confirm_leave:${uid}`).setLabel('Sair da Fila').setStyle(ButtonStyle.Danger)
        if (!snap) {
          row.addComponents(btnJoin)
          await interaction.reply({ content: 'Você não está na fila. Deseja entrar?', components: [row], flags: MessageFlags.Ephemeral })
          try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 30000) } catch {}
        } else {
          row.addComponents(btnLeave)
          await interaction.reply({ content: 'Você já está na fila. Deseja sair?', components: [row], flags: MessageFlags.Ephemeral })
          try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 30000) } catch {}
        }
      }
      if (interaction.commandName === 'perfil') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver seu perfil.', flags: MessageFlags.Ephemeral }); return }
        const uref = userDoc(uid)
        const usnap = await uref.get()
        const data = usnap.exists ? usnap.data() : {}
        const inQ = await isInQueue(uid)
        const inQueue = !!inQ
        const rank = await getRankingForUser(data)
        const nomeBase = data.playerName || data.nome || interaction.member?.displayName || interaction.user.username
        const embed = new EmbedBuilder()
          .setTitle('Perfil do Jogador')
          .addFields(
            { name: 'Nome', value: `${nomeBase}` , inline: true },
            { name: 'Elo', value: `${rank.elo ?? '-'}`, inline: true },
            { name: 'Divisão', value: `${rank.divisao ?? '-'}`, inline: true },
            { name: 'Role Principal', value: `${data.rolePrincipal ?? '-'}`, inline: true },
            { name: 'Role Secundária', value: `${data.roleSecundaria ?? '-'}`, inline: true },
            { name: 'Status da Fila', value: inQueue ? 'Na fila' : 'Fora da fila', inline: true }
          )
        await interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral })
      }
      if (interaction.commandName === 'linkuid') {
        const uid = interaction.options.getString('uid')
        try {
          const ref = userDoc(uid)
          const usnap = await ref.get()
          if (!usnap.exists) {
            await ref.set({
              uid,
              nome: interaction.user.username,
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
              source: 'discord-autoprovision'
            }, { merge: true })
          }
          const discordId = interaction.user.id
          const q = await db.collection('users').where('discordUserId','==',discordId).limit(1).get()
          if (!q.empty && q.docs[0].id !== uid) { await interaction.reply({ content: 'Seu Discord já está vinculado a outro UID.', flags: MessageFlags.Ephemeral }); return }
          await ref.set({ discordUserId: discordId, discordUsername: interaction.user.username, discordLinkedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
          await interaction.reply({ content: 'Vínculo com o site concluído com sucesso. Deslogue da sua conta e logue novamente.', flags: MessageFlags.Ephemeral })
        } catch (e) {
          await interaction.reply({ content: 'Falha ao vincular. Tente novamente mais tarde.', flags: MessageFlags.Ephemeral })
        }
      }
      if (interaction.commandName === 'linkcode') {
        try {
          const discordId = interaction.user.id
          const code = await createUniqueCode(discordId)
          if (!code) { await interaction.reply({ content: 'Não foi possível gerar código. Tente novamente.', flags: MessageFlags.Ephemeral }); return }
          await interaction.reply({ content: `Seu código: ${code}. Use-o em Editar Perfil → Vincular pelo Código. Expira em 10 minutos.`, flags: MessageFlags.Ephemeral })
          try { const user = await client.users.fetch(discordId); await user.send({ content: `Código de vínculo: ${code}. Expira em 10 minutos.` }) } catch {}
        } catch {
          await interaction.reply({ content: 'Falha ao gerar código.', flags: MessageFlags.Ephemeral })
        }
      }
      if (interaction.commandName === 'pendente') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver pendências.', flags: MessageFlags.Ephemeral }); return }
        const col = db.collection('aguardandoPartidas')
        const qsnap = await col.where('uids','array-contains', uid).get().catch(()=>null)
        if (!qsnap || qsnap.empty) { await interaction.reply({ content: 'Nenhuma partida pendente.', flags: MessageFlags.Ephemeral }); return }
        let doc = null
        let latest = -1
        qsnap.forEach((d) => {
          const dataTmp = d.data() || {}
          const ok = String(dataTmp.status||'') === 'readyCheck'
          const ts = dataTmp.createdAt && dataTmp.createdAt.toMillis ? dataTmp.createdAt.toMillis() : 0
          if (ok && ts >= latest) { latest = ts; doc = d }
        })
        if (!doc) { await interaction.reply({ content: 'Nenhuma partida pendente.', flags: MessageFlags.Ephemeral }); return }
        const data = doc.data() || {}
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId(`accept:${doc.id}`).setLabel('Aceitar Partida').setStyle(ButtonStyle.Success),
          new ButtonBuilder().setCustomId(`decline:${doc.id}`).setLabel('Recusar Partida').setStyle(ButtonStyle.Danger)
        )
        const jogadores = playerList(data)
        const detalhes = formatPlayersResult(jogadores, '') || '-'
        const embed = new EmbedBuilder().setTitle('Partida Pendente').addFields(
          { name: 'Status', value: String(data.status||'readyCheck'), inline: true },
          { name: 'Expira em', value: data.timestampFim?.toDate ? data.timestampFim.toDate().toLocaleString() : '—', inline: true },
          { name: 'Jogadores', value: detalhes }
        )
        await interaction.reply({ embeds: [embed], components: [row], flags: MessageFlags.Ephemeral })
      }
      
      if (interaction.commandName === 'cleanupready') {
        const age = interaction.options.getInteger('age')
        const all = interaction.options.getBoolean('all')
        let count = 0
        const col = db.collection('aguardandoPartidas')
        const limitMs = age ? Date.now() - age * 60 * 1000 : 0
        let q = col.where('status','==','readyCheck')
        if (!all && limitMs) {
          const limitTs = admin.firestore.Timestamp.fromMillis(limitMs)
          q = q.where('createdAt','<', limitTs)
        }
        const qsnap = await q.get()
        for (const doc of qsnap.docs) { try { await doc.ref.delete(); count++ } catch {} }
        await interaction.reply({ content: `Ready Checks apagados: ${count}`, flags: MessageFlags.Ephemeral })
      }
      if (interaction.commandName === 'readylist') {
        const q = db.collection('aguardandoPartidas')
          .where('status','==','readyCheck')
          .orderBy('createdAt','desc')
          .limit(50)
        const qsnap = await q.get()
        const lines = []
        qsnap.forEach((doc) => {
          const d = doc.data() || {}
          const t = d.createdAt && d.createdAt.toDate ? d.createdAt.toDate().toLocaleString() : 'sem data'
          lines.push(`${doc.id} - ${t}`)
        })
        await interaction.reply({ content: lines.join('\n') || 'Nenhum Ready Check.', flags: MessageFlags.Ephemeral })
      }
      if (interaction.commandName === 'clearqueue') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Nenhum vínculo encontrado. Use /link primeiro.', flags: MessageFlags.Ephemeral }); return }
        await queueDoc(uid).delete().catch(() => {})
        await interaction.reply({ content: 'Sua entrada na fila foi removida.', flags: MessageFlags.Ephemeral })
      }
      if (interaction.commandName === 'maketestmatch') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await safeReply(interaction, { content: 'Use este comando no canal de Resultados.' }); return }
        const id = `test_${Date.now()}`
        const ref = db.collection('Historico').doc(id)
        const nomes1 = ['Alpha','Bravo','Charlie','Delta','Echo']
        const nomes2 = ['Foxtrot','Golf','Hotel','India','Juliet']
        const payload = {
          status: 'pending',
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          time1: { nome: 'Time Azul', jogadores: nomes1.map(n=>({nome:n})) },
          time2: { nome: 'Time Vermelho', jogadores: nomes2.map(n=>({nome:n})) }
        }
        try {
          await ref.set(payload)
          await interaction.reply({ content: `Partida de teste criada. match_id: ${id}`, flags: MessageFlags.Ephemeral })
          try {
            const pub = new EmbedBuilder().setTitle('Partida de Teste Criada').addFields(
              { name: 'match_id', value: id, inline: true },
              { name: 'Time 1', value: nomes1.join(', '), inline: true },
              { name: 'Time 2', value: nomes2.join(', '), inline: true }
            ).setColor(0x5865F2)
            await sendTemporaryChannelMessage(interaction.channel, { embeds: [pub] }, 60*1000)
          } catch {}
        } catch (e) {
          await interaction.reply({ content: 'Falha ao criar partida de teste.', flags: MessageFlags.Ephemeral })
        }
      }
      if (interaction.commandName === 'resultado') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await safeReply(interaction, { content: 'Use este comando no canal de Resultados.' }); return }
        const matchId = interaction.options.getString('match_id')
        const att = interaction.options.getAttachment('imagem')
        console.log('[resultado]', new Date().toISOString(), 'start', { userId: interaction.user.id, channelId: interaction.channelId, matchId })
        if (!interaction.deferred && !interaction.replied) { try { await interaction.deferReply({ flags: MessageFlags.Ephemeral }) } catch (e) { console.error('[resultado]', new Date().toISOString(), 'defer error', e && e.code, e && e.message) } }
        console.log('[resultado]', new Date().toISOString(), 'deferred')
        if (!matchId) { await safeReply(interaction, { content: 'Informe o match_id.' }); return }
        if (!att || !att.url) { await safeReply(interaction, { content: 'Anexe o print da partida (imagem).' }); return }
        try {
          const ref = db.collection('Historico').doc(matchId)
          const snap = await ref.get()
          console.log('[resultado]', new Date().toISOString(), 'historico fetch', { exists: snap.exists })
          if (!snap.exists) { await safeReply(interaction, { content: 'Partida não encontrada.' }); return }
          const d0 = snap.data() || {}
          if (d0.vencedor && d0.vencedor !== 'N/A') { console.log('[resultado]', new Date().toISOString(), 'already has winner', { vencedor: d0.vencedor }); await safeReply(interaction, { content: `Partida já possui vencedor (${d0.vencedor}). Use /corrigirresultado para alterar.` }); return }
          console.log('[resultado]', new Date().toISOString(), 'team names start')
          const teams = await getMatchTeamNames(matchId)
          console.log('[resultado]', new Date().toISOString(), 'ocr start')
          const ocrT0 = Date.now()
          const text = await ocrTextFromUrl(att.url)
          console.log('[resultado]', new Date().toISOString(), 'ocr done', { ms: Date.now()-ocrT0, textLen: String(text||'').length })
          const textNorm = normalizeText(text)
          const tokens = tokenizeTextNorm(textNorm)
          let matched = []
          for (const n of teams.t1.concat(teams.t2)) { const nn = normalizeName(n); if (nameMatches(textNorm, tokens, nn)) matched.push(n) }
          const uniqueMatched = Array.from(new Set(matched))
          console.log('[resultado]', new Date().toISOString(), 'players matched', { count: uniqueMatched.length })
          try { const demo = buildOcrDemoEmbed(att.url, teams, uniqueMatched, Date.now()-ocrT0, String(text||'').length); await sendTemporaryChannelMessage(interaction.channel, { embeds: [demo], files: brandAssets() }, 60*1000) } catch {}
          if (uniqueMatched.length < 6) {
            const txt = `Não foi possível validar o print: apenas ${uniqueMatched.length} jogador(es) conferem com a partida. É necessário coincidir pelo menos 6.`
            await safeReply(interaction, { content: txt })
            try { await sendTemporaryChannelMessage(interaction.channel, { content: txt }, 60*1000) } catch {}
            return
          }
          const winnerSide = detectWinnerFromText(text, teams.t1, teams.t2)
          console.log('[resultado]', new Date().toISOString(), 'winner by names count', { winnerSide })
          if (!winnerSide) { const txt = 'Não foi possível determinar o time vencedor a partir dos nomes do print. Use /corrigirresultado se necessário.'; await safeReply(interaction, { content: txt }); try { await sendTemporaryChannelMessage(interaction.channel, { content: txt }, 60*1000) } catch {}; return }
          const vencedor = winnerSide === 'time1' ? teams.time1Name : teams.time2Name
          const payload = {
            vencedor,
            resultadoSource: 'discord',
            resultadoBy: interaction.user.id,
            resultadoAt: admin.firestore.FieldValue.serverTimestamp()
          }
          payload.proofUrl = att.url
          payload.verificadoPorOcr = true
          payload.jogadoresValidados = uniqueMatched.slice(0, 10)
          payload.qtdJogadoresValidados = uniqueMatched.length
          console.log('[resultado]', new Date().toISOString(), 'firestore set start')
          const setT0 = Date.now()
          await ref.set(payload, { merge: true })
          console.log('[resultado]', new Date().toISOString(), 'firestore set done', { ms: Date.now()-setT0 })
          await safeReply(interaction, { content: `Resultado registrado: ${vencedor} (match_id: ${matchId}).` })
          console.log('[resultado]', new Date().toISOString(), 'reply edited')
          try {
            const teamsMentions = await formatTeamsMentionsFromHistorico(matchId)
            const pub = new EmbedBuilder()
              .setTitle('Resultado de Partida')
              .addFields(
                { name: 'match_id', value: `${matchId}`, inline: true },
                { name: 'Vencedor', value: `${vencedor}`, inline: true },
                { name: 'Validados', value: `${uniqueMatched.length} jogadores`, inline: true }
              )
              .addFields(
                { name: 'Time Azul', value: teamsMentions.blueStr || '-', inline: true },
                { name: 'Time Vermelho', value: teamsMentions.redStr || '-', inline: true }
              )
              .setImage(att.url)
              .setColor(0x57F287)
              .setThumbnail('attachment://lollogo.png')
            await sendTemporaryChannelMessage(interaction.channel, { embeds: [pub], files: brandAssets() }, 60*1000)
            console.log('[resultado]', new Date().toISOString(), 'public embed sent')
          } catch {}
        } catch (e) {
          console.error('[resultado]', new Date().toISOString(), 'error', e && e.stack ? e.stack : e)
          await safeReply(interaction, { content: 'Falha ao registrar resultado.' })
        }
      }
      if (interaction.commandName === 'corrigirresultado') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await interaction.reply({ content: 'Use este comando no canal de Resultados.', flags: MessageFlags.Ephemeral }); return }
        const matchId = interaction.options.getString('match_id')
        const vencedorOpt = interaction.options.getString('vencedor')
        const motivo = interaction.options.getString('motivo') || ''
        const att = interaction.options.getAttachment('imagem')
        console.log('[corrigirresultado]', new Date().toISOString(), 'start', { userId: interaction.user.id, channelId: interaction.channelId, matchId, vencedorOpt, motivoLen: String(motivo||'').length })
        if (!interaction.deferred && !interaction.replied) { try { await interaction.deferReply({ flags: MessageFlags.Ephemeral }) } catch (e) { console.error('[corrigirresultado]', new Date().toISOString(), 'defer error', e && e.code, e && e.message) } }
        console.log('[corrigirresultado]', new Date().toISOString(), 'deferred')
        if (!matchId || !vencedorOpt) { await safeReply(interaction, { content: 'Parâmetros inválidos.' }); return }
        if (!att || !att.url) { await safeReply(interaction, { content: 'Anexe o print da partida (imagem).' }); return }
        try {
          const ref = db.collection('Historico').doc(matchId)
          const snap = await ref.get()
          console.log('[corrigirresultado]', new Date().toISOString(), 'historico fetch', { exists: snap.exists })
          if (!snap.exists) { await safeReply(interaction, { content: 'Partida não encontrada.' }); return }
          const teams = await getMatchTeamNames(matchId)
          console.log('[corrigirresultado]', new Date().toISOString(), 'ocr start')
          const ocrT0 = Date.now()
          const text = await ocrTextFromUrl(att.url)
          console.log('[corrigirresultado]', new Date().toISOString(), 'ocr done', { ms: Date.now()-ocrT0, textLen: String(text||'').length })
          const textNorm = normalizeText(text)
          const tokens = tokenizeTextNorm(textNorm)
          let matched = []
          for (const n of teams.t1.concat(teams.t2)) { const nn = normalizeName(n); if (nameMatches(textNorm, tokens, nn)) matched.push(n) }
          const uniqueMatched = Array.from(new Set(matched))
          console.log('[corrigirresultado]', new Date().toISOString(), 'players matched', { count: uniqueMatched.length })
          try { const demo = buildOcrDemoEmbed(att.url, teams, uniqueMatched, Date.now()-ocrT0, String(text||'').length); await sendTemporaryChannelMessage(interaction.channel, { embeds: [demo], files: brandAssets() }, 60*1000) } catch {}
          if (uniqueMatched.length < 6) { const txt = `Não foi possível validar o print: apenas ${uniqueMatched.length} jogador(es) conferem com a partida. É necessário coincidir pelo menos 6.`; await safeReply(interaction, { content: txt }); try { await sendTemporaryChannelMessage(interaction.channel, { content: txt }, 60*1000) } catch {}; return }
          const vencedor = vencedorOpt === 'azul' ? teams.time1Name : teams.time2Name
          const payload = {
            vencedor,
            resultadoSource: 'discord-corrigido',
            resultadoBy: interaction.user.id,
            resultadoAt: admin.firestore.FieldValue.serverTimestamp(),
            corrigido: true,
            corrigidoMotivo: motivo
          }
          payload.proofUrl = att.url
          payload.verificadoPorOcr = true
          payload.jogadoresValidados = uniqueMatched.slice(0, 10)
          payload.qtdJogadoresValidados = uniqueMatched.length
          console.log('[corrigirresultado]', new Date().toISOString(), 'firestore set start')
          const setT0 = Date.now()
          await ref.set(payload, { merge: true })
          console.log('[corrigirresultado]', new Date().toISOString(), 'firestore set done', { ms: Date.now()-setT0 })
          await safeReply(interaction, { content: `Resultado corrigido para ${vencedor} (match_id: ${matchId}).` })
          console.log('[corrigirresultado]', new Date().toISOString(), 'reply edited')
          try {
            const pub = new EmbedBuilder()
              .setTitle('Correção de Resultado')
              .addFields(
                { name: 'match_id', value: `${matchId}`, inline: true },
                { name: 'Vencedor', value: `${vencedor}`, inline: true },
                { name: 'Motivo', value: motivo || '—', inline: true },
                { name: 'Validados', value: `${uniqueMatched.length} jogadores`, inline: true }
              )
              .addFields(
                { name: 'Time Azul', value: (await formatTeamsMentionsFromHistorico(matchId)).blueStr || '-', inline: true },
                { name: 'Time Vermelho', value: (await formatTeamsMentionsFromHistorico(matchId)).redStr || '-', inline: true }
              )
              .setImage(att.url)
              .setColor(0xFEE75C)
              .setThumbnail('attachment://lollogo.png')
            await sendTemporaryChannelMessage(interaction.channel, { embeds: [pub], files: brandAssets() }, 60*1000)
            console.log('[corrigirresultado]', new Date().toISOString(), 'public embed sent')
          } catch {}
        } catch (e) {
          console.error('[corrigirresultado]', new Date().toISOString(), 'error', e && e.stack ? e.stack : e)
          await safeReply(interaction, { content: 'Falha ao corrigir resultado.' })
        }
      }
      
      if (interaction.commandName === 'channels') {
        const guildId = process.env.DISCORD_GUILD_ID
        const guild = guildId ? await client.guilds.fetch(guildId).catch(() => null) : client.guilds.cache.first()
        if (!guild) { await interaction.reply({ content: 'Guild não encontrada.', flags: MessageFlags.Ephemeral }); return }
        const channels = await guild.channels.fetch()
        const lines = []
        channels.forEach((c) => { if (c && c.type === ChannelType.GuildText) lines.push(`${c.name} ${c.id}`) })
        const out = lines.slice(0, 50).join('\n')
        await interaction.reply({ content: out || 'Sem canais de texto.', flags: MessageFlags.Ephemeral })
      }
  } else if (interaction.isButton()) {
    const cid = interaction.customId
    const parts = cid.split(':')
    const action = parts[0]
    const matchId = parts[1]
    const targetUserId = parts[2]
    const userId = interaction.user.id
    const uid = await resolveUidByDiscordId(userId)
    
    if (action === 'perfil_open') {
      if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver seu perfil.', flags: MessageFlags.Ephemeral }); return }
      const uref = userDoc(uid)
      const usnap = await uref.get()
      const data = usnap.exists ? usnap.data() : {}
      const inQ = await isInQueue(uid)
      const inQueue = !!inQ
      const rank = await getRankingForUser(data)
      const nomeBase = data.playerName || data.nome || interaction.member?.displayName || interaction.user.username
      const embed = new EmbedBuilder()
        .setTitle('Perfil do Jogador')
        .addFields(
          { name: 'Nome', value: `${nomeBase}` , inline: true },
          { name: 'Elo', value: `${rank.elo ?? '-'}`, inline: true },
          { name: 'Divisão', value: `${rank.divisao ?? '-'}`, inline: true },
          { name: 'Role Principal', value: `${data.rolePrincipal ?? '-'}`, inline: true },
          { name: 'Role Secundária', value: `${data.roleSecundaria ?? '-'}`, inline: true },
          { name: 'Status da Fila', value: inQueue ? 'Na fila' : 'Fora da fila', inline: true }
        )
      await interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral })
      try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 10000) } catch {}
      return
    }
    if ((action === 'accept' || action === 'decline') && targetUserId && targetUserId !== userId) {
      await interaction.reply({ content: 'Este botão não é para você.', flags: MessageFlags.Ephemeral });
      return
    }
    if (action === 'linkuid_start') {
      const txt = 'Para vincular via UID: Abra Editar Perfil no site, copie seu UID e use o comando /linkuid uid:<seu UID> aqui no Discord.'
      await interaction.reply({ content: txt, flags: MessageFlags.Ephemeral }); return
    }
    if (action === 'linkcode_start') {
      const code = await createUniqueCode(userId)
      if (!code) { await interaction.reply({ content: 'Não foi possível gerar código. Tente novamente.', flags: MessageFlags.Ephemeral }); return }
      await interaction.reply({ content: `Seu código: ${code}. Use-o em Editar Perfil → Vincular pelo Código. Expira em 10 minutos.`, flags: MessageFlags.Ephemeral }); return
    }
    if (action === 'queue_join') {
      if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode antes de entrar na fila.', flags: MessageFlags.Ephemeral }); return }
      const existing = await isInQueue(uid)
      if (existing) {
        const msg = [
          'Você já está na fila.',
          '• Para sair, clique em "Sair da Fila" ou use o comando /fila.'
        ].join('\n')
        await interaction.reply({ content: msg, flags: MessageFlags.Ephemeral })
        return
      }
      const uref = userDoc(uid)
      const usnap = await uref.get()
      const data = usnap.exists ? usnap.data() : {}
      const rank = await getRankingForUser(data)
      const nomeBase = data.playerName || data.nome || (interaction.member && interaction.member.displayName) || interaction.user.username
      const payload = userToQueueData(uid, { ...data, nome: nomeBase, elo: rank.elo, divisao: rank.divisao, discordUserId: interaction.user.id, discordUsername: interaction.user.username })
      await db.collection('queue').doc(uid).set(payload)
      const msg = [
        'Você entrou na fila com sucesso.',
        '• Você receberá um Ready Check quando uma partida for montada.',
        '• Para sair, clique em "Sair da Fila" ou use o comando /fila.'
      ].join('\n')
      await interaction.reply({ content: msg, flags: MessageFlags.Ephemeral })
      try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 5000) } catch {}
      return
    }
    if (action === 'queue_confirm_join') {
      const targetUid = matchId || uid
      if (!targetUid) { await interaction.reply({ content: 'Vincule seu Discord primeiro.', flags: MessageFlags.Ephemeral }); return }
      const uref = userDoc(targetUid)
      const usnap = await uref.get()
      const data = usnap.exists ? usnap.data() : {}
      const existing = await isInQueue(targetUid)
      if (!existing) {
        const rank = await getRankingForUser(data)
        const nomeBase = data.playerName || data.nome || (interaction.member && interaction.member.displayName) || interaction.user.username
        const payload = userToQueueData(targetUid, { ...data, nome: nomeBase, elo: rank.elo, divisao: rank.divisao, discordUserId: interaction.user.id, discordUsername: interaction.user.username })
        await db.collection('queue').doc(targetUid).set(payload)
        await interaction.reply({ content: 'Você entrou na fila!', flags: MessageFlags.Ephemeral })
        try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 5000) } catch {}
      } else {
        await interaction.reply({ content: 'Você já está na fila.', flags: MessageFlags.Ephemeral })
        try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 5000) } catch {}
      }
      return
    }
    if (action === 'queue_confirm_leave') {
      const targetUid = matchId || uid
      if (!targetUid) { await interaction.reply({ content: 'Vincule seu Discord primeiro.', flags: MessageFlags.Ephemeral }); return }
      try {
        const qsnap = await db.collection('queue').where('uid','==',targetUid).get()
        const dels = []
        qsnap.forEach(doc=> dels.push(doc.ref.delete()))
        await Promise.all(dels)
      } catch {}
      await interaction.reply({ content: 'Você saiu da fila.', flags: MessageFlags.Ephemeral })
      try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 5000) } catch {}
      return
    }
    if (action === 'queue_leave') {
      if (!uid) { await interaction.reply({ content: 'Nenhum vínculo encontrado. Use /linkuid ou /linkcode primeiro.', flags: MessageFlags.Ephemeral }); return }
      try {
        const qsnap = await db.collection('queue').where('uid','==',uid).get()
        const dels = []
        qsnap.forEach(doc=> dels.push(doc.ref.delete()))
        await Promise.all(dels)
      } catch {}
      const msg = [
        'Você saiu da fila.',
        '• Quando quiser retornar, clique em "Entrar na Fila" ou use o comando /fila.'
      ].join('\n')
      await interaction.reply({ content: msg, flags: MessageFlags.Ephemeral });
      try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 5000) } catch {}
      return
    }
    if (action === 'queue_confirm_join' || action === 'queue_confirm_leave' || action === 'queue_join' || action === 'queue_leave') {
      try {
        const msg = interaction.message
        if (msg && !msg.pinned && msg.deletable) {
          await msg.delete().catch(()=>{})
        }
      } catch {}
      return
    }
    if (action === 'accept' || action === 'decline') {
      const mref = db.collection('aguardandoPartidas').doc(matchId)
      const msnap = await mref.get()
      if (!msnap.exists) { await interaction.reply({ content: 'Partida não encontrada ou expirada.', flags: MessageFlags.Ephemeral }); return }
      const matchData = msnap.data() || {}
      if (uid && Array.isArray(matchData.uids) && !matchData.uids.includes(uid)) { await interaction.reply({ content: 'Você não está nesta partida.', flags: MessageFlags.Ephemeral }); return }
      await mref.set({ playersReady: msnap.data().playersReady || {}, playerAcceptances: msnap.data().playerAcceptances || {} }, { merge: true })
      if (action === 'accept') {
        await mref.update({ [`playersReady.${userId}`]: true })
        if (uid) { await mref.update({ [`playerAcceptances.${uid}`]: 'accepted' }) }
        try {
          const updated = await mref.get()
          const d = updated.data() || {}
          const playersRaw = playersFromTimes(d).length ? playersFromTimes(d) : playerList(d)
          const acceptMap = d.playerAcceptances || {}
          const players = []
          for (let p of playersRaw) {
            let obj = typeof p === 'object' ? { ...p } : { nome: String(p||'') }
            const uidIt = obj.uid || null
            if (uidIt) { try { const us = await userDoc(uidIt).get(); if (us.exists) { const ud = us.data()||{}; obj.tag = ud.tag || obj.tag; obj.nome = obj.nome || ud.nome || ud.playerName; obj.elo = ud.elo || obj.elo; obj.divisao = ud.divisao || obj.divisao; obj.role = obj.role || ud.roleAtribuida || ud.rolePrincipal } } catch {} }
            players.push(obj)
          }
          const details = players.map((p) => { const nome = String(p.nome||'').trim(); const tag = String(p.tag||'').trim().replace(/^#/,''); const handle = tag ? `${nome}#${tag}` : nome; const puid = p.uid || null; const accepted = puid && acceptMap[puid] === 'accepted'; const mark = accepted ? '✅ ' : ''; const div = p.divisao || ''; const lane = p.roleAtribuida || p.role || p.funcao || ''; const laneText = lane ? ` (${lane})` : ''; return `${mark}${handle} • ${div}${laneText}` }).join('\n')
          const msg = interaction.message
          if (msg) {
            const until = d.timestampFim && d.timestampFim.toDate ? d.timestampFim.toDate() : null
            const now = new Date()
            const msLeft = until ? Math.max(0, until.getTime() - now.getTime()) : 30 * 1000
            const secLeft = Math.ceil(msLeft / 1000)
            const row = new ActionRowBuilder().addComponents(
              new ButtonBuilder().setCustomId(`accept:${matchId}`).setLabel('Aceitar').setStyle(ButtonStyle.Success),
              new ButtonBuilder().setCustomId(`decline:${matchId}`).setLabel('Recusar').setStyle(ButtonStyle.Danger)
            )
            const embed = new EmbedBuilder()
              .setTitle('Partida encontrada — Fila aberta!')
              .setColor(0x9B59B6)
              .setDescription(`O prazo para aceitar a fila termina em ${secLeft} segundo(s).`)
              .addFields({ name: 'Jogadores', value: details || '-' })
              .setThumbnail('attachment://lollogo.png')
              .setImage('attachment://background.png')
            await msg.edit({ embeds: [embed], components: [row] })
          }
          await interaction.reply({ content: 'Aceite registrado.', flags: MessageFlags.Ephemeral })
          try {
            const uids = Array.isArray(d.uids) ? d.uids : []
            const allAccepted = uids.length > 0 && uids.every(u => acceptMap[u] === 'accepted')
            if (allAccepted) {
              const t = d.times || {}
              const time1 = t.time1 || d.time1 || { nome: 'Time Azul', jogadores: [] }
              const time2 = t.time2 || d.time2 || { nome: 'Time Vermelho', jogadores: [] }
              const historicoId = d.historicoId || matchId
              const href = db.collection('historicoPartidas').doc(historicoId)
              const payload = {
                status: 'pending',
                createdAt: admin.firestore.FieldValue.serverTimestamp(),
                time1,
                time2,
                vencedor: 'Pendente',
                readyDocId: matchId,
                uids: Array.isArray(d.uids) ? d.uids : [],
                pontuacaoDiferenca: typeof d.pontuacaoDiferenca === 'number' ? d.pontuacaoDiferenca : undefined,
                isRandom: d.isRandom === true,
                data: typeof d.data === 'string' ? d.data : undefined
              }
              await href.set(payload, { merge: true })
              // remove todos aceitos da fila
              for (const u of uids) { try { await queueDoc(u).delete().catch(()=>{}) } catch {} }
              // fechar card e mover status
              try { await deleteReadyPrompt(matchId) } catch {}
              await mref.set({ status: 'confirmada', historicoId, confirmedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
              // aviso público
              try {
                const ch = await getQueueChannel()
                if (ch) {
                const embed = new EmbedBuilder().setTitle('Partida Pendente criada').addFields(
                  { name: 'match_id', value: `${historicoId}`, inline: true },
                  { name: 'Times', value: `${time1?.nome || 'Azul'} vs ${time2?.nome || 'Vermelho'}`, inline: true }
                  ).setColor(0x5865F2).setThumbnail('attachment://lollogo.png').setImage('attachment://background.png')
                  await sendTemporaryChannelMessage(ch, { embeds: [embed], files: brandAssets() }, 60*1000)
                }
              } catch {}
            }
          } catch {}
        } catch {}
      } else if (action === 'decline') {
        await mref.update({ [`playersReady.${userId}`]: false })
        if (uid) {
          await mref.update({ [`playerAcceptances.${uid}`]: 'declined' })
          await queueDoc(uid).delete().catch(() => {})
          const until = new Date(Date.now() + 15 * 60 * 1000)
          await userDoc(uid).set({ matchmakingBanUntil: admin.firestore.Timestamp.fromDate(until) }, { merge: true })
        }
        try {
          await mref.set({ status: 'cancelled', cancelledAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
          await deleteReadyPrompt(matchId)
          const others = Array.isArray(matchData.uids) ? matchData.uids.filter(u => u !== uid) : []
          // Devolver demais para a fila (garantir presença)
          for (const ouid of others) {
            const exists = await isInQueue(ouid)
            if (!exists) {
              const usnap = await userDoc(ouid).get()
              const ud = usnap.exists ? usnap.data() : {}
              const rank = await getRankingForUser(ud)
              const nomeBase = ud.playerName || ud.nome || ''
              const payload = userToQueueData(ouid, { ...ud, nome: nomeBase, elo: rank.elo, divisao: rank.divisao })
              await queueDoc(ouid).set(payload)
            }
          }
          await interaction.reply({ content: 'Você recusou. Partida cancelada e fila ajustada.', flags: MessageFlags.Ephemeral })
        } catch {}
      }
    }
    }
    if (interaction.isButton()) {
      const cid = interaction.customId || ''
      if (cid === 'resultado_send') {
        try {
          const cmds = await interaction.client.application.commands.fetch()
          const cmd = [...cmds.values()].find(c => c.name === 'resultado')
          if (cmd) {
            await interaction.reply({ content: `Clique para abrir o comando: </resultado:${cmd.id}>\nAnexe o print e informe o match_id copiado da página Player.`, flags: MessageFlags.Ephemeral })
          } else {
            await interaction.reply({ content: 'Use o comando /resultado, anexe o print e informe o match_id.', flags: MessageFlags.Ephemeral })
          }
        } catch {
          await interaction.reply({ content: 'Use o comando /resultado, anexe o print e informe o match_id.', flags: MessageFlags.Ephemeral })
        }
        return
      }
    }
  }
  catch (e) {
    console.error('Erro em interação', e)
    try {
      if (interaction.isRepliable()) {
        if (interaction.deferred || interaction.replied) {
          try { await interaction.editReply({ content: 'Ocorreu um erro.' }) } catch {}
        } else {
          try { await interaction.reply({ content: 'Ocorreu um erro.', flags: MessageFlags.Ephemeral }) } catch {}
        }
      }
    } catch {}
  }
})

const readyNotified = new Set()
const resultNotified = new Set()
const metrics = { readyChecksCreated: 0, dmsSent: 0, dmsFailed: 0, channelAnnouncements: 0, resultsAnnounced: 0 }

function playerList(docData) {
  if (Array.isArray(docData.jogadores)) return docData.jogadores
  if (Array.isArray(docData.players)) return docData.players
  return []
}

function playersFromTimes(data) {
  try {
    const t = data.times || {}
    const t1 = (t.time1 && t.time1.jogadores) ? t.time1.jogadores : (data.time1 && data.time1.jogadores ? data.time1.jogadores : [])
    const t2 = (t.time2 && t.time2.jogadores) ? t.time2.jogadores : (data.time2 && data.time2.jogadores ? data.time2.jogadores : [])
    return ([]).concat(t1 || [], t2 || [])
  } catch { return [] }
}

function userToQueueData(uid, d) {
  const baseNome = d.nome || d.playerName || d.apelido_conta || d.apelido || ''
  const nome = baseNome || uid
  const elo = d.elo || 'Ferro'
  const divisao = d.divisao || 'IV'
  const rolePrincipal = d.rolePrincipal || 'Preencher'
  const roleSecundaria = d.roleSecundaria || 'Preencher'
  const tag = d.tag ? (String(d.tag).startsWith('#') ? d.tag : `#${d.tag}`) : ''
  return {
    uid,
    nome,
    elo,
    divisao,
    rolePrincipal,
    roleSecundaria,
    tag,
    discordUserId: d.discordUserId || null,
    discordUsername: d.discordUsername || '',
    source: 'queue',
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  }
}

async function getRankingForUser(d) {
  try {
    if (d && d.uid) {
      const us = await userDoc(d.uid).get(); const ud = us.exists ? us.data() : {};
      return { elo: ud.elo || 'Ferro', divisao: ud.divisao || 'IV' };
    }
    const baseName = (d.nome || d.playerName || d.apelido_conta || '').toLowerCase().trim()
    if (!baseName) return { elo: d.elo || 'Ferro', divisao: d.divisao || 'IV' }
    const qNome = await db.collection('users').where('nome','==', baseName).limit(1).get()
    if (!qNome.empty) { const ud = qNome.docs[0].data() || {}; return { elo: ud.elo || 'Ferro', divisao: ud.divisao || 'IV' } }
    const qPlayer = await db.collection('users').where('playerName','==', baseName).limit(1).get()
    if (!qPlayer.empty) { const ud = qPlayer.docs[0].data() || {}; return { elo: ud.elo || 'Ferro', divisao: ud.divisao || 'IV' } }
    return { elo: d.elo || 'Ferro', divisao: d.divisao || 'IV' }
  } catch { return { elo: d.elo || 'Ferro', divisao: d.divisao || 'IV' } }
}

async function isInQueue(uid) {
  try {
    const snap = await queueDoc(uid).get()
    if (!snap.exists) return null
    return snap
  } catch { return null }
}

function formatPlayers(list) {
  if (!Array.isArray(list)) return '-' 
  return list
    .map((p) => {
      if (typeof p === 'string') return `<@${p}>`
      if (p && typeof p === 'object') {
        const id = p.discordUserId || p.id || p.userId
        const role = p.role || p.funcao || p.posicao
        return role ? `<@${id}> (${role})` : `<@${id}>`
      }
      return '-'
    })
    .join('\n')
}

// Formata jogadores como: Nome#TAG • Elo Divisão (Role) [• MVP]
function formatPlayersResult(list, mvpName, preferMention = false, guildId) {
  if (!Array.isArray(list)) return '-'
  const mvp = String(mvpName || '').trim().toLowerCase()
  return list
    .map((p) => {
      if (!p || typeof p !== 'object') return '-'
      const nome = String(p.nome || '').trim()
      const tag = String(p.tag || '').trim().replace(/^#/, '')
      const handle = tag ? `${nome}#${tag}` : nome
      const idStr = p.discordUserId ? String(p.discordUserId) : ''
      const isSnowflake = /^\d{17,20}$/.test(idStr)
      const mention = isSnowflake ? `<@${idStr}>` : ''
      const elo = p.elo || '-'
      const div = p.divisao || ''
      const lane = p.roleAtribuida || p.role || p.funcao || ''
      const isMvp = nome && nome.toLowerCase() === mvp
      const mvpBadge = isMvp ? ' • MVP' : ''
      const laneKey = normalizeLane(lane)
      const eloKey = normalizeElo(elo)
      const laneIcon = guildId ? emojiFor(guildId, laneKey) : ''
      const eloIcon = guildId ? emojiFor(guildId, eloKey) : ''
      const laneText = laneIcon || (lane ? ` (${lane})` : '')
      const left = preferMention ? (mention ? `${handle} ${mention}` : handle) : handle
      return `${eloIcon} ${laneIcon} ${left} • ${div}${mvpBadge}`.trim()
    })
    .join('\n')
}

function startOfYesterdayMs() {
  const now = new Date()
  const y = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1)
  return y.getTime()
}

function docCreatedMs(data) {
  const created = data.createdAt && data.createdAt.toMillis ? data.createdAt.toMillis() : null
  if (created) return created
  const ds = data.data || null
  if (ds) {
    const t = new Date(ds).getTime()
    if (!Number.isNaN(t)) return t
  }
  return null
}

async function hasAnnounced(type, id) {
  try { const snap = await db.collection('Notificacoes').doc(`${type}:${id}`).get(); return snap.exists } catch { return false }
}

async function markAnnounced(type, id) {
  try { await db.collection('Notificacoes').doc(`${type}:${id}`).set({ ts: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }) } catch {}
}

function aplicarXp(tier, divisao, xp, isWin) {
  const ELO_ORDER = ['Ferro','Bronze','Prata','Ouro','Platina','Esmeralda','Diamante','Mestre','Grão-Mestre','Desafiante']
  const DIV_ORDER = ['I','II','III','IV']
  const prataIdx = ELO_ORDER.indexOf('Prata')
  let ti = Math.max(0, ELO_ORDER.indexOf(String(tier||'Ferro')))
  let di = Math.max(0, DIV_ORDER.indexOf(String(divisao||'IV')))
  xp = Math.max(0, Number(xp||0)) + (isWin ? 30 : -30)
  while (xp >= 100) { xp -= 100; if (di > 0) di--; else if (ti < ELO_ORDER.length - 1) { ti++; di = DIV_ORDER.length - 1; } else { xp = Math.min(xp, 99); break; } }
  if (xp < 0) { if (ti <= prataIdx) { xp = 0; di = DIV_ORDER.length - 1; ti = prataIdx; } else { while (xp < 0 && ti > prataIdx) { xp += 100; if (di < DIV_ORDER.length - 1) di++; else { ti--; di = 0; } } xp = Math.max(0, xp) } }
  return { tier: ELO_ORDER[ti], divisao: DIV_ORDER[di], xp }
}

async function resolveDiscordIdByUid(uid) { try { const snap = await userDoc(uid).get(); const d = snap.exists ? snap.data() : {}; return d.discordUserId || null } catch { return null } }
async function getRankingState(uid) { try { const snap = await userDoc(uid).get(); const d = snap.exists ? snap.data() : {}; return { tier: d.elo || 'Ferro', divisao: d.divisao || 'IV', xp: d.xp || 0 } } catch { return { tier: 'Ferro', divisao: 'IV', xp: 0 } }
}

async function getNotificationPrefs(uid){
  try { const snap = await userDoc(uid).get(); const d = snap.exists ? snap.data() : {}; return {
    ready: d.notifyReadyCheck !== false,
    result: d.notifyResult !== false,
    mvp: d.notifyMvp !== false
  } } catch { return { ready: true, result: true, mvp: true } }
}

async function publishDiscordConfig() {
  const clientId = process.env.DISCORD_OAUTH_CLIENT_ID
  const redirectUri = process.env.DISCORD_OAUTH_REDIRECT_URI
  if (!clientId || !redirectUri) return
  try { await db.collection('config').doc('discord').set({ oauthClientId: clientId, oauthRedirectUri: redirectUri }, { merge: true }) } catch {}
}

  // 📌 Mensagens fixas de instruções nos canais de Fila e Vínculo
  async function ensurePinnedHelpMessages() {
    try {
      const queueCh = await getQueueChannel()
      const linkChId = process.env.DISCORD_LINK_CHANNEL_ID
      const linkCh = linkChId ? await client.channels.fetch(linkChId).catch(()=>null) : null
      const resultsChId = process.env.DISCORD_RESULTS_CHANNEL_ID || '1444182835975028848'
      const resultsCh = resultsChId ? await client.channels.fetch(resultsChId).catch(()=>null) : null

    async function ensurePinned(channel, content, marker, components, embed){
      if (!channel || channel.type !== ChannelType.GuildText) return
      try {
        const pins = await channel.messages.fetchPins().catch(()=>null)
        let mine = pins ? pins.find(m => m.author?.id === client.user?.id && String(m.content||'').startsWith(marker)) : null
        if (mine) {
          await mine.edit({ content, components: components ? [components] : [], embeds: embed ? [embed] : [] }).catch(()=>{})
        } else {
          const sent = await channel.send({ content, components: components ? [components] : [], embeds: embed ? [embed] : [] }).catch(()=>null)
          if (sent) { try { await sent.pin() } catch {} }
        }
      } catch {}
    }

    const queueMarker = '📌 Guia da Fila'
    const queueContent = '📌 Guia da Fila'
    const queueEmbed = new EmbedBuilder()
      .setTitle('📌 Guia da Fila — como usar o bot')
      .setColor(0x5865F2)
      .setDescription([
        '• Use "Entrar na Fila" para começar a procurar partidas. Se já estiver na fila, use "Sair da Fila".',
        '• Clique em "Perfil" para ver seu Elo, Divisão, roles e status da fila.',
        '• Ao montar uma partida, publicamos o Ready Check aqui com botões de Aceitar/Recusar.',
        '• O Resultado da partida será anunciado com link para o histórico.'
      ].join('\n'))
    const queueRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('queue_join').setLabel('Entrar na Fila').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('queue_leave').setLabel('Sair da Fila').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('perfil_open').setLabel('Perfil').setStyle(ButtonStyle.Secondary)
    )
    await ensurePinned(queueCh, queueContent, queueMarker, queueRow, queueEmbed)

    const linkMarker = '📌 Guia de Vínculo'
    const linkContent = '📌 Guia de Vínculo'
    const linkEmbed = new EmbedBuilder()
      .setTitle('📌 Guia de Vínculo — conectar seu Discord ao site')
      .setColor(0x57F287)
      .setDescription([
        '• Clique em "Vincular por UID" para receber instruções de vínculo via UID.',
        '• Clique em "Gerar Código de Vínculo" para criar um código e usar no site (Editar Perfil → Vincular pelo Código).',
        '• Após vincular, deslogue e logue novamente no site para carregar o Discord.',
        `• OAuth: ${process.env.DISCORD_OAUTH_REDIRECT_URI || '—'} (se habilitado).`
      ].join('\n'))
    const linkRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('linkuid_start').setLabel('Vincular por UID').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('linkcode_start').setLabel('Gerar Código de Vínculo').setStyle(ButtonStyle.Secondary)
    )
    await ensurePinned(linkCh, linkContent, linkMarker, linkRow, linkEmbed)

    const resultsMarker = '📌 Guia de Resultados'
    const resultsContent = '📌 Guia de Resultados'
    const resultsEmbed = new EmbedBuilder()
      .setTitle('📌 Guia de Resultados — enviar o print')
      .setColor(0x57F287)
      .setDescription([
        '• Copie o match_id na página Player → Histórico → Em andamento (use o botão Copiar UID da Partida).',
        '• No canal Resultados, use o comando /resultado com match_id e anexe o print.',
        '• Página Player: https://customdasestrelas.com.br/player.html/player.html'
      ].join('\n'))
    const resultsRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('resultado_send').setLabel('Enviar Resultado').setStyle(ButtonStyle.Primary)
    )
    await ensurePinned(resultsCh, resultsContent, resultsMarker, resultsRow, resultsEmbed)
    } catch {}
  }

async function sendReadyCheckNotifications(doc) {
  const data = doc.data()
  const createdMs = docCreatedMs(data)
  if (createdMs < startOfYesterdayMs()) return
  if (await hasAnnounced('ready', doc.id)) return
  const players = playersFromTimes(data).length ? playersFromTimes(data) : playerList(data)
  if (!players || players.length === 0) return
  const siteBase = sanitizeBaseUrl(process.env.SITE_BASE_URL || '')
  const guildId = process.env.DISCORD_GUILD_ID
  const guild = guildId ? await client.guilds.fetch(guildId).catch(() => null) : client.guilds.cache.first()
  const members = guild ? await guild.members.fetch().catch(() => null) : null
  function resolveIdByUsername(name) {
    if (!name || !members) return null
    const m = members.find((mm) => mm.user.username === name || mm.user.globalName === name)
    return m ? m.user.id : null
  }
  async function resolveIdByUid(uid) {
    if (!uid) return null
    try {
      const snap = await userDoc(uid).get()
      const d = snap.exists ? snap.data() : null
      return d && d.discordUserId ? d.discordUserId : null
    } catch { return null }
  }
  // DMs desativadas: apenas anúncio no canal público
  const channel = await getQueueChannel()
  // Canal de destino para Ready Check: agora é sempre o canal de queue
  const targetChannel = channel
  if (targetChannel) {
    try {
      const playersRaw = playersFromTimes(data).length ? playersFromTimes(data) : playerList(data)
      const players = []
      for (let p of playersRaw) {
        let id = p && typeof p === 'object' ? (p.discordUserId || p.id || p.userId || null) : null
        if (!id && p && typeof p === 'object' && p.uid) { id = await resolveDiscordIdByUid(p.uid) }
        let obj = typeof p === 'object' ? { ...p } : { nome: String(p||'') }
        obj.discordUserId = id || null
        const uidIt = obj.uid || null
        if (uidIt) {
          try { const us = await userDoc(uidIt).get(); if (us.exists) { const ud = us.data()||{}; obj.tag = ud.tag || obj.tag; obj.nome = obj.nome || ud.nome || ud.playerName; obj.elo = ud.elo || obj.elo; obj.divisao = ud.divisao || obj.divisao; obj.role = obj.role || ud.roleAtribuida || ud.rolePrincipal } } catch {}
        }
        players.push(obj)
      }
      await ensureGuildEmojisForChannel(targetChannel)
      const acceptMap = data.playerAcceptances || {}
      const details = players.map((p) => {
        const nome = String((typeof p === 'object' ? p.nome : p) || '').trim()
        const tag = String((typeof p === 'object' ? p.tag : '') || '').trim().replace(/^#/, '')
        const handle = tag ? `${nome}#${tag}` : nome
        const uid = typeof p === 'object' ? (p.uid || null) : null
        const accepted = uid && acceptMap[uid] === 'accepted'
        const mark = accepted ? '✅ ' : ''
        const elo = p.elo || '-'
        const div = p.divisao || ''
        const lane = p.roleAtribuida || p.role || p.funcao || ''
        const laneText = lane ? ` (${lane})` : ''
        return `${mark}${handle} • ${div}${laneText}`.trim()
      }).join('\n')
      const until = data.timestampFim && data.timestampFim.toDate ? data.timestampFim.toDate() : null
      const now = new Date()
      const msLeft = until ? Math.max(0, until.getTime() - now.getTime()) : 30 * 1000
      const secLeft = Math.ceil(msLeft / 1000)
      const embed = new EmbedBuilder()
        .setTitle('Partida encontrada — Fila aberta!')
        .setColor(0x9B59B6)
        .setDescription(`O prazo para aceitar a fila termina em ${secLeft} segundo(s).`)
        .addFields({ name: 'Jogadores', value: details || '-' })
        .setThumbnail('attachment://lollogo.png')
        .setImage('attachment://background.png')
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(`accept:${doc.id}`).setLabel('Aceitar').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(`decline:${doc.id}`).setLabel('Recusar').setStyle(ButtonStyle.Danger)
      )
      const sent = await targetChannel.send({ embeds: [embed], components: [row], files: brandAssets() })
      try { await doc.ref.set({ readyMessageId: sent.id, readyChannelId: targetChannel.id }, { merge: true }) } catch {}
      metrics.channelAnnouncements++
    } catch (e) {
      console.error('Falha ao enviar mensagem de canal:', e?.message || e)
    }
  }
  try {
    const untilTs = data.timestampFim && data.timestampFim.toMillis ? data.timestampFim.toMillis() : (Date.now() + 30*1000)
    scheduleReadyTimeout(doc.id, untilTs)
  } catch {}
  await markAnnounced('ready', doc.id)
}

async function deleteReadyPrompt(matchId) {
  try {
    const ref = db.collection('aguardandoPartidas').doc(matchId)
    const snap = await ref.get()
    if (!snap.exists) return
    const d = snap.data() || {}
    const chid = d.readyChannelId
    const mid = d.readyMessageId
    if (chid && mid) {
      try { const ch = await client.channels.fetch(chid).catch(()=>null); const msg = ch ? await ch.messages.fetch(mid).catch(()=>null) : null; if (msg && msg.deletable) await msg.delete().catch(()=>{}) } catch {}
    }
  } catch {}
}

const readyTimeouts = new Map()
function scheduleReadyTimeout(matchId, expireMs) {
  try {
    const delay = Math.max(0, expireMs - Date.now())
    if (readyTimeouts.has(matchId)) { clearTimeout(readyTimeouts.get(matchId)) }
    const to = setTimeout(async () => {
      try {
        const ref = db.collection('aguardandoPartidas').doc(matchId)
        const snap = await ref.get()
        if (!snap.exists) return
        const d = snap.data() || {}
        const st = d.status || 'readyCheck'
        if (!['readyCheck','pending','Aberta'].includes(st)) return
        // delete channel prompt
        try { await deleteReadyPrompt(matchId) } catch {}
        // Ajuste da fila conforme regra: remover indecisos, devolver aceitos
        const uids = Array.isArray(d.uids) ? d.uids : []
        const acc = d.playerAcceptances || {}
        const accepted = uids.filter(u => acc[u] === 'accepted')
        const undecided = uids.filter(u => !acc[u])
        try {
          for (const u of undecided) { await queueDoc(u).delete().catch(()=>{}) }
          for (const u of accepted) {
            const exists = await isInQueue(u)
            if (!exists) {
              const usnap = await userDoc(u).get()
              const ud = usnap.exists ? usnap.data() : {}
              const rank = await getRankingForUser(ud)
              const nomeBase = ud.playerName || ud.nome || ''
              const payload = userToQueueData(u, { ...ud, nome: nomeBase, elo: rank.elo, divisao: rank.divisao })
              await queueDoc(u).set(payload)
            }
          }
        } catch {}
        await ref.set({ status: 'timeout', timeoutAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
      } catch {}
    }, delay)
    readyTimeouts.set(matchId, to)
  } catch {}
}

async function sendFinalResult(doc) {
  const data = doc.data()
  if (!data || !data.vencedor) return
  const createdMs = docCreatedMs(data)
  if (createdMs < startOfYesterdayMs()) return
  if (await hasAnnounced('result', doc.id)) return
  const winner = data.vencedor
  const teams = data.teams || { blue: data.timeAzul || data.teamBlue || data.time1, red: data.timeVermelho || data.teamRed || data.time2 }
  const blue = teams?.blue?.jogadores || teams?.team1?.jogadores || teams?.blue || []
  const red = teams?.red?.jogadores || teams?.team2?.jogadores || teams?.red || []
  const channel = await getAnnounceChannel()
  if (channel) { try { await ensureGuildEmojisForChannel(channel) } catch {} }
  const gid = channel?.guild?.id
  const embed = new EmbedBuilder()
    .setTitle('Resultado da Partida')
    .addFields(
      { name: 'Time Vencedor', value: `${winner}` },
      { name: 'Time Azul', value: formatPlayersResult(blue, data.mvpResult?.time1, true, gid) || '-', inline: true },
      { name: 'Time Vermelho', value: formatPlayersResult(red, data.mvpResult?.time2, true, gid) || '-', inline: true }
    )
  const base = sanitizeBaseUrl(process.env.MATCH_HISTORY_BASE_URL || process.env.SITE_BASE_URL || '')
  let link = ''
  if (base) {
    link = base.includes('?') ? `${base}&matchId=${doc.id}` : `${base}?matchId=${doc.id}`
  }
  if (link) embed.setFooter({ text: `Histórico: ${link}` })
  if (channel) { await sendTemporaryChannelMessage(channel, { embeds: [embed] }, 60*1000) }
  metrics.resultsAnnounced++

  const winnerStr = String(winner||'').trim().toLowerCase()
  const blueName = (teams?.blue?.nome || 'Time Azul').toString().trim().toLowerCase()
  const redName = (teams?.red?.nome || 'Time Vermelho').toString().trim().toLowerCase()
  async function notifyTeam(list, isWin, teamLabel){
    if (!Array.isArray(list)) return
    for (const p of list) {
      let uid = typeof p === 'object' ? (p.uid || null) : null
      let discordId = typeof p === 'string' ? p : (p && typeof p === 'object' ? (p.discordUserId || p.id || p.userId) : null)
      if (!discordId && uid) discordId = await resolveDiscordIdByUid(uid)
      if (!discordId) continue
      const prefs = uid ? await getNotificationPrefs(uid) : { result: true }
      if (!prefs.result) continue
      try {
        const user = await client.users.fetch(discordId)
        const state = uid ? await getRankingState(uid) : { tier:'Ferro',divisao:'IV',xp:0 }
        const after = aplicarXp(state.tier, state.divisao, state.xp, !!isWin)
        const deltaXp = after.xp - state.xp
        const promoted = after.tier !== state.tier || after.divisao !== state.divisao
        const txt = isWin ? 'Vitória' : 'Derrota'
        const handle = (()=>{ const nome = String(p.nome||'').trim(); const tag = String(p.tag||'').trim().replace(/^#/,''); return tag ? `${nome}#${tag}` : nome })()
        const promoTxt = promoted
          ? (isWin ? `⬆️ Subiu para ${after.tier} ${after.divisao}` : `⬇️ Mudou para ${after.tier} ${after.divisao}`)
          : `↔️ Permanece em ${after.tier} ${after.divisao}`
        const mvpName1 = data.mvpResult?.time1 || ''
        const mvpName2 = data.mvpResult?.time2 || ''
        const isMvp = (String(p.nome||'').trim().toLowerCase() === String(mvpName1||'').trim().toLowerCase()) ||
                      (String(p.nome||'').trim().toLowerCase() === String(mvpName2||'').trim().toLowerCase())
        const mvpTxt = isMvp ? '\n🏆 Você foi o jogador mais honrado (MVP) nesta partida!' : ''
        const roleTxt = p.roleAtribuida || p.role || p.funcao ? ` • Role: ${p.roleAtribuida||p.role||p.funcao}` : ''
        const base = sanitizeBaseUrl(process.env.MATCH_HISTORY_BASE_URL || process.env.SITE_BASE_URL || '')
        const link = base ? (base.includes('?') ? `${base}&matchId=${doc.id}` : `${base}?matchId=${doc.id}`) : ''
        const embed = new EmbedBuilder()
          .setTitle(`Resultado: ${txt}`)
          .addFields(
            { name: 'Jogador', value: handle, inline: true },
            { name: 'Time', value: teamLabel, inline: true },
            { name: 'Role', value: (p.roleAtribuida||p.role||p.funcao||'-'), inline: true },
            { name: 'XP', value: `${state.xp} → ${after.xp} (${deltaXp>=0?'+':''}${deltaXp})` },
            { name: 'Elo', value: `${after.tier} ${after.divisao}` }
          )
        if (mvpTxt) embed.setFooter({ text: '🏆 MVP desta partida' })
        const row = link ? new ActionRowBuilder().addComponents(new ButtonBuilder().setLabel('Ver Histórico').setStyle(ButtonStyle.Link).setURL(link)) : null
        await sendTemporaryDmMessage(discordId, { embeds: [embed], components: row ? [row] : [] }, 60*1000)
      } catch {}
    }
  }
  const isBlueWin = winnerStr === blueName
  const isRedWin = winnerStr === redName
  await notifyTeam(blue, isBlueWin, 'Time Azul')
  await notifyTeam(red, isRedWin, 'Time Vermelho')
  // Atualiza a coleção 'users' com resultado e MVPs
  try {
    const mvp1 = String(data.mvpResult?.time1||'').trim().toLowerCase()
    const mvp2 = String(data.mvpResult?.time2||'').trim().toLowerCase()
    async function applyForList(list, isWin){
      if (!Array.isArray(list)) return
      for (const p of list) {
        if (!p || typeof p !== 'object') continue
        const nome = String(p.nome||'').trim()
        const isMvp = (!!mvp1 && mvp1 === nome.toLowerCase()) || (!!mvp2 && mvp2 === nome.toLowerCase())
        let ref = p.uid ? userDoc(p.uid) : null
        if (!ref && nome) {
          const q = await db.collection('users').where('nome','==', nome).limit(1).get().catch(()=>null)
          if (q && !q.empty) { ref = db.collection('users').doc(q.docs[0].id) }
          else {
            const q2 = await db.collection('users').where('playerName','==', nome).limit(1).get().catch(()=>null)
            if (q2 && !q2.empty) { ref = db.collection('users').doc(q2.docs[0].id) }
          }
        }
        if (!ref) continue
        const snap = await ref.get().catch(()=>null)
        const d = snap && snap.exists ? (snap.data()||{}) : {}
        const state = { tier: d.elo || 'Ferro', divisao: d.divisao || 'IV', xp: d.xp || 0 }
        const after = aplicarXp(state.tier, state.divisao, state.xp, !!isWin)
        const winsAfter = (d.totalVitorias || 0) + (isWin ? 1 : 0)
        const lossesAfter = (d.totalDerrotas || 0) + (isWin ? 0 : 1)
        const pontos = Math.max(0, (winsAfter * 2) - lossesAfter)
        const payload = {
          elo: after.tier,
          divisao: after.divisao,
          xp: after.xp,
          totalVitorias: admin.firestore.FieldValue.increment(isWin ? 1 : 0),
          totalDerrotas: admin.firestore.FieldValue.increment(isWin ? 0 : 1),
          pontuacao: pontos
        }
        if (isMvp) payload.mvpCount = admin.firestore.FieldValue.increment(1)
        await ref.set(payload, { merge: true }).catch(()=>{})
      }
    }
    await applyForList(blue, isBlueWin)
    await applyForList(red, isRedWin)
  } catch {}
  try {
    const all = [].concat(blue || [], red || [])
    for (const p of all) { if (p && typeof p === 'object' && p.uid) { await queueDoc(p.uid).delete().catch(()=>{}) } }
  } catch {}
  await markAnnounced('result', doc.id)
}

function setupMatchListeners() {
  const READY_WINDOW_MINUTES = parseInt(process.env.READY_WINDOW_MINUTES || '180', 10)
  const READY_LISTENER_LIMIT = parseInt(process.env.READY_LISTENER_LIMIT || '50', 10)
  const sinceTs = admin.firestore.Timestamp.fromMillis(Date.now() - READY_WINDOW_MINUTES * 60 * 1000)
  function handleSnapshot(snapshot) {
    snapshot.docChanges().forEach((change) => {
      const doc = change.doc
      const data = doc.data()
      if (!data) return
      if (change.type === 'added' && (data.status === 'readyCheck' || data.status === 'pending' || data.status === 'Aberta')) {
        const createdMs = docCreatedMs(data)
        const nowMs = Date.now()
        const windowMs = 10 * 60 * 1000
        if (createdMs >= startOfYesterdayMs() && nowMs - createdMs <= windowMs) {
          sendReadyCheckNotifications(doc)
        }
      }
      if ((change.type === 'modified' || change.type === 'added') && data.status && !['readyCheck','pending','Aberta'].includes(data.status)) {
        (async () => {
          const mid = data.readyMessageId
          const chid = data.readyChannelId
          if (mid && chid) {
            try { const ch = await client.channels.fetch(chid).catch(()=>null); const msg = ch ? await ch.messages.fetch(mid).catch(()=>null) : null; if (msg && msg.deletable) await msg.delete().catch(()=>{}) } catch {}
          }
          const dmMap = data.dmMessageIds || {}
          try {
            for (const [discordId, msgId] of Object.entries(dmMap)) {
              try { const user = await client.users.fetch(discordId); const dm = await user.createDM(); const m = await dm.messages.fetch(msgId).catch(()=>null); if (m && m.deletable) await m.delete().catch(()=>{}) } catch {}
            }
          } catch {}
        })()
      }
      if ((change.type === 'modified' || change.type === 'added') && data.vencedor) {
        sendFinalResult(doc)
      }
    })
  }
  try {
    const q0 = db.collection('aguardandoPartidas')
      .where('status', 'in', ['readyCheck','pending','Aberta'])
      .where('createdAt','>=', sinceTs)
      .orderBy('createdAt','desc')
      .limit(READY_LISTENER_LIMIT)
    q0.onSnapshot(handleSnapshot, () => {
      try {
        const q1 = db.collection('aguardandoPartidas')
          .where('createdAt','>=', sinceTs)
          .orderBy('createdAt','desc')
          .limit(READY_LISTENER_LIMIT)
        q1.onSnapshot(handleSnapshot)
      } catch {}
    })
  } catch {}
  const HISTORICO_LISTENER_LIMIT = parseInt(process.env.HISTORICO_LISTENER_LIMIT || '200', 10)
  db.collection('Historico')
    .where('createdAt', '>=', admin.firestore.Timestamp.fromMillis(startOfYesterdayMs()))
    .orderBy('createdAt','desc')
    .limit(HISTORICO_LISTENER_LIMIT)
    .onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const doc = change.doc
      const data = doc.data()
      if (!data) return
      const vRaw = data.vencedor
      const vencedor = typeof vRaw === 'string' ? vRaw.trim() : ''
      const vNorm = vencedor.toLowerCase()
      const hasWinner = !!vencedor && vNorm !== 'n/a' && vNorm !== 'pendente'
      if (change.type === 'added') {
        (async () => {
          const createdMs = docCreatedMs(data)
          if (!createdMs || createdMs < startOfYesterdayMs()) return
          if (!hasWinner) {
            if (await hasAnnounced('ongoing', doc.id)) return
            const teams = data.teams || { blue: data.timeAzul || data.teamBlue || data.time1, red: data.timeVermelho || data.teamRed || data.time2 }
            const blue = teams?.blue?.jogadores || teams?.team1?.jogadores || teams?.blue || []
            const red = teams?.red?.jogadores || teams?.team2?.jogadores || teams?.red || []
            const ch = await getQueueChannel()
            if (ch) {
              try {
                await ensureGuildEmojisForChannel(ch)
                const gid = ch.guild?.id
                const embed = new EmbedBuilder().setTitle('Partida em andamento').addFields(
                  { name: 'Time Azul', value: formatPlayersResult(blue, '', true, gid) || '-', inline: true },
                  { name: 'Time Vermelho', value: formatPlayersResult(red, '', true, gid) || '-', inline: true }
                )
                const sent = await ch.send({ embeds: [embed] })
                try { await doc.ref.set({ ongoingMessageId: sent.id, ongoingChannelId: ch.id }, { merge: true }) } catch {}
                await markAnnounced('ongoing', doc.id)
              } catch {}
            }
          } else { sendFinalResult(doc) }
        })()
      }
      // Remover mensagem de "Partida em andamento" quando finalizada/cancelada ou removida
      const st = String(data.status||'').toLowerCase()
      const shouldRemoveOngoing = hasWinner || (st && st !== 'pending')
      if ((change.type === 'modified' && shouldRemoveOngoing) || change.type === 'removed') {
        (async () => {
          try {
            const chid = data.ongoingChannelId
            const mid = data.ongoingMessageId
            if (chid && mid) {
              const ch = await client.channels.fetch(chid).catch(()=>null)
              const msg = ch ? await ch.messages.fetch(mid).catch(()=>null) : null
              if (msg && msg.deletable) await msg.delete().catch(()=>{})
              await doc.ref.set({ ongoingMessageId: admin.firestore.FieldValue.delete(), ongoingChannelId: admin.firestore.FieldValue.delete() }, { merge: true })
            }
          } catch {}
        })()
      }
      const hasMvp = !!(data.mvpResult && (data.mvpResult.time1 || data.mvpResult.time2))
      if ((change.type === 'modified' || change.type === 'added') && hasMvp) {
        sendMvpNotifications(doc)
      }
    })
  })
  setupQueueChannelListeners()
  setupLinkListeners()
}

async function setupQueueChannelListeners() {
  try {
    const ch = await getQueueChannel()
    if (!ch) return
    let messageId = null
    const DEBOUNCE_MS = Number(process.env.QUEUE_EDIT_DEBOUNCE_MS || '700')
    let pending = null
    let timer = null
    let rendering = false
    async function renderAndSend(snapshot) {
      const players = []
      snapshot.forEach((doc) => { const d = doc.data() || {}; players.push(d) })
      await ensureGuildEmojisForChannel(ch)
      const description = formatPlayersResult(players.slice(0,50), '', true, ch.guild?.id) || 'Nenhum jogador na fila.'
      const embed = new EmbedBuilder().setTitle('Jogadores na Fila').setColor(0x5865F2).setDescription(description).setThumbnail('attachment://lollogo.png').setImage('attachment://background.png')
      try {
        if (messageId) {
          const msg = await ch.messages.fetch(messageId).catch(()=>null)
          if (msg) { await msg.edit({ embeds: [embed] }) ; return }
        }
        const recent = await ch.messages.fetch({ limit: 10 }).catch(()=>null)
        if (recent) {
          const mine = recent.find(m => m.author?.id === client.user?.id && Array.isArray(m.embeds) && m.embeds[0]?.title === 'Jogadores na Fila')
          if (mine) { messageId = mine.id; await mine.edit({ embeds: [embed] }); return }
        }
        const sent = await ch.send({ embeds: [embed], files: brandAssets() })
        messageId = sent.id
      } catch {}
    }
    function schedule(snapshot){
      pending = snapshot
      if (timer) { clearTimeout(timer) }
      timer = setTimeout(async () => {
        if (!pending) return
        if (rendering) { schedule(pending); return }
        rendering = true
        const snap = pending
        pending = null
        await renderAndSend(snap)
        rendering = false
        if (pending) { schedule(pending) }
      }, DEBOUNCE_MS)
    }
    db.collection('queuee').orderBy('timestamp', 'desc').limit(50).onSnapshot((snapshot) => { schedule(snapshot) })
  } catch {}
}

function randomCode() { return String(Math.floor(100000 + Math.random()*900000)) }

async function createUniqueCode(discordId) {
  let code = randomCode()
  for (let i=0;i<10;i++) {
    const ref = db.collection('linkCodes').doc(code)
    const snap = await ref.get()
    if (!snap.exists) {
      const until = new Date(Date.now() + 10*60*1000)
      await ref.set({ discordId, createdAt: admin.firestore.FieldValue.serverTimestamp(), expiresAt: admin.firestore.Timestamp.fromDate(until), used: false })
      return code
    }
    code = randomCode()
  }
  return null
}

async function setupLinkListeners() {
  db.collection('linkRequests').where('createdAt', '>=', admin.firestore.Timestamp.fromMillis(Date.now() - 60 * 60 * 1000)).onSnapshot((snapshot) => {
    snapshot.docChanges().forEach(async (change) => {
      if (change.type !== 'added') return
      const doc = change.doc
      const data = doc.data() || {}
      const uid = String(data.uid||'').trim()
      const code = String(data.code||'').trim()
      if (!uid || !code) return
      try {
        const cref = db.collection('linkCodes').doc(code)
        const csnap = await cref.get()
        if (!csnap.exists) { await doc.ref.delete().catch(()=>{}); return }
        const cd = csnap.data() || {}
        const exp = cd.expiresAt && cd.expiresAt.toMillis ? cd.expiresAt.toMillis() : 0
        if (cd.used || (exp && Date.now() > exp)) { await doc.ref.delete().catch(()=>{}); return }
        const discordId = cd.discordId
        const uref = userDoc(uid)
        const usnap = await uref.get()
        if (!usnap.exists) { await doc.ref.delete().catch(()=>{}); return }
        const q = await db.collection('users').where('discordUserId','==',discordId).limit(1).get()
        if (!q.empty && q.docs[0].id !== uid) { await doc.ref.delete().catch(()=>{}); return }
        await uref.set({ discordUserId: discordId, discordUsername: discordId ? (await client.users.fetch(discordId)).username : '', discordLinkedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
        await cref.update({ used: true, usedAt: admin.firestore.FieldValue.serverTimestamp(), uid })
        await doc.ref.delete().catch(()=>{})
        try { const user = await client.users.fetch(discordId); await user.send({ content: 'Vínculo com o site concluído com sucesso.' }) } catch {}
      } catch {
        await doc.ref.delete().catch(()=>{})
      }
    })
  })
}

async function sendMvpNotifications(doc) {
  const data = doc.data() || {}
  const teams = data.teams || { blue: data.timeAzul || data.teamBlue || data.time1, red: data.timeVermelho || data.teamRed || data.time2 }
  const blue = teams?.blue?.jogadores || teams?.team1?.jogadores || teams?.blue || []
  const red = teams?.red?.jogadores || teams?.team2?.jogadores || teams?.red || []
  const name1 = data.mvpResult?.time1 || ''
  const name2 = data.mvpResult?.time2 || ''
  function findByName(list, name){ if (!Array.isArray(list)) return null; const n = String(name||'').toLowerCase(); return list.find(j => String((j&&j.nome)||'').toLowerCase() === n) || null }
  const p1 = findByName(blue, name1)
  const p2 = findByName(red, name2)
  async function notify(p){
    if (!p) return
    let uid = p.uid || null
    let discordId = p.discordUserId || p.id || p.userId || null
    if (!discordId && uid) discordId = await resolveDiscordIdByUid(uid)
    const prefs = uid ? await getNotificationPrefs(uid) : { mvp: true }
    if (!prefs.mvp) return
    if (!discordId) return
    try { const user = await client.users.fetch(discordId); await user.send({ content: 'Parabéns! Você foi eleito MVP nesta partida. +MVP' }) } catch {}
  }
  await notify(p1)
  await notify(p2)
  // Aplica MVPs na coleção 'users' (idempotente via notifications)
  try {
    if (await hasAnnounced('mvpApplied', doc.id)) return
    async function incMvp(p){
      if (!p || typeof p !== 'object') return
      const nome = String(p.nome||'').trim()
      let ref = p.uid ? userDoc(p.uid) : null
      if (!ref && nome) {
        const q = await db.collection('users').where('nome','==', nome).limit(1).get().catch(()=>null)
        if (q && !q.empty) { ref = db.collection('users').doc(q.docs[0].id) }
        else {
          const q2 = await db.collection('users').where('playerName','==', nome).limit(1).get().catch(()=>null)
          if (q2 && !q2.empty) { ref = db.collection('users').doc(q2.docs[0].id) }
        }
      }
      if (!ref) return
      await ref.set({ mvpCount: admin.firestore.FieldValue.increment(1) }, { merge: true }).catch(()=>{})
    }
    await incMvp(p1)
    await incMvp(p2)
    await markAnnounced('mvpApplied', doc.id)
  } catch {}
}

function startOAuthServer() {
  const port = Number(process.env.OAUTH_PORT || process.env.PORT || 5050)
  const clientId = process.env.DISCORD_OAUTH_CLIENT_ID
  const clientSecret = process.env.DISCORD_OAUTH_CLIENT_SECRET
  const redirectUri = process.env.DISCORD_OAUTH_REDIRECT_URI
  if (!clientId || !redirectUri || !clientSecret) return
  if (clientSecret.includes('.')) {
    console.error('DISCORD_OAUTH_CLIENT_SECRET inválido: parece um token de bot. Use o Client Secret da aba OAuth2 do Developer Portal.')
    return
  }
  console.log('OAuth server ativo. redirect_uri:', redirectUri)
  if (startOAuthServer._started) return
  startOAuthServer._started = true
  const server = http.createServer(async (req, res) => {
    try {
      const u = new URL(req.url, `http://localhost:${port}`)
      if (u.pathname === '/health') {
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ ok: true, metrics }))
      } else if (u.pathname === '/discord-oauth/callback' || u.pathname === '/discord_callback') {
        const code = u.searchParams.get('code')
        const stateUid = u.searchParams.get('state')
        if (!code || !stateUid) { res.statusCode = 400; res.end('invalid'); return }
        if (!clientSecret) { res.statusCode = 500; res.end('missing secret'); return }
        const tokenBody = new URLSearchParams({
          client_id: clientId,
          client_secret: clientSecret,
          grant_type: 'authorization_code',
          code,
          redirect_uri: redirectUri
        }).toString()
        const tReq = https.request({
          hostname: 'discord.com',
          path: '/api/oauth2/token',
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(tokenBody) }
        })
        const tokenPromise = new Promise((resolve, reject) => {
          tReq.on('response', (tRes) => {
            let data = ''
            tRes.on('data', (c) => (data += c))
            tRes.on('end', () => {
              try { resolve(JSON.parse(data)) } catch (e) { reject(e) }
            })
          })
          tReq.on('error', reject)
        })
        tReq.write(tokenBody)
        tReq.end()
        const tokenData = await tokenPromise
        const access = tokenData.access_token
        if (!access) {
          console.error('Falha OAuth: resposta sem access_token', tokenData && tokenData.error ? tokenData.error : tokenData)
          res.statusCode = 400
          res.end('no token')
          return
        }
        const mePromise = new Promise((resolve, reject) => {
          const mReq = https.request({
            hostname: 'discord.com',
            path: '/api/users/@me',
            method: 'GET',
            headers: { Authorization: `Bearer ${access}` }
          })
          mReq.on('response', (mRes) => {
            let body = ''
            mRes.on('data', (c) => (body += c))
            mRes.on('end', () => { try { resolve(JSON.parse(body)) } catch (e) { reject(e) } })
          })
          mReq.on('error', reject)
          mReq.end()
        })
        const me = await mePromise
        const id = me.id
        const username = me.username || me.global_name || ''
        const uref = userDoc(stateUid)
        const usnap = await uref.get()
        if (!usnap.exists) { res.statusCode = 400; res.end('invalid uid'); return }
        const q = await db.collection('users').where('discordUserId', '==', id).limit(1).get()
        if (!q.empty && q.docs[0].id !== stateUid) { res.statusCode = 409; res.end('already linked'); return }
        await uref.set({ discordUserId: id, discordUsername: username, discordLinkedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
        const base = sanitizeBaseUrl(process.env.SITE_BASE_URL || '')
        const redirect = joinBase(base, 'editarperfil.html?discord_linked=1')
        res.writeHead(302, { Location: redirect })
        res.end()
      } else {
        res.statusCode = 200
        res.end('ok')
      }
    } catch (e) {
      res.statusCode = 500
      res.end('error')
    }
  })
  server.on('error', (e) => { try { console.error('[oauth]', new Date().toISOString(), e && e.stack ? e.stack : e) } catch {} })
  try { server.listen(port, '0.0.0.0') } catch (e) { try { console.error('[oauth listen]', new Date().toISOString(), e && e.stack ? e.stack : e) } catch {} }
}

async function logGuildInfo() {
  try {
    for (const [gid, guild] of client.guilds.cache) {
      console.log('Guild:', guild.name, gid)
      const channels = await guild.channels.fetch()
      channels.forEach((c) => {
        if (c && c.type === ChannelType.GuildText) {
          console.log('Channel:', c.name, c.id)
        }
      })
    }
  } catch {}
}

client.login(token)
process.on('unhandledRejection', (e) => { try { console.error('[unhandledRejection]', new Date().toISOString(), e && e.stack ? e.stack : e) } catch {} })
process.on('uncaughtException', (e) => { try { console.error('[uncaughtException]', new Date().toISOString(), e && e.stack ? e.stack : e) } catch {} })
function sanitizeBaseUrl(s) {
  if (!s) return ''
  let v = String(s).trim()
  if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith('"') && v.endsWith('"')) || (v.startsWith('`') && v.endsWith('`'))) {
    v = v.slice(1, -1)
  }
  v = v.replace(/`/g, '')
  v = v.replace(/\s+/g, '')
  v = v.replace(/\/+$/, '')
  return v
}

function joinBase(base, path) {
  const b = sanitizeBaseUrl(base)
  const p = String(path || '').replace(/^\/+/, '')
  return b ? `${b}/${p}` : `/${p}`
}
