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
  ChannelType
} = require('discord.js')

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

async function ocrTextFromUrl(url) {
  try {
    const res = await Tesseract.recognize(url, 'eng', { logger: null })
    return res && res.data && res.data.text ? res.data.text : ''
  } catch { return '' }
}

async function getMatchPlayerNames(matchId){
  try {
    const ref = db.collection('historicoPartidas').doc(matchId)
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
    const ref = db.collection('historicoPartidas').doc(matchId)
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
  const hasVitoria = norm.includes('vitoria')
  const hasDerrota = norm.includes('derrota')
  if (!hasVitoria && !hasDerrota) return null
  const i1 = norm.indexOf('equipe 1')
  const i2 = norm.indexOf('equipe 2')
  const slice1 = i1 >= 0 ? norm.slice(i1, i1 + 800) : norm
  const slice2 = i2 >= 0 ? norm.slice(i2, i2 + 800) : norm
  function countMatches(slice, names){
    let c = 0
    for (const n of names) { const nn = normalizeName(n); if (nn && nn.length >= 3 && slice.includes(nn)) c++ }
    return c
  }
  const c1 = countMatches(slice1, team1Names)
  const c2 = countMatches(slice2, team2Names)
  if (c1 === 0 && c2 === 0) return null
  if (hasVitoria) return c1 >= c2 ? 'time1' : 'time2'
  if (hasDerrota) return c1 >= c2 ? 'time2' : 'time1'
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
  return db.collection('queue').doc(uid)
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

client.once('ready', async () => {
  try {
    await registerCommands()
  } catch (e) {
    console.error('Falha ao registrar comandos', e)
  }
  try { await publishDiscordConfig() } catch {}
  try { await ensurePinnedHelpMessages() } catch {}
  setupMatchListeners()
  startOAuthServer()
})

client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === 'fila') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode antes de usar a fila.', ephemeral: true }); return }
        const snap = await isInQueue(uid)
        const row = new ActionRowBuilder()
        const btnJoin = new ButtonBuilder().setCustomId(`queue_confirm_join:${uid}`).setLabel('Entrar na Fila').setStyle(ButtonStyle.Primary)
        const btnLeave = new ButtonBuilder().setCustomId(`queue_confirm_leave:${uid}`).setLabel('Sair da Fila').setStyle(ButtonStyle.Danger)
        if (!snap) {
          row.addComponents(btnJoin)
          await interaction.reply({ content: 'Você não está na fila. Deseja entrar?', components: [row], ephemeral: true })
          try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 30000) } catch {}
        } else {
          row.addComponents(btnLeave)
          await interaction.reply({ content: 'Você já está na fila. Deseja sair?', components: [row], ephemeral: true })
          try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 30000) } catch {}
        }
      }
      if (interaction.commandName === 'perfil') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver seu perfil.', ephemeral: true }); return }
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
            { name: 'Elo', value: `${rank.siteElo ?? '-'}`, inline: true },
            { name: 'Divisão', value: `${rank.siteDivisao ?? '-'}`, inline: true },
            { name: 'Role Principal', value: `${data.rolePrincipal ?? '-'}`, inline: true },
            { name: 'Role Secundária', value: `${data.roleSecundaria ?? '-'}`, inline: true },
            { name: 'Status da Fila', value: inQueue ? 'Na fila' : 'Fora da fila', inline: true }
          )
        await interaction.reply({ embeds: [embed], ephemeral: true })
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
          if (!q.empty && q.docs[0].id !== uid) { await interaction.reply({ content: 'Seu Discord já está vinculado a outro UID.', ephemeral: true }); return }
          await ref.set({ discordUserId: discordId, discordUsername: interaction.user.username, discordLinkedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
          await interaction.reply({ content: 'Vínculo com o site concluído com sucesso. Deslogue da sua conta e logue novamente.', ephemeral: true })
        } catch (e) {
          await interaction.reply({ content: 'Falha ao vincular. Tente novamente mais tarde.', ephemeral: true })
        }
      }
      if (interaction.commandName === 'linkcode') {
        try {
          const discordId = interaction.user.id
          const code = await createUniqueCode(discordId)
          if (!code) { await interaction.reply({ content: 'Não foi possível gerar código. Tente novamente.', ephemeral: true }); return }
          await interaction.reply({ content: `Seu código: ${code}. Use-o em Editar Perfil → Vincular pelo Código. Expira em 10 minutos.`, ephemeral: true })
          try { const user = await client.users.fetch(discordId); await user.send({ content: `Código de vínculo: ${code}. Expira em 10 minutos.` }) } catch {}
        } catch {
          await interaction.reply({ content: 'Falha ao gerar código.', ephemeral: true })
        }
      }
      if (interaction.commandName === 'pendente') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver pendências.', ephemeral: true }); return }
        const col = db.collection('aguardandoPartidas')
        const qsnap = await col.where('status','==','readyCheck').where('uids','array-contains', uid).orderBy('createdAt','desc').limit(1).get().catch(()=>null)
        if (!qsnap || qsnap.empty) { await interaction.reply({ content: 'Nenhuma partida pendente.', ephemeral: true }); return }
        const doc = qsnap.docs[0]
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
        await interaction.reply({ embeds: [embed], components: [row], ephemeral: true })
      }
      
      if (interaction.commandName === 'cleanupready') {
        const age = interaction.options.getInteger('age')
        const all = interaction.options.getBoolean('all')
        let count = 0
        const col = db.collection('aguardandoPartidas')
        const qsnap = await col.get()
        const limitMs = age ? Date.now() - age * 60 * 1000 : 0
        for (const doc of qsnap.docs) {
          const d = doc.data() || {}
          if (d.status !== 'readyCheck') continue
          if (all) { await doc.ref.delete(); count++; continue }
          const createdMs = d.createdAt && d.createdAt.toMillis ? d.createdAt.toMillis() : 0
          if (!limitMs || (createdMs && createdMs < limitMs)) { await doc.ref.delete(); count++ }
        }
        await interaction.reply({ content: `Ready Checks apagados: ${count}`, ephemeral: true })
      }
      if (interaction.commandName === 'readylist') {
        const qsnap = await db.collection('aguardandoPartidas').get()
        const lines = []
        qsnap.forEach((doc) => {
          const d = doc.data() || {}
          if (d.status === 'readyCheck') {
            const t = d.createdAt && d.createdAt.toDate ? d.createdAt.toDate().toLocaleString() : 'sem data'
            lines.push(`${doc.id} - ${t}`)
          }
        })
        await interaction.reply({ content: lines.slice(0, 50).join('\n') || 'Nenhum Ready Check.', ephemeral: true })
      }
      if (interaction.commandName === 'clearqueue') {
        const discordId = interaction.user.id
        const uid = await resolveUidByDiscordId(discordId)
        if (!uid) { await interaction.reply({ content: 'Nenhum vínculo encontrado. Use /link primeiro.', ephemeral: true }); return }
        await queueDoc(uid).delete().catch(() => {})
        await interaction.reply({ content: 'Sua entrada na fila foi removida.', ephemeral: true })
      }
      if (interaction.commandName === 'maketestmatch') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await interaction.reply({ content: 'Use este comando no canal de Resultados.', ephemeral: true }); return }
        const id = `test_${Date.now()}`
        const ref = db.collection('historicoPartidas').doc(id)
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
          await interaction.reply({ content: `Partida de teste criada. match_id: ${id}`, ephemeral: true })
          try {
            const pub = new EmbedBuilder().setTitle('Partida de Teste Criada').addFields(
              { name: 'match_id', value: id, inline: true },
              { name: 'Time 1', value: nomes1.join(', '), inline: true },
              { name: 'Time 2', value: nomes2.join(', '), inline: true }
            ).setColor(0x5865F2)
            await interaction.channel.send({ embeds: [pub] })
          } catch {}
        } catch (e) {
          await interaction.reply({ content: 'Falha ao criar partida de teste.', ephemeral: true })
        }
      }
      if (interaction.commandName === 'resultado') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await interaction.reply({ content: 'Use este comando no canal de Resultados.', ephemeral: true }); return }
        const matchId = interaction.options.getString('match_id')
        const att = interaction.options.getAttachment('imagem')
        if (!matchId) { await interaction.reply({ content: 'Informe o match_id.', ephemeral: true }); return }
        if (!att || !att.url) { await interaction.reply({ content: 'Anexe o print da partida (imagem).', ephemeral: true }); return }
        try {
          const ref = db.collection('historicoPartidas').doc(matchId)
          const snap = await ref.get()
          if (!snap.exists) { await interaction.reply({ content: 'Partida não encontrada.', ephemeral: true }); return }
          const d0 = snap.data() || {}
          if (d0.vencedor && d0.vencedor !== 'N/A') { await interaction.reply({ content: `Partida já possui vencedor (${d0.vencedor}). Use /corrigirresultado para alterar.`, ephemeral: true }); return }
          const teams = await getMatchTeamNames(matchId)
          const text = await ocrTextFromUrl(att.url)
          const textNorm = normalizeText(text)
          let matched = []
          for (const n of teams.t1.concat(teams.t2)) { const nn = normalizeName(n); if (nn && nn.length >= 3 && textNorm.includes(nn)) matched.push(n) }
          const uniqueMatched = Array.from(new Set(matched))
          if (uniqueMatched.length < 6) {
            await interaction.reply({ content: `Não foi possível validar o print: apenas ${uniqueMatched.length} jogador(es) conferem com a partida. É necessário coincidir pelo menos 6.`, ephemeral: true })
            return
          }
          const winnerSide = detectWinnerFromText(text, teams.t1, teams.t2)
          if (!winnerSide) { await interaction.reply({ content: 'Não foi possível detectar VITÓRIA/DERROTA ou vincular ao lado. Envie um print da tela de fim de jogo com "VITÓRIA/DERROTA" visível.', ephemeral: true }); return }
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
          await ref.set(payload, { merge: true })
          await interaction.reply({ content: `Resultado registrado: ${vencedor} (match_id: ${matchId}).`, ephemeral: true })
          try {
            const pub = new EmbedBuilder()
              .setTitle('Resultado de Partida')
              .addFields(
                { name: 'match_id', value: `${matchId}`, inline: true },
                { name: 'Vencedor', value: `${vencedor}`, inline: true },
                { name: 'Validados', value: `${uniqueMatched.length} jogadores`, inline: true }
              )
              .setImage(att.url)
              .setColor(0x57F287)
            await interaction.channel.send({ embeds: [pub] })
          } catch {}
        } catch (e) {
          await interaction.reply({ content: 'Falha ao registrar resultado.', ephemeral: true })
        }
      }
      if (interaction.commandName === 'corrigirresultado') {
        const channelOk = String(interaction.channelId||'') === RESULTS_CHANNEL_ID
        if (!channelOk) { await interaction.reply({ content: 'Use este comando no canal de Resultados.', ephemeral: true }); return }
        const matchId = interaction.options.getString('match_id')
        const vencedorOpt = interaction.options.getString('vencedor')
        const motivo = interaction.options.getString('motivo') || ''
        const att = interaction.options.getAttachment('imagem')
        if (!matchId || !vencedorOpt) { await interaction.reply({ content: 'Parâmetros inválidos.', ephemeral: true }); return }
        if (!att || !att.url) { await interaction.reply({ content: 'Anexe o print da partida (imagem).', ephemeral: true }); return }
        try {
          const ref = db.collection('historicoPartidas').doc(matchId)
          const snap = await ref.get()
          if (!snap.exists) { await interaction.reply({ content: 'Partida não encontrada.', ephemeral: true }); return }
          const teams = await getMatchTeamNames(matchId)
          const text = await ocrTextFromUrl(att.url)
          const textNorm = normalizeText(text)
          let matched = []
          for (const n of teams.t1.concat(teams.t2)) { const nn = normalizeName(n); if (nn && nn.length >= 3 && textNorm.includes(nn)) matched.push(n) }
          const uniqueMatched = Array.from(new Set(matched))
          if (uniqueMatched.length < 6) { await interaction.reply({ content: `Não foi possível validar o print: apenas ${uniqueMatched.length} jogador(es) conferem com a partida. É necessário coincidir pelo menos 6.`, ephemeral: true }); return }
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
          await ref.set(payload, { merge: true })
          await interaction.reply({ content: `Resultado corrigido para ${vencedor} (match_id: ${matchId}).`, ephemeral: true })
          try {
            const pub = new EmbedBuilder()
              .setTitle('Correção de Resultado')
              .addFields(
                { name: 'match_id', value: `${matchId}`, inline: true },
                { name: 'Vencedor', value: `${vencedor}`, inline: true },
                { name: 'Motivo', value: motivo || '—', inline: true },
                { name: 'Validados', value: `${uniqueMatched.length} jogadores`, inline: true }
              )
              .setImage(att.url)
              .setColor(0xFEE75C)
            await interaction.channel.send({ embeds: [pub] })
          } catch {}
        } catch (e) {
          await interaction.reply({ content: 'Falha ao corrigir resultado.', ephemeral: true })
        }
      }
      
      if (interaction.commandName === 'channels') {
        const guildId = process.env.DISCORD_GUILD_ID
        const guild = guildId ? await client.guilds.fetch(guildId).catch(() => null) : client.guilds.cache.first()
        if (!guild) { await interaction.reply({ content: 'Guild não encontrada.', ephemeral: true }); return }
        const channels = await guild.channels.fetch()
        const lines = []
        channels.forEach((c) => { if (c && c.type === ChannelType.GuildText) lines.push(`${c.name} ${c.id}`) })
        const out = lines.slice(0, 50).join('\n')
        await interaction.reply({ content: out || 'Sem canais de texto.', ephemeral: true })
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
      if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode para ver seu perfil.', ephemeral: true }); return }
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
          { name: 'Elo', value: `${rank.siteElo ?? '-'}`, inline: true },
          { name: 'Divisão', value: `${rank.siteDivisao ?? '-'}`, inline: true },
          { name: 'Role Principal', value: `${data.rolePrincipal ?? '-'}`, inline: true },
          { name: 'Role Secundária', value: `${data.roleSecundaria ?? '-'}`, inline: true },
          { name: 'Status da Fila', value: inQueue ? 'Na fila' : 'Fora da fila', inline: true }
        )
      await interaction.reply({ embeds: [embed], ephemeral: true })
      return
    }
    if ((action === 'accept' || action === 'decline') && targetUserId && targetUserId !== userId) {
      await interaction.reply({ content: 'Este botão não é para você.', ephemeral: true });
      return
    }
    if (action === 'linkuid_start') {
      const txt = 'Para vincular via UID: Abra Editar Perfil no site, copie seu UID e use o comando /linkuid uid:<seu UID> aqui no Discord.'
      await interaction.reply({ content: txt, ephemeral: true }); return
    }
    if (action === 'linkcode_start') {
      const code = await createUniqueCode(userId)
      if (!code) { await interaction.reply({ content: 'Não foi possível gerar código. Tente novamente.', ephemeral: true }); return }
      await interaction.reply({ content: `Seu código: ${code}. Use-o em Editar Perfil → Vincular pelo Código. Expira em 10 minutos.`, ephemeral: true }); return
    }
    if (action === 'queue_join') {
      if (!uid) { await interaction.reply({ content: 'Vincule seu Discord ao site com /linkuid ou /linkcode antes de entrar na fila.', ephemeral: true }); return }
      const existing = await isInQueue(uid)
      if (existing) {
        const msg = [
          'Você já está na fila.',
          '• Para sair, clique em "Sair da Fila" ou use o comando /fila.'
        ].join('\n')
        await interaction.reply({ content: msg, ephemeral: true })
        return
      }
      const uref = userDoc(uid)
      const usnap = await uref.get()
      const data = usnap.exists ? usnap.data() : {}
      const rank = await getRankingForUser(data)
      const nomeBase = data.playerName || data.nome || (interaction.member && interaction.member.displayName) || interaction.user.username
      const payload = userToQueueData(uid, { ...data, nome: nomeBase, siteElo: rank.siteElo, siteDivisao: rank.siteDivisao })
      await db.collection('queue').doc(uid).set(payload)
      const msg = [
        'Você entrou na fila com sucesso.',
        '• Você receberá um Ready Check quando uma partida for montada.',
        '• Para sair, clique em "Sair da Fila" ou use o comando /fila.'
      ].join('\n')
      await interaction.reply({ content: msg, ephemeral: true })
      return
    }
    if (action === 'queue_confirm_join') {
      const targetUid = matchId || uid
      if (!targetUid) { await interaction.reply({ content: 'Vincule seu Discord primeiro.', ephemeral: true }); return }
      const uref = userDoc(targetUid)
      const usnap = await uref.get()
      const data = usnap.exists ? usnap.data() : {}
      const existing = await isInQueue(targetUid)
      if (!existing) {
      const rank = await getRankingForUser(data)
      const nomeBase = data.playerName || data.nome || (interaction.member && interaction.member.displayName) || interaction.user.username
      const payload = userToQueueData(targetUid, { ...data, nome: nomeBase, siteElo: rank.siteElo, siteDivisao: rank.siteDivisao })
        await db.collection('queue').doc(targetUid).set(payload)
        await interaction.reply({ content: 'Você entrou na fila!', ephemeral: true })
        try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 10000) } catch {}
      } else {
        await interaction.reply({ content: 'Você já está na fila.', ephemeral: true })
        try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 10000) } catch {}
      }
      return
    }
    if (action === 'queue_confirm_leave') {
      const targetUid = matchId || uid
      if (!targetUid) { await interaction.reply({ content: 'Vincule seu Discord primeiro.', ephemeral: true }); return }
      try {
        const qsnap = await db.collection('queue').where('uid','==',targetUid).get()
        const dels = []
        qsnap.forEach(doc=> dels.push(doc.ref.delete()))
        await Promise.all(dels)
      } catch {}
      await interaction.reply({ content: 'Você saiu da fila.', ephemeral: true })
      try { setTimeout(()=>{ interaction.deleteReply().catch(()=>{}) }, 10000) } catch {}
      return
    }
    if (action === 'queue_leave') {
      if (!uid) { await interaction.reply({ content: 'Nenhum vínculo encontrado. Use /linkuid ou /linkcode primeiro.', ephemeral: true }); return }
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
      await interaction.reply({ content: msg, ephemeral: true }); return
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
      if (!msnap.exists) { await interaction.reply({ content: 'Partida não encontrada ou expirada.', ephemeral: true }); return }
      const matchData = msnap.data() || {}
      if (uid && Array.isArray(matchData.uids) && !matchData.uids.includes(uid)) { await interaction.reply({ content: 'Você não está nesta partida.', ephemeral: true }); return }
      await mref.set({ playersReady: msnap.data().playersReady || {}, playerAcceptances: msnap.data().playerAcceptances || {} }, { merge: true })
      if (action === 'accept') {
        await mref.update({ [`playersReady.${userId}`]: true })
        if (uid) { await mref.update({ [`playerAcceptances.${uid}`]: 'accepted' }) }
        await interaction.reply({ content: 'Você aceitou a partida.', ephemeral: true })
        try { if (interaction.message && interaction.message.deletable) await interaction.message.delete().catch(()=>{}) } catch {}
      } else if (action === 'decline') {
        await mref.update({ [`playersReady.${userId}`]: false })
        if (uid) {
          await mref.update({ [`playerAcceptances.${uid}`]: 'declined' })
          await queueDoc(uid).delete().catch(() => {})
          const until = new Date(Date.now() + 15 * 60 * 1000)
          await userDoc(uid).set({ matchmakingBanUntil: admin.firestore.Timestamp.fromDate(until) }, { merge: true })
        }
        await interaction.reply({ content: 'Você recusou a partida.', ephemeral: true })
        try { if (interaction.message && interaction.message.deletable) await interaction.message.delete().catch(()=>{}) } catch {}
    }
  }
  }
  }
  catch (e) {
    console.error('Erro em interação', e)
    if (interaction.isRepliable()) {
      try {
        await interaction.reply({ content: 'Ocorreu um erro.', ephemeral: true })
      } catch {}
    }
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
  const nome = d.nome || d.playerName || ''
  const elo = d.siteElo || d.elo || 'Ferro'
  const divisao = d.siteDivisao || d.divisao || 'IV'
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
    source: 'queue',
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  }
}

async function getRankingForUser(d) {
  try {
    const baseName = (d.nome || d.playerName || d.apelido_conta || '').toLowerCase().trim()
    if (!baseName) return { siteElo: d.siteElo || d.elo || 'Ferro', siteDivisao: d.siteDivisao || d.divisao || 'IV' }
    const rsnap = await db.collection('ranking').doc(baseName).get()
    if (!rsnap.exists) return { siteElo: d.siteElo || d.elo || 'Ferro', siteDivisao: d.siteDivisao || d.divisao || 'IV' }
    const r = rsnap.data() || {}
    return { siteElo: r.siteElo || d.siteElo || d.elo || 'Ferro', siteDivisao: r.siteDivisao || d.siteDivisao || d.divisao || 'IV' }
  } catch { return { siteElo: d.siteElo || d.elo || 'Ferro', siteDivisao: d.siteDivisao || d.divisao || 'IV' } }
}

async function isInQueue(uid) {
  try {
    const qsnap = await db.collection('queue').where('uid','==',uid).limit(1).get()
    if (qsnap.empty) return null
    return qsnap.docs[0]
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
function formatPlayersResult(list, mvpName) {
  if (!Array.isArray(list)) return '-'
  const mvp = String(mvpName || '').trim().toLowerCase()
  return list
    .map((p) => {
      if (!p || typeof p !== 'object') return '-'
      const nome = String(p.nome || '').trim()
      const tag = String(p.tag || '').trim().replace(/^#/, '')
      const handle = tag ? `${nome}#${tag}` : nome
      const elo = p.siteElo || p.elo || '-'
      const div = p.siteDivisao || p.divisao || ''
      const lane = p.roleAtribuida || p.role || p.funcao || ''
      const isMvp = nome && nome.toLowerCase() === mvp
      const mvpBadge = isMvp ? ' • MVP' : ''
      const laneText = lane ? ` (${lane})` : ''
      return `${handle} • ${elo} ${div}${laneText}${mvpBadge}`
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
  try { const snap = await db.collection('notifications').doc(`${type}:${id}`).get(); return snap.exists } catch { return false }
}

async function markAnnounced(type, id) {
  try { await db.collection('notifications').doc(`${type}:${id}`).set({ ts: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }) } catch {}
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
async function getRankingState(uid) { try { const snap = await userDoc(uid).get(); const d = snap.exists ? snap.data() : {}; return { tier: d.siteElo || d.elo || 'Ferro', divisao: d.siteDivisao || d.divisao || 'IV', xp: d.siteXP || 0 } } catch { return { tier: 'Ferro', divisao: 'IV', xp: 0 } }
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

    async function ensurePinned(channel, content, marker, components, embed){
      if (!channel || channel.type !== ChannelType.GuildText) return
      try {
        const pins = await channel.messages.fetchPinned().catch(()=>null)
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
        '• Ao montar uma partida, você receberá uma DM com botões para Aceitar/Recusar. Também publicamos o Ready Check aqui.',
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
  for (const p of players) {
    let id = typeof p === 'string' ? p : p.discordUserId || p.id || p.userId
    if (!id && typeof p === 'object' && p.uid) {
      id = await resolveIdByUid(p.uid)
    }
    let uid = typeof p === 'object' ? (p.uid || null) : null
    if (!uid && id) { const resolved = await resolveUidByDiscordId(id); uid = resolved || null }
    const prefs = uid ? await getNotificationPrefs(uid) : { ready: true }
    if (!prefs.ready) continue
    if (!id && typeof p === 'object') {
      if (p.discordUsername) id = resolveIdByUsername(p.discordUsername)
      if (!id && p.discordGlobalName) id = resolveIdByUsername(p.discordGlobalName)
    }
    if (!id) continue
    try {
      const user = await client.users.fetch(id)
      const details = formatPlayersResult(players, '')
      const rowForUser = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(`accept:${doc.id}:${id}`).setLabel('Aceitar Partida').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(`decline:${doc.id}:${id}`).setLabel('Recusar Partida').setStyle(ButtonStyle.Danger)
      )
      await user.send({ content: `READY CHECK! Sua partida foi encontrada!\n${details}\n${joinBase(siteBase, 'queue.html')}` , components: [rowForUser] })
      metrics.dmsSent++
    } catch (e) { metrics.dmsFailed++ }
  }
  const channel = await getQueueChannel()
  // Canal de destino para Ready Check: prioriza `DISCORD_READY_CHANNEL_ID`
  const readyChannel = await getReadyChannel()
  const targetChannel = readyChannel || channel
  if (targetChannel) {
    const playersStr = formatPlayersResult(players, '')
    try {
      await targetChannel.send({ content: `Ready Check iniciado! ${playersStr}` })
      metrics.channelAnnouncements++
    } catch (e) {
      console.error('Falha ao enviar mensagem de canal:', e?.message || e)
    }
  }
  await markAnnounced('ready', doc.id)
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
  const embed = new EmbedBuilder()
    .setTitle('Resultado da Partida')
    .addFields(
      { name: 'Time Vencedor', value: `${winner}` },
      { name: 'Time Azul', value: formatPlayersResult(blue, data.mvpResult?.time1) || '-', inline: true },
      { name: 'Time Vermelho', value: formatPlayersResult(red, data.mvpResult?.time2) || '-', inline: true }
    )
  const base = sanitizeBaseUrl(process.env.MATCH_HISTORY_BASE_URL || process.env.SITE_BASE_URL || '')
  let link = ''
  if (base) {
    link = base.includes('?') ? `${base}&matchId=${doc.id}` : `${base}?matchId=${doc.id}`
  }
  if (link) embed.setFooter({ text: `Histórico: ${link}` })
  const channel = await getAnnounceChannel()
  if (channel) {
    await channel.send({ embeds: [embed] })
  }
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
        await user.send({ embeds: [embed], components: row ? [row] : [] })
      } catch {}
    }
  }
  const isBlueWin = winnerStr === blueName
  const isRedWin = winnerStr === redName
  await notifyTeam(blue, isBlueWin, 'Time Azul')
  await notifyTeam(red, isRedWin, 'Time Vermelho')
  try {
    const all = [].concat(blue || [], red || [])
    for (const p of all) { if (p && typeof p === 'object' && p.uid) { await queueDoc(p.uid).delete().catch(()=>{}) } }
  } catch {}
  await markAnnounced('result', doc.id)
}

function setupMatchListeners() {
  db.collection('aguardandoPartidas').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const doc = change.doc
      const data = doc.data()
      if (!data) return
      if (change.type === 'added' && (data.status === 'readyCheck' || data.status === 'pending')) {
        const createdMs = docCreatedMs(data)
        const nowMs = Date.now()
        const windowMs = 10 * 60 * 1000
        if (createdMs >= startOfYesterdayMs() && nowMs - createdMs <= windowMs) {
          sendReadyCheckNotifications(doc)
        }
      }
      if ((change.type === 'modified' || change.type === 'added') && data.vencedor) {
        sendFinalResult(doc)
      }
    })
  })
  db.collection('historicoPartidas').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const doc = change.doc
      const data = doc.data()
      if (!data) return
      const vRaw = data.vencedor
      const vencedor = typeof vRaw === 'string' ? vRaw.trim() : ''
      const vNorm = vencedor.toLowerCase()
      const hasWinner = !!vencedor && vNorm !== 'n/a' && vNorm !== 'pendente'
      if (change.type === 'added' || change.type === 'modified') {
        (async () => {
          const createdMs = docCreatedMs(data)
          if (!createdMs || createdMs < startOfYesterdayMs()) return
          if (!hasWinner) {
            const teams = data.teams || { blue: data.timeAzul || data.teamBlue || data.time1, red: data.timeVermelho || data.teamRed || data.time2 }
            const blue = teams?.blue?.jogadores || teams?.team1?.jogadores || teams?.blue || []
            const red = teams?.red?.jogadores || teams?.team2?.jogadores || teams?.red || []
            const embed = new EmbedBuilder().setTitle('Partida em andamento').addFields(
              { name: 'Time Azul', value: formatPlayersResult(blue, '') || '-', inline: true },
              { name: 'Time Vermelho', value: formatPlayersResult(red, '') || '-', inline: true }
            )
            const ch = await getOngoingChannel()
            if (ch) { try { await ch.send({ embeds: [embed] }) } catch {} }
          } else {
            sendFinalResult(doc)
          }
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
    async function renderAndSend(snapshot) {
      const players = []
      snapshot.forEach((doc) => { const d = doc.data() || {}; players.push(d) })
      const lines = players.map((p) => {
        const name = p.nome || '-'
        const elo = p.elo || '-'
        const div = p.divisao || ''
        const role = p.rolePrincipal || p.role || ''
        return `${name} • ${elo} ${div} ${role ? '('+role+')' : ''}`
      })
      const content = `Jogadores na fila:\n${lines.slice(0, 50).join('\n') || 'Nenhum jogador na fila.'}`
      try {
        if (messageId) {
          const msg = await ch.messages.fetch(messageId).catch(()=>null)
          if (msg) { await msg.edit({ content }) ; return }
        }
        const recent = await ch.messages.fetch({ limit: 10 }).catch(()=>null)
        if (recent) {
          const mine = recent.find(m => m.author?.id === client.user?.id && String(m.content||'').startsWith('Jogadores na fila:'))
          if (mine) { messageId = mine.id; await mine.edit({ content }); return }
        }
        const sent = await ch.send({ content })
        messageId = sent.id
      } catch {}
    }
    db.collection('queue').onSnapshot((snapshot) => { renderAndSend(snapshot) })
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
  db.collection('linkRequests').onSnapshot((snapshot) => {
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
}

function startOAuthServer() {
  const port = process.env.PORT ? Number(process.env.PORT) : 5050
  const clientId = process.env.DISCORD_OAUTH_CLIENT_ID
  const clientSecret = process.env.DISCORD_OAUTH_CLIENT_SECRET
  const redirectUri = process.env.DISCORD_OAUTH_REDIRECT_URI
  if (!clientId || !redirectUri || !clientSecret) return
  if (clientSecret.includes('.')) {
    console.error('DISCORD_OAUTH_CLIENT_SECRET inválido: parece um token de bot. Use o Client Secret da aba OAuth2 do Developer Portal.')
    return
  }
  console.log('OAuth server ativo. redirect_uri:', redirectUri)
  const server = http.createServer(async (req, res) => {
    try {
      const u = new URL(req.url, `http://localhost:${port}`)
      if (u.pathname === '/health') {
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ ok: true, metrics }))
      } else if (u.pathname === '/discord-oauth/callback') {
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
  server.listen(port)
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
