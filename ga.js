const https = require('https')

const MEASUREMENT_ID = process.env.GA_MEASUREMENT_ID || 'G-13P4NV2CN5'
const API_SECRET = process.env.GA_API_SECRET || ''

function trackEvent(name, params = {}, userId) {
  try {
    if (!API_SECRET || !MEASUREMENT_ID) return
    const path = `/mp/collect?measurement_id=${encodeURIComponent(MEASUREMENT_ID)}&api_secret=${encodeURIComponent(API_SECRET)}`
    const body = JSON.stringify({
      client_id: 'discord-bot',
      ...(userId ? { user_id: String(userId) } : {}),
      events: [{ name, params }]
    })
    const req = https.request({
      hostname: 'www.google-analytics.com',
      path,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => { res.on('data', () => {}) })
    req.on('error', () => {})
    req.write(body)
    req.end()
  } catch {}
}

module.exports = { trackEvent }

