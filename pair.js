const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const cheerio = require('cheerio');
const moment = require('moment-timezone');
const crypto = require('crypto');
const axios = require('axios');
const os = require('os');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    getContentType,
    makeCacheableSignalKeyStore,
    Browsers,
    jidNormalizedUser,
    downloadContentFromMessage,
    proto,
    DisconnectReason
} = require('baileys');

// ==================== CONFIG ====================

const BOT_NAME_FANCY = '💀 𝐂𝐃𝐓 𝐇𝐀𝐂𝐊𝐄𝐑 𝐌𝐈𝐍𝐈 💀';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'true',
  AUTO_LIKE_EMOJI: ['🔥','💀','👾','🤖','💻','🎯','⚡','🔪','🗡️','💣'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  RCD_IMAGE_PATH: 'https://files.catbox.moe/xveuc2.jpg',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: '94789226570',
  BOT_NAME: '💀 𝐂𝐃𝐓 𝐇𝐀𝐂𝐊𝐄𝐑 𝐌𝐈𝐍𝐈 💀',
  BOT_VERSION: '3.0.0',
  OWNER_NAME: '𝐂𝐃𝐓 𝐇𝐀𝐂𝐊𝐄𝐑 𝐓𝐄𝐀𝐌',
  IMAGE_PATH: 'https://files.catbox.moe/xveuc2.jpg',
  BOT_FOOTER: '💀 𝐂𝐃𝐓 𝐇𝐀𝐂𝐊𝐄𝐑 𝐁𝐎𝐓 💀',
  MODE: process.env.BOT_MODE || 'public',
  SESSIONS_DIR: './sessions',
  DEFAULT_LOGO: 'https://files.catbox.moe/xveuc2.jpg'
};

// ==================== JSON File Storage ====================

const SESSIONS_DIR = config.SESSIONS_DIR;
const NUMBERS_FILE = path.join(SESSIONS_DIR, 'numbers.json');
const ADMINS_FILE = path.join(SESSIONS_DIR, 'admins.json');
const NEWSLETTER_FILE = path.join(SESSIONS_DIR, 'newsletters.json');
const CONFIGS_FILE = path.join(SESSIONS_DIR, 'configs.json');
const PREFIX_FILE = path.join(SESSIONS_DIR, 'prefixes.json');

if (!fs.existsSync(SESSIONS_DIR)) fs.ensureDirSync(SESSIONS_DIR);

function readJSON(filePath, defaultValue = {}) {
  try {
    if (fs.existsSync(filePath)) {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    }
    return defaultValue;
  } catch (e) {
    console.error(`Error reading ${filePath}:`, e);
    return defaultValue;
  }
}

function writeJSON(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
    return true;
  } catch (e) {
    console.error(`Error writing ${filePath}:`, e);
    return false;
  }
}

// Storage functions
async function saveCredsToFile(number, creds, keys = null) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    const sessionFile = path.join(SESSIONS_DIR, `session_${sanitized}.json`);
    const data = { number: sanitized, creds, keys, updatedAt: new Date().toISOString() };
    fs.writeFileSync(sessionFile, JSON.stringify(data, null, 2));
    return true;
  } catch (e) { return false; }
}

async function loadCredsFromFile(number) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    const sessionFile = path.join(SESSIONS_DIR, `session_${sanitized}.json`);
    if (fs.existsSync(sessionFile)) {
      return JSON.parse(fs.readFileSync(sessionFile, 'utf8'));
    }
    return null;
  } catch (e) { return null; }
}

async function removeSessionFromFile(number) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    const sessionFile = path.join(SESSIONS_DIR, `session_${sanitized}.json`);
    if (fs.existsSync(sessionFile)) fs.unlinkSync(sessionFile);
    return true;
  } catch (e) { return false; }
}

async function addNumberToFile(number) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    let numbers = readJSON(NUMBERS_FILE, []);
    if (!numbers.includes(sanitized)) {
      numbers.push(sanitized);
      writeJSON(NUMBERS_FILE, numbers);
    }
    return true;
  } catch (e) { return false; }
}

async function removeNumberFromFile(number) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    let numbers = readJSON(NUMBERS_FILE, []);
    numbers = numbers.filter(n => n !== sanitized);
    writeJSON(NUMBERS_FILE, numbers);
    return true;
  } catch (e) { return false; }
}

async function getAllNumbersFromFile() {
  return readJSON(NUMBERS_FILE, []);
}

async function loadAdminsFromFile() {
  return readJSON(ADMINS_FILE, []);
}

async function addAdminToFile(jidOrNumber) {
  let admins = readJSON(ADMINS_FILE, []);
  if (!admins.includes(jidOrNumber)) {
    admins.push(jidOrNumber);
    writeJSON(ADMINS_FILE, admins);
  }
  return true;
}

async function removeAdminFromFile(jidOrNumber) {
  let admins = readJSON(ADMINS_FILE, []);
  admins = admins.filter(a => a !== jidOrNumber);
  writeJSON(ADMINS_FILE, admins);
  return true;
}

async function addNewsletterToFile(jid, emojis = []) {
  let newsletters = readJSON(NEWSLETTER_FILE, []);
  const existing = newsletters.find(n => n.jid === jid);
  if (existing) {
    existing.emojis = emojis;
  } else {
    newsletters.push({ jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date().toISOString() });
  }
  writeJSON(NEWSLETTER_FILE, newsletters);
  return true;
}

async function removeNewsletterFromFile(jid) {
  let newsletters = readJSON(NEWSLETTER_FILE, []);
  newsletters = newsletters.filter(n => n.jid !== jid);
  writeJSON(NEWSLETTER_FILE, newsletters);
  return true;
}

async function listNewslettersFromFile() {
  return readJSON(NEWSLETTER_FILE, []);
}

async function setUserConfigInFile(number, conf) {
  const sanitized = number.replace(/[^0-9]/g, '');
  let configs = readJSON(CONFIGS_FILE, {});
  configs[sanitized] = { ...configs[sanitized], ...conf, updatedAt: new Date().toISOString() };
  writeJSON(CONFIGS_FILE, configs);
  return true;
}

async function loadUserConfigFromFile(number) {
  const sanitized = number.replace(/[^0-9]/g, '');
  const configs = readJSON(CONFIGS_FILE, {});
  return configs[sanitized] || null;
}

async function getPrefixForChat(chatId) {
  const prefixes = readJSON(PREFIX_FILE, {});
  return prefixes[chatId] || config.PREFIX;
}

async function setPrefixForChat(chatId, prefix) {
  let prefixes = readJSON(PREFIX_FILE, {});
  prefixes[chatId] = prefix;
  writeJSON(PREFIX_FILE, prefixes);
  return true;
}

// Alias functions
const initMongo = async () => console.log('✅ HACKER BOT STORAGE READY');
const loadAdminsFromMongo = loadAdminsFromFile;
const addAdminToMongo = addAdminToFile;
const removeAdminFromMongo = removeAdminFromFile;
const loadUserConfigFromMongo = loadUserConfigFromFile;
const setUserConfigInMongo = setUserConfigInFile;
const addNewsletterToMongo = addNewsletterToFile;
const removeNewsletterFromMongo = removeNewsletterFromFile;
const listNewslettersFromMongo = listNewslettersFromFile;
const getAllNumbersFromMongo = getAllNumbersFromFile;
const addNumberToMongo = addNumberToFile;
const removeNumberFromMongo = removeNumberFromFile;
const removeSessionFromMongo = removeSessionFromFile;

// ==================== Utils ====================

function formatMessage(title, content, footer) {
  return `╭━━━❰ *${title}* ❱━━━❍\n┃\n${content}\n┃\n╰━━━━━━━━━━━━━━━━━━━━❍\n\n> *${footer}*`;
}

function getSriLankaTimestamp(){ 
  return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); 
}

function generateOTP(){ 
  return Math.floor(100000 + Math.random() * 900000).toString(); 
}

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();

// ==================== PROGRESS BAR & VOICE MESSAGE GENERATOR ====================

async function sendWithProgress(socket, sender, finalMessage, logoUrl = null) {
  try {
    const progressChars = ['░', '▒', '▓', '█'];
    const messages = [];
    
    // Send initial loading message
    const loadMsg = await socket.sendMessage(sender, { text: '🔄 *Initializing HACKER BOT...* 0%' });
    
    // Progress animation from 1 to 100
    for (let i = 1; i <= 100; i++) {
      const percent = i;
      const filled = Math.floor(percent / 5);
      const empty = 20 - filled;
      const bar = '█'.repeat(filled) + '░'.repeat(empty);
      const emoji = percent < 30 ? '🔴' : (percent < 70 ? '🟡' : '🟢');
      
      const progressText = `${emoji} *${botName}* ${emoji}\n\n┌──────────────┐\n│${bar}│ ${percent}%\n└──────────────┘\n\n💀 *HACKING PROGRESS...* 💀`;
      
      try {
        await socket.sendMessage(sender, { text: progressText, edit: loadMsg.key });
      } catch(e) {}
      await delay(30);
    }
    
    // Send final message with logo
    let finalContent = finalMessage;
    if (logoUrl) {
      try {
        await socket.sendMessage(sender, { 
          image: { url: logoUrl }, 
          caption: finalMessage,
          contextInfo: { forwardingScore: 999, isForwarded: true }
        });
      } catch(e) {
        await socket.sendMessage(sender, { text: finalMessage });
      }
    } else {
      await socket.sendMessage(sender, { text: finalMessage });
    }
    
    // Send voice message for special commands
    return true;
  } catch(e) {
    await socket.sendMessage(sender, { text: finalMessage });
    return false;
  }
}

async function sendVoiceWithMessage(socket, sender, textMessage, logoUrl = null) {
  // Send voice note (as audio with ptt true)
  const voiceText = textMessage.replace(/[^a-zA-Z0-9\s]/g, '').substring(0, 100);
  
  try {
    // Using a TTS API to generate voice
    const ttsUrl = `https://translate.google.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(voiceText)}&tl=en&client=tw-ob`;
    const voiceBuffer = await axios.get(ttsUrl, { responseType: 'arraybuffer' });
    
    await socket.sendMessage(sender, {
      audio: voiceBuffer.data,
      mimetype: 'audio/mpeg',
      ptt: true
    });
  } catch(e) {
    // If TTS fails, just continue without voice
  }
  
  // Send image with text
  if (logoUrl) {
    try {
      await socket.sendMessage(sender, { 
        image: { url: logoUrl }, 
        caption: textMessage,
        contextInfo: { forwardingScore: 999, isForwarded: true }
      });
    } catch(e) {
      await socket.sendMessage(sender, { text: textMessage });
    }
  } else {
    await socket.sendMessage(sender, { text: textMessage });
  }
}

async function sendLoadingAnimation(socket, sender, finalMessage, logoUrl = null) {
  const botName = BOT_NAME_FANCY;
  
  // Animation frames
  const frames = [
    '🔴 [░░░░░░░░░░░░░░░░░░░░] 0%',
    '🟠 [█░░░░░░░░░░░░░░░░░░░] 5%',
    '🟡 [██░░░░░░░░░░░░░░░░░░] 10%',
    '🟡 [███░░░░░░░░░░░░░░░░░] 15%',
    '🟢 [████░░░░░░░░░░░░░░░░] 20%',
    '🟢 [█████░░░░░░░░░░░░░░░] 25%',
    '🔵 [██████░░░░░░░░░░░░░░] 30%',
    '🔵 [███████░░░░░░░░░░░░░] 35%',
    '💜 [████████░░░░░░░░░░░░] 40%',
    '💜 [█████████░░░░░░░░░░░] 45%',
    '❤️ [██████████░░░░░░░░░░] 50%',
    '❤️ [███████████░░░░░░░░░] 55%',
    '🧡 [████████████░░░░░░░░] 60%',
    '🧡 [█████████████░░░░░░░] 65%',
    '💛 [██████████████░░░░░░] 70%',
    '💛 [███████████████░░░░░] 75%',
    '💚 [████████████████░░░░] 80%',
    '💚 [█████████████████░░░] 85%',
    '💙 [██████████████████░░] 90%',
    '💙 [███████████████████░] 95%',
    '💀 [████████████████████] 100%'
  ];
  
  let loadMsg = null;
  
  for (let i = 0; i < frames.length; i++) {
    const frame = frames[i];
    const text = `╭━━━❰ *💀 ${botName} 💀* ❱━━━❍\n┃\n┃ ${frame}\n┃\n┃ 💀 *SYSTEM BOOTING UP...* 💀\n┃\n╰━━━━━━━━━━━━━━━━━━━━❍`;
    
    if (i === 0) {
      loadMsg = await socket.sendMessage(sender, { text: text });
    } else {
      try {
        await socket.sendMessage(sender, { text: text, edit: loadMsg.key });
      } catch(e) {}
    }
    await delay(40);
  }
  
  // Send final message with logo
  if (logoUrl) {
    try {
      await socket.sendMessage(sender, { 
        image: { url: logoUrl }, 
        caption: finalMessage,
        contextInfo: { forwardingScore: 999, isForwarded: true }
      });
    } catch(e) {
      await socket.sendMessage(sender, { text: finalMessage });
    }
  } else {
    await socket.sendMessage(sender, { text: finalMessage });
  }
  
  // Try to delete loading message
  try {
    await socket.sendMessage(sender, { delete: loadMsg.key });
  } catch(e) {}
}

// ==================== Join Group ====================

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      if (retries === 0) return { status: 'failed', error: error.message };
      await delay(2000);
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

// ==================== Send Messages ====================

async function sendAdminConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  const admins = await loadAdminsFromFile();
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
  const caption = formatMessage(botName, `📞 *Number:* ${number}\n🕐 *Time:* ${getSriLankaTimestamp()}\n📡 *Status:* Connected Successfully!`, botName);
  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      await sendLoadingAnimation(socket, to, caption, image);
    } catch (err) {}
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  try {
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
    const caption = formatMessage(`👑 OWNER CONNECT`, `📞 *Number:* ${number}\n🔢 *Active Sessions:* ${activeCount}\n🕐 *Time:* ${getSriLankaTimestamp()}`, botName);
    await sendLoadingAnimation(socket, ownerJid, caption, image);
  } catch (err) { console.error('Failed to send owner connect message:', err); }
}

// ==================== Newsletter Handlers ====================

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromFile();
      if (!followedDocs.some(d => d.jid === jid)) return;

      let emojis = config.AUTO_LIKE_EMOJI;
      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          break;
        } catch (err) {
          await delay(1200);
        }
      }
    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message);
    }
  });
}

// ==================== Status Handlers ====================

async function setupStatusHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast') return;
    try {
      const sanitizedNumber = (socket.user && socket.user.id) ? socket.user.id.split(':')[0] : null;
      const sessionConfig = sanitizedNumber ? (await loadUserConfigFromFile(sanitizedNumber) || {}) : {};

      const stviewEnabled = (typeof sessionConfig.stview !== 'undefined') ? !!sessionConfig.stview : (config.AUTO_VIEW_STATUS === 'true');
      if (stviewEnabled) {
        try { await socket.readMessages([message.key]); } catch (e) {}
      }

      let emojis = Array.isArray(sessionConfig.sr) && sessionConfig.sr.length ? sessionConfig.sr : config.AUTO_LIKE_EMOJI;
      if (emojis && emojis.length > 0) {
        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
        try {
          await socket.sendMessage(message.key.remoteJid, { react: { text: randomEmoji, key: message.key } }, { statusJidList: [message.key.participant] });
        } catch (error) {}
      }
    } catch (error) { console.error('Status handler error:', error); }
  });
}

// ==================== Cleanup ====================

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);
    await removeSessionFromFile(sanitized);
    await removeNumberFromFile(sanitized);
    const sessionDir = path.join(SESSIONS_DIR, `session_${sanitized}`);
    if (fs.existsSync(sessionDir)) fs.removeSync(sessionDir);
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ==================== Auto Restart ====================

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const isLoggedOut = statusCode === 401;
      if (isLoggedOut) {
        await deleteSessionAndCleanup(number, socket);
      } else {
        setTimeout(() => {
          activeSockets.delete(number.replace(/[^0-9]/g,''));
          socketCreationTime.delete(number.replace(/[^0-9]/g,''));
        }, 10000);
      }
    }
  });
}

// ==================== Download Media Helper ====================

async function downloadQuotedMedia(quoted) {
  if (!quoted) return null;
  const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
  const qType = qTypes.find(t => quoted[t]);
  if (!qType) return null;
  const messageType = qType.replace(/Message$/i, '').toLowerCase();
  const stream = await downloadContentFromMessage(quoted[qType], messageType);
  let buffer = Buffer.from([]);
  for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
  return { buffer, mime: quoted[qType].mimetype || '', caption: quoted[qType].caption || '' };
}

// ==================== Number Info Function ====================

async function getNumberInfo(number, socket) {
  try {
    const cleanNumber = number.replace(/[^0-9]/g, '');
    const jid = cleanNumber + '@s.whatsapp.net';
    
    let ppUrl = null;
    let hasDp = false;
    try {
      ppUrl = await socket.profilePictureUrl(jid, 'image');
      hasDp = true;
    } catch { hasDp = false; }
    
    let about = '📝 *About:* No bio available';
    try {
      const status = await socket.fetchStatus(jid);
      if (status && status.status) about = `📝 *About:* ${status.status}`;
    } catch {}
    
    let presence = '🔵 *Presence:* Offline';
    try {
      const presenceData = await socket.presenceSubscribe(jid);
      if (presenceData && presenceData.presence === 'available') presence = '🟢 *Presence:* Online';
    } catch {}
    
    const countryCodes = {
      '94': { name: 'Sri Lanka 🇱🇰', flag: '🇱🇰', dialCode: '+94' },
      '91': { name: 'India 🇮🇳', flag: '🇮🇳', dialCode: '+91' },
      '1': { name: 'USA 🇺🇸', flag: '🇺🇸', dialCode: '+1' },
      '44': { name: 'UK 🇬🇧', flag: '🇬🇧', dialCode: '+44' },
      '61': { name: 'Australia 🇦🇺', flag: '🇦🇺', dialCode: '+61' }
    };
    
    let countryCode = cleanNumber.substring(0, 2);
    const country = countryCodes[countryCode] || { name: 'Unknown 🌐', flag: '🌐', dialCode: '+' + countryCode };
    const formattedNumber = `${country.dialCode} ${cleanNumber.substring(countryCode.length)}`;
    const currentTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
    
    const infoText = `╭━━━❰ *📱 NUMBER INFORMATION* ❱━━━❍
┃
┃ 📞 *Number:* ${formattedNumber}
┃ 🌍 *Country:* ${country.name} ${country.flag}
┃ 🕐 *Local Time:* ${currentTime}
┃ ${about}
┃ ${presence}
┃ 📸 *Profile Picture:* ${hasDp ? '✅ Available' : '❌ Not Available'}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
    
    return { infoText, ppUrl, hasDp };
  } catch (error) {
    return { infoText: `❌ *Error:* ${error.message}`, ppUrl: null, hasDp: false };
  }
}

// ==================== Command Handlers ====================

function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net') : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const isOwner = senderNumber === config.OWNER_NUMBER.replace(/[^0-9]/g,'');
    const isGroup = String(from || '').endsWith('@g.us');

    const body = (type === 'conversation') ? msg.message.conversation
      : (type === 'extendedTextMessage') ? msg.message.extendedTextMessage.text
      : (type === 'imageMessage' && msg.message.imageMessage.caption) ? msg.message.imageMessage.caption
      : (type === 'videoMessage' && msg.message.videoMessage.caption) ? msg.message.videoMessage.caption
      : '';

    if (!body || typeof body !== 'string') return;

    const prefix = await getPrefixForChat(from);
    const isCmd = body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    if (!command) return;

    // Permission check
    try {
      const sanitizedNumber = (number || '').replace(/[^0-9]/g, '');
      const sessionConfig = await loadUserConfigFromFile(sanitizedNumber) || {};
      const sessionMode = sessionConfig.mode || config.MODE;

      if (!isOwner) {
        if (sessionMode === 'private') {
          await socket.sendMessage(sender, { text: '❌ *Permission Denied!*\nBot is in *private* mode.' });
          return;
        }
        if (isGroup && sessionMode === 'inbox') {
          await socket.sendMessage(sender, { text: '❌ *Permission Denied!*\nBot is in *inbox* mode.' });
          return;
        }
        if (!isGroup && sessionMode === 'groups') {
          await socket.sendMessage(sender, { text: '❌ *Permission Denied!*\nBot is in *groups* mode.' });
          return;
        }
      }
    } catch (permErr) {}

    try {
      // Load config for logo and botName
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const cfg = await loadUserConfigFromFile(sanitized) || {};
      const botName = cfg.botName || BOT_NAME_FANCY;
      const logo = cfg.logo || config.RCD_IMAGE_PATH;
      
      switch (command) {
        
        // ==================== MAIN MENU ====================
        case 'menu':
        case 'help':
        case 'cmds': {
          await socket.sendMessage(sender, { react: { text: '💀', key: msg.key } });
          
          const uptime = Math.floor((Date.now() - (socketCreationTime.get(number) || Date.now())) / 1000);
          const hours = Math.floor(uptime / 3600);
          const minutes = Math.floor((uptime % 3600) / 60);
          const seconds = Math.floor(uptime % 60);
          
          const menuText = `
╔══════════════════════════════════╗
║     💀 *${botName}* 💀      ║
║   🔥 HACKER EDITION v3.0 🔥    ║
╚══════════════════════════════════╝

┌──────────────────────────────────┐
│         🤖 *BOT INFO* 🤖          │
├──────────────────────────────────┤
│ 🕐 Uptime: ${hours}h ${minutes}m ${seconds}s
│ 👥 Active: ${activeSockets.size} session(s)
│ 📌 Prefix: ${prefix}
│ 👑 Owner: ${config.OWNER_NAME}
└──────────────────────────────────┘

┌──────────────────────────────────┐
│       📍 *BASIC COMMANDS* 📍       │
├──────────────────────────────────┤
│ ${prefix}alive - 🟢 Bot status
│ ${prefix}ping - 🏓 Check latency
│ ${prefix}menu - 📋 Show menu
│ ${prefix}owner - 👑 Owner info
│ ${prefix}system - 💻 System info
│ ${prefix}jid - 🆔 Get chat JID
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      🎵 *DOWNLOADER* 🎵           │
├──────────────────────────────────┤
│ ${prefix}song <name> - 🎧 Download song
│ ${prefix}tt <url> - 📱 TikTok video
│ ${prefix}fb <url> - 📘 Facebook video
│ ${prefix}ig <url> - 📸 Instagram video
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      🛠️ *TOOLS* 🛠️                │
├──────────────────────────────────┤
│ ${prefix}getdp <number> - 🖼️ Get DP
│ ${prefix}numberinfo <number> - 📊 Number info
│ ${prefix}font <text> - ✨ Fancy font
│ ${prefix}cid <link> - 📡 Get channel ID
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      🤖 *AI FEATURES* 🤖          │
├──────────────────────────────────┤
│ ${prefix}ai <text> - 🧠 Chat with AI
│ ${prefix}aiimg <prompt> - 🎨 AI image
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      ⚙️ *SETTINGS* ⚙️             │
├──────────────────────────────────┤
│ ${prefix}setlogo (reply to image) - 🖼️ Set logo
│ ${prefix}setbotname <name> - 🤖 Set bot name
│ ${prefix}setmode <mode> - 🔧 Set mode
│ ${prefix}setprefix <symbol> - 🔣 Set prefix
│ ${prefix}getmode - 📊 View mode
│ ${prefix}showconfig - ⚙️ View config
│ ${prefix}resetconfig - 🔄 Reset config
│ ${prefix}deleteme - 🗑️ Delete session
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      👥 *GROUP COMMANDS* 👥       │
├──────────────────────────────────┤
│ ${prefix}tagall <msg> - 📢 Tag all members
│ ${prefix}hidetag <msg> - 👻 Hidden tag
│ ${prefix}tagadmins - 👑 Tag admins
│ ${prefix}gjid - 📋 Get group JID list
│ ${prefix}online - 🟢 Check online members
│ ${prefix}left <jid> - 🚪 Leave group(s)
│ ${prefix}leave - 🚶 Leave current group
│ ${prefix}savecontact <jid> - 💾 Save group contacts
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      💥 *HACKER TOOLS* 💥         │
├──────────────────────────────────┤
│ ${prefix}spam <count> <msg> - 💣 Spam message
│ ${prefix}bug <number> - 🐛 Bug report
└──────────────────────────────────┘

┌──────────────────────────────────┐
│      📰 *NEWS & CHANNELS* 📰      │
├──────────────────────────────────┤
│ ${prefix}newslist - 📑 List followed channels
│ ${prefix}cid <link> - 🔍 Get channel ID
└──────────────────────────────────┘

╔══════════════════════════════════╗
║   💀 ${botName} 💀   ║
║   🔥 HACK THE SYSTEM! 🔥   ║
╚══════════════════════════════════╝
          `.trim();
          
          await sendLoadingAnimation(socket, sender, menuText, logo);
          break;
        }
        
        // ==================== ALIVE ====================
        case 'alive': {
          const uptime = Math.floor((Date.now() - (socketCreationTime.get(number) || Date.now())) / 1000);
          const hours = Math.floor(uptime / 3600);
          const minutes = Math.floor((uptime % 3600) / 60);
          const seconds = Math.floor(uptime % 60);
          
          const text = `╭━━━❰ *💀 ${botName} 💀* ❱━━━❍
┃
┃ 🟢 *Status:* Online & Ready
┃ 👑 *Owner:* ${config.OWNER_NAME}
┃ ⏳ *Uptime:* ${hours}h ${minutes}m ${seconds}s
┃ 📌 *Prefix:* ${prefix}
┃ 💫 *Version:* ${config.BOT_VERSION}
┃
┃ 💀 *"Hack the System!"* 💀
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          
          await sendLoadingAnimation(socket, sender, text, logo);
          await sendVoiceWithMessage(socket, sender, `Bot is online and ready to hack. Uptime ${hours} hours ${minutes} minutes.`, logo);
          break;
        }
        
        // ==================== PING ====================
        case 'ping': {
          const latency = Date.now() - (msg.messageTimestamp * 1000 || Date.now());
          const text = `🏓 *Pong!*\n⏱️ *Latency:* ${latency}ms\n🟢 *Status:* Online\n💀 *Hacker Bot Active*`;
          await sendLoadingAnimation(socket, sender, text, logo);
          await sendVoiceWithMessage(socket, sender, `Pong! Latency ${latency} milliseconds.`, logo);
          break;
        }
        
        // ==================== OWNER ====================
        case 'owner': {
          const text = `╭━━━❰ *👑 OWNER INFO* ❱━━━❍
┃
┃ 📛 *Name:* ${config.OWNER_NAME}
┃ 📞 *Contact:* +${config.OWNER_NUMBER}
┃ 💫 *Bot:* ${BOT_NAME_FANCY}
┃ 📌 *Version:* ${config.BOT_VERSION}
┃
┃ 💀 *For support or queries,* 
┃ 💀 *contact the owner directly.*
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          await sendLoadingAnimation(socket, sender, text, logo);
          break;
        }
        
        // ==================== SYSTEM ====================
        case 'system': {
          const text = `╭━━━❰ *💻 SYSTEM INFO* ❱━━━❍
┃
┃ 💀 *OS:* ${os.type()} ${os.release()}
┃ 🔱 *Platform:* ${os.platform()}
┃ 🧠 *CPU Cores:* ${os.cpus().length}
┃ 💾 *Total RAM:* ${(os.totalmem()/1024/1024/1024).toFixed(2)} GB
┃ 📊 *Free RAM:* ${(os.freemem()/1024/1024/1024).toFixed(2)} GB
┃ ✨ *Bot:* ${BOT_NAME_FANCY}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          await sendLoadingAnimation(socket, sender, text, logo);
          break;
        }
        
        // ==================== JID ====================
        case 'jid': {
          await sendLoadingAnimation(socket, sender, `🆔 *Chat JID:*\n┗━━ ${sender}`, logo);
          break;
        }
        
        // ==================== GET DP ====================
        case 'getdp': {
          let q = args[0];
          if (!q) {
            await sendLoadingAnimation(socket, sender, "❌ *Please provide a number!*\n📌 Usage: .getdp <number>", logo);
            return;
          }
          
          let jid = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
          let ppUrl;
          try {
            ppUrl = await socket.profilePictureUrl(jid, "image");
          } catch {
            ppUrl = "https://files.catbox.moe/xveuc2.jpg";
          }
          
          await sendLoadingAnimation(socket, sender, `🖼️ *Profile Picture of* +${q}\n💀 *Fetched by ${BOT_NAME_FANCY}*`, ppUrl);
          break;
        }
        
        // ==================== NUMBER INFO ====================
        case 'numberinfo':
        case 'numinfo':
        case 'ni': {
          let q = args[0];
          if (!q) {
            await sendLoadingAnimation(socket, sender, "❌ *Please provide a number!*\n📌 Usage: .numberinfo <number>\n💀 Example: .numberinfo 94789226570", logo);
            return;
          }
          
          await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });
          
          const { infoText, ppUrl, hasDp } = await getNumberInfo(q, socket);
          
          if (hasDp && ppUrl) {
            await sendLoadingAnimation(socket, sender, infoText, ppUrl);
          } else {
            await sendLoadingAnimation(socket, sender, infoText, logo);
          }
          break;
        }
        
        // ==================== SET LOGO ====================
        case 'setlogo': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (senderNum !== sanitized && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\nOnly session owner can change logo.', logo);
            return;
          }
          
          const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
          if (!quoted || !quoted.imageMessage) {
            await sendLoadingAnimation(socket, sender, "❌ *Reply to an image!*\n📌 Usage: Reply to an image with .setlogo", logo);
            return;
          }
          
          try {
            const media = await downloadQuotedMedia(quoted);
            if (!media || !media.buffer) {
              await sendLoadingAnimation(socket, sender, "❌ *Failed to download image!*", logo);
              return;
            }
            
            const sessionPath = path.join(SESSIONS_DIR, `session_${sanitized}`);
            fs.ensureDirSync(sessionPath);
            const logoPath = path.join(sessionPath, `logo.jpg`);
            fs.writeFileSync(logoPath, media.buffer);
            
            let cfg = await loadUserConfigFromFile(sanitized) || {};
            cfg.logo = logoPath;
            await setUserConfigInFile(sanitized, cfg);
            
            await sendLoadingAnimation(socket, sender, "✅ *Logo updated successfully!*\n💀 New logo will be used for this session.", logoPath);
          } catch (e) {
            await sendLoadingAnimation(socket, sender, `❌ *Error:* ${e.message}`, logo);
          }
          break;
        }
        
        // ==================== SET PREFIX ====================
        case 'setprefix': {
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (!isOwner && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\nOnly bot owner can change prefix.', logo);
            return;
          }
          
          const newPrefix = args[0];
          if (!newPrefix) {
            await sendLoadingAnimation(socket, sender, `❌ *Please provide a prefix!*\n📌 Usage: .setprefix .\n💀 Current prefix: ${prefix}`, logo);
            return;
          }
          
          await setPrefixForChat(from, newPrefix);
          await sendLoadingAnimation(socket, sender, `✅ *Prefix changed!*\n🔣 New prefix: \`${newPrefix}\``, logo);
          break;
        }
        
        // ==================== SET BOT NAME ====================
        case 'setbotname': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (senderNum !== sanitized && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\nOnly session owner can change bot name.', logo);
            return;
          }
          
          const name = args.join(' ').trim();
          if (!name) {
            await sendLoadingAnimation(socket, sender, '❌ *Provide a name!*\n📌 Usage: .setbotname My Bot Name', logo);
            return;
          }
          
          let cfg = await loadUserConfigFromFile(sanitized) || {};
          cfg.botName = name;
          await setUserConfigInFile(sanitized, cfg);
          await sendLoadingAnimation(socket, sender, `✅ *Bot name set to:* ${name}`, logo);
          break;
        }
        
        // ==================== SET MODE ====================
        case 'setmode': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (senderNum !== sanitized && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\nOnly session owner can change mode.', logo);
            return;
          }
          
          const modeArg = (args[0] || '').toLowerCase();
          const allowed = ['private', 'inbox', 'groups', 'public'];
          
          if (!modeArg || !allowed.includes(modeArg)) {
            await sendLoadingAnimation(socket, sender, '❌ *Invalid mode!*\n📌 Available: public, private, inbox, groups', logo);
            return;
          }
          
          let cfg = await loadUserConfigFromFile(sanitized) || {};
          cfg.mode = modeArg;
          await setUserConfigInFile(sanitized, cfg);
          await sendLoadingAnimation(socket, sender, `✅ *Mode updated to:* ${modeArg.toUpperCase()}`, logo);
          break;
        }
        
        // ==================== GET MODE ====================
        case 'getmode': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const cfg = await loadUserConfigFromFile(sanitized) || {};
          const mode = cfg.mode || config.MODE || 'public';
          await sendLoadingAnimation(socket, sender, `🔧 *Current Mode:* ${mode.toUpperCase()}`, logo);
          break;
        }
        
        // ==================== SHOW CONFIG ====================
        case 'showconfig': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const cfg = await loadUserConfigFromFile(sanitized) || {};
          const botNameCfg = cfg.botName || BOT_NAME_FANCY;
          const mode = cfg.mode || config.MODE || 'public';
          
          const text = `╭━━━❰ *⚙️ CONFIGURATION* ❱━━━❍
┃
┃ 🤖 *Bot Name:* ${botNameCfg}
┃ 🔧 *Mode:* ${mode.toUpperCase()}
┃ 📌 *Prefix:* ${prefix}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          await sendLoadingAnimation(socket, sender, text, logo);
          break;
        }
        
        // ==================== RESET CONFIG ====================
        case 'resetconfig': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (senderNum !== sanitized && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*', logo);
            return;
          }
          
          await setUserConfigInFile(sanitized, {});
          await sendLoadingAnimation(socket, sender, '✅ *Config reset to defaults!*', logo);
          break;
        }
        
        // ==================== DELETE SESSION ====================
        case 'deleteme': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const senderNum = (nowsender || '').split('@')[0];
          const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
          
          if (senderNum !== sanitized && senderNum !== ownerNum) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*', logo);
            return;
          }
          
          await sendLoadingAnimation(socket, sender, '⚠️ *Deleting session...*', logo);
          await removeSessionFromFile(sanitized);
          await removeNumberFromFile(sanitized);
          activeSockets.delete(sanitized);
          socketCreationTime.delete(sanitized);
          await socket.sendMessage(sender, { text: '✅ *Session deleted!*\n💀 Bot will logout now.' });
          setTimeout(async () => {
            try { await socket.logout(); } catch(e) {}
            try { socket.ws?.close(); } catch(e) {}
          }, 2000);
          break;
        }
        
        // ==================== SONG DOWNLOAD ====================
        case 'song':
        case 'music': {
          const yts = require('yt-search');
          const query = args.join(" ").trim();
          
          if (!query) {
            await sendLoadingAnimation(socket, sender, '🎵 *Please provide a song name or YouTube link!*\n📌 Usage: .song Song Name', logo);
            return;
          }
          
          await socket.sendMessage(sender, { react: { text: '🎵', key: msg.key } });
          
          let ytUrl = query;
          if (!/^https?:\/\//i.test(query)) {
            const search = await yts(query);
            if (!search || !search.videos || search.videos.length === 0) {
              await sendLoadingAnimation(socket, sender, '❌ *No results found!*', logo);
              return;
            }
            ytUrl = search.videos[0].url;
          }
          
          const apiUrl = `https://api.srihub.store/download/ytmp3?apikey=dew_EtVuyJGtlCzvZY44TP6MbXpPlAltC6VH2uGOPAJL&url=${encodeURIComponent(ytUrl)}`;
          const apiRes = await axios.get(apiUrl, { timeout: 15000 }).then(r => r.data).catch(e => null);
          
          if (!apiRes || (!apiRes.downloadUrl && !apiRes.result?.download?.url)) {
            await sendLoadingAnimation(socket, sender, '❌ *Failed to get download link!*', logo);
            return;
          }
          
          const downloadUrl = apiRes.downloadUrl || apiRes.result?.download?.url;
          const title = apiRes.title || apiRes.result?.title || 'Unknown Title';
          
          await sendLoadingAnimation(socket, sender, `🎵 *${title}*\n💀 *Downloaded by ${BOT_NAME_FANCY}*`, logo);
          
          await socket.sendMessage(sender, {
            audio: { url: downloadUrl },
            mimetype: 'audio/mpeg',
            ptt: false
          });
          break;
        }
        
        // ==================== AI CHAT ====================
        case 'ai':
        case 'chat':
        case 'gpt': {
          const q = args.join(" ").trim();
          if (!q) {
            await sendLoadingAnimation(socket, sender, '🤖 *Please provide a message for AI!*\n📌 Usage: .ai Your question', logo);
            return;
          }
          
          await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
          
          const payload = { contents: [{ parts: [{ text: q }] }] };
          const { data } = await axios.post(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=AIzaSyDD79CzhemWoS4WXoMTpZcs8g0fWNytNug`,
            payload,
            { headers: { "Content-Type": "application/json" } }
          );
          
          if (!data?.candidates?.[0]?.content?.parts?.[0]?.text) {
            await sendLoadingAnimation(socket, sender, '❌ *AI reply not found!*', logo);
            return;
          }
          
          const aiReply = data.candidates[0].content.parts[0].text;
          await sendLoadingAnimation(socket, sender, `🤖 *AI Response:*\n\n${aiReply}\n\n💀 ${BOT_NAME_FANCY}`, logo);
          break;
        }
        
        // ==================== FONT ====================
        case 'font': {
          const text = args.join(" ").trim();
          if (!text) {
            await sendLoadingAnimation(socket, sender, '✨ *Please provide text to convert!*\n📌 Usage: .font Hello', logo);
            return;
          }
          
          const apiUrl = `https://www.dark-yasiya-api.site/other/font?text=${encodeURIComponent(text)}`;
          const response = await axios.get(apiUrl);
          
          if (!response.data.status || !response.data.result) {
            await sendLoadingAnimation(socket, sender, "❌ *Error fetching fonts!*", logo);
            return;
          }
          
          const fontList = response.data.result.slice(0, 8)
            .map(font => `*${font.name}:*\n${font.result}`)
            .join("\n\n");
          
          await sendLoadingAnimation(socket, sender, `✨ *Fancy Fonts*\n\n${fontList}\n\n💀 ${BOT_NAME_FANCY}`, logo);
          break;
        }
        
        // ==================== CHANNEL ID ====================
        case 'cid':
        case 'channelid': {
          const link = args[0];
          if (!link) {
            await sendLoadingAnimation(socket, sender, "❌ *Please provide a WhatsApp Channel link!*\n📌 Usage: .cid https://whatsapp.com/channel/xxxxx", logo);
            return;
          }
          
          const match = link.match(/whatsapp\.com\/channel\/([\w-]+)/);
          if (!match) {
            await sendLoadingAnimation(socket, sender, "⚠️ *Invalid channel link format!*", logo);
            return;
          }
          
          const inviteId = match[1];
          
          try {
            const metadata = await socket.newsletterMetadata("invite", inviteId);
            if (!metadata || !metadata.id) {
              await sendLoadingAnimation(socket, sender, '❌ *Channel not found!*', logo);
              return;
            }
            
            const text = `╭━━━❰ *📡 CHANNEL INFO* ❱━━━❍
┃
┃ 🆔 *ID:* ${metadata.id}
┃ 📌 *Name:* ${metadata.name}
┃ 👥 *Followers:* ${metadata.subscribers?.toLocaleString() || 'N/A'}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
            
            if (metadata.preview) {
              await sendLoadingAnimation(socket, sender, text, `https://pps.whatsapp.net${metadata.preview}`);
            } else {
              await sendLoadingAnimation(socket, sender, text, logo);
            }
          } catch (err) {
            await sendLoadingAnimation(socket, sender, `❌ *Error:* ${err.message}`, logo);
          }
          break;
        }
        
        // ==================== SPAM ====================
        case 'spam':
        case 'spm': {
          if (!isOwner) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\n💀 Only bot owner can use spam command.', logo);
            return;
          }
          
          const count = parseInt(args[0]);
          const message = args.slice(1).join(" ");
          
          if (!count || !message || count > 50) {
            await sendLoadingAnimation(socket, sender, "❌ *Usage:* .spam <count> <message>\n📌 Example: .spam 10 Hello\n💀 Max count: 50", logo);
            return;
          }
          
          await sendLoadingAnimation(socket, sender, `💀 *Starting spam ${count} times...*`, logo);
          
          for (let i = 0; i < count; i++) {
            await socket.sendMessage(sender, { text: message });
            await delay(500);
          }
          
          await sendLoadingAnimation(socket, sender, `✅ *Spam completed!* ${count} messages sent.`, logo);
          break;
        }
        
        // ==================== BUG REPORT ====================
        case 'bug':
        case 'report': {
          const bugDesc = args.join(" ").trim();
          if (!bugDesc) {
            await sendLoadingAnimation(socket, sender, "🐛 *Please describe the bug!*\n📌 Usage: .bug Something is wrong...", logo);
            return;
          }
          
          const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
          const reportText = `╭━━━❰ *🐛 BUG REPORT* ❱━━━❍
┃
┃ 👤 *Reported by:* ${senderNumber}
┃ 🕐 *Time:* ${getSriLankaTimestamp()}
┃ 📝 *Description:* ${bugDesc}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          
          await sendLoadingAnimation(socket, ownerJid, reportText, logo);
          await sendLoadingAnimation(socket, sender, "✅ *Bug report sent to owner!*\n💀 Thank you for reporting.", logo);
          break;
        }
        
        // ==================== TAG ALL ====================
        case 'tagall': {
          if (!isGroup) {
            await sendLoadingAnimation(socket, sender, '❌ *This command only works in groups!*', logo);
            return;
          }
          
          const groupMeta = await socket.groupMetadata(from);
          const participants = groupMeta.participants || [];
          const mentions = participants.map(p => p.id);
          const message = args.length ? args.join(" ") : "📢 Announcement";
          
          const text = `╭━━━❰ *📢 GROUP ANNOUNCEMENT* ❱━━━❍
┃
┃ 💬 *Message:* ${message}
┃ 👥 *Members:* ${participants.length}
┃
┃ *Tagging all members:*
┃
${mentions.map((m, i) => `┃ ${i+1}. @${m.split('@')[0]}`).join('\n')}
┃
╰━━━━━━━━━━━━━━━━━━━━❍
💀 ${BOT_NAME_FANCY}`;
          
          await socket.sendMessage(from, { text: text, mentions: mentions });
          break;
        }
        
        // ==================== HIDE TAG ====================
        case 'hidetag':
        case 'h': {
          if (!isGroup) {
            await sendLoadingAnimation(socket, sender, '❌ *This command only works in groups!*', logo);
            return;
          }
          
          const groupMeta = await socket.groupMetadata(from);
          const participants = groupMeta.participants || [];
          const mentions = participants.map(p => p.id);
          const message = args.length ? args.join(" ") : "👻 Hidden Tag";
          
          await socket.sendMessage(from, { text: message, mentions: mentions });
          break;
        }
        
        // ==================== TAG ADMINS ====================
        case 'tagadmins': {
          if (!isGroup) {
            await sendLoadingAnimation(socket, sender, '❌ *This command only works in groups!*', logo);
            return;
          }
          
          const groupMeta = await socket.groupMetadata(from);
          const admins = (groupMeta.participants || []).filter(p => p.admin === 'admin' || p.admin === 'superadmin').map(p => p.id);
          
          if (admins.length === 0) {
            await sendLoadingAnimation(socket, sender, '❌ *No admins found!*', logo);
            return;
          }
          
          const message = args.length ? args.join(" ") : "👑 Attention Admins!";
          const text = `╭━━━❰ *👑 ADMIN MENTION* ❱━━━❍
┃
┃ 💬 *Message:* ${message}
┃ 👑 *Admins:* ${admins.length}
┃
${admins.map((a, i) => `┃ ${i+1}. @${a.split('@')[0]}`).join('\n')}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          
          await socket.sendMessage(from, { text: text, mentions: admins });
          break;
        }
        
        // ==================== GROUP JID LIST ====================
        case 'gjid':
        case 'grouplist': {
          if (!isOwner) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*\n💀 Only bot owner can use this.', logo);
            return;
          }
          
          const groups = await socket.groupFetchAllParticipating();
          const groupArray = Object.values(groups);
          
          if (groupArray.length === 0) {
            await sendLoadingAnimation(socket, sender, '❌ *No groups found!*', logo);
            return;
          }
          
          let text = `╭━━━❰ *📋 GROUP JID LIST* ❱━━━❍\n┃\n`;
          groupArray.forEach((g, i) => {
            text += `┃ ${i+1}. *${g.subject || 'Unknown'}*\n┃    🆔 ${g.id}\n┃\n`;
          });
          text += `╰━━━━━━━━━━━━━━━━━━━━❍\n💀 Total: ${groupArray.length} groups`;
          
          await sendLoadingAnimation(socket, sender, text, logo);
          break;
        }
        
        // ==================== LEAVE GROUP ====================
        case 'leave': {
          if (!isGroup) {
            await sendLoadingAnimation(socket, sender, '❌ *This command only works in groups!*', logo);
            return;
          }
          
          if (!isOwner) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!\n💀 Only bot owner can make bot leave.', logo);
            return;
          }
          
          await sendLoadingAnimation(socket, from, '👋 *Bot is leaving this group...*', logo);
          await delay(2000);
          await socket.groupLeave(from);
          break;
        }
        
        // ==================== LEAVE MULTIPLE GROUPS ====================
        case 'left': {
          if (!isOwner) {
            await sendLoadingAnimation(socket, sender, '❌ *Permission Denied!*', logo);
            return;
          }
          
          const jids = args.join(" ").split(",").map(j => j.trim());
          if (!jids.length) {
            await sendLoadingAnimation(socket, sender, "❌ *Provide group JIDs!*\n📌 Usage: .left jid1,jid2,jid3", logo);
            return;
          }
          
          await sendLoadingAnimation(socket, sender, `⏳ *Leaving ${jids.length} group(s)...*`, logo);
          
          let success = 0, failed = 0;
          for (const jid of jids) {
            try {
              await socket.groupLeave(jid);
              success++;
              await delay(1000);
            } catch {
              failed++;
            }
          }
          
          await sendLoadingAnimation(socket, sender, `✅ *Left ${success} groups*\n❌ *Failed ${failed} groups*`, logo);
          break;
        }
        
        // ==================== ONLINE MEMBERS ====================
        case 'online': {
          if (!isGroup) {
            await sendLoadingAnimation(socket, sender, '❌ *This command only works in groups!*', logo);
            return;
          }
          
          await sendLoadingAnimation(socket, sender, "🔍 *Scanning for online members...*", logo);
          
          const groupMeta = await socket.groupMetadata(from);
          const participants = groupMeta.participants || [];
          
          const onlineList = [];
          for (const p of participants) {
            try {
              await socket.presenceSubscribe(p.id);
              onlineList.push(p.id);
            } catch {}
          }
          
          if (onlineList.length === 0) {
            await sendLoadingAnimation(socket, sender, "⚠️ *No online members detected!*", logo);
            return;
          }
          
          const text = `╭━━━❰ *🟢 ONLINE MEMBERS* ❱━━━❍
┃
┃ 👥 *Online:* ${onlineList.length}/${participants.length}
┃
${onlineList.map((o, i) => `┃ ${i+1}. @${o.split('@')[0]}`).join('\n')}
┃
╰━━━━━━━━━━━━━━━━━━━━❍`;
          
          await socket.sendMessage(from, { text: text, mentions: onlineList });
          break;
        }
        
        // ==================== SAVE CONTACTS ====================
        case 'savecontact':
        case 'savecontacts': {
          const groupJid = args[0];
          if (!groupJid || !groupJid.endsWith('@g.us')) {
            await sendLoadingAnimation(socket, sender, "❌ *Provide a valid group JID!*\n📌 Usage: .savecontact 123456789-123@g.us", logo);
            return;
          }
          
          const groupMeta = await socket.groupMetadata(groupJid);
          const participants = groupMeta.participants || [];
          
          let vcard = '';
          participants.forEach((p, i) => {
            const num = p.id.split('@')[0];
            vcard += `BEGIN:VCARD\nVERSION:3.0\nFN:Group Member ${i+1}\nTEL;waid=${num}:+${num}\nEND:VCARD\n`;
          });
          
          const filePath = path.join(SESSIONS_DIR, `contacts_${Date.now()}.vcf`);
          fs.writeFileSync(filePath, vcard);
          
          await socket.sendMessage(sender, {
            document: fs.readFileSync(filePath),
            mimetype: 'text/vcard',
            fileName: `group_contacts.vcf`,
            caption: `✅ *Contacts Exported!*\n👥 Group: ${groupMeta.subject}\n📇 Total: ${participants.length}\n💀 ${BOT_NAME_FANCY}`
          });
          
          fs.unlinkSync(filePath);
          break;
        }
        
        // ==================== NEWS LIST ====================
        case 'newslist': {
          const newsletters = await listNewslettersFromFile();
          if (!newsletters.length) {
            await sendLoadingAnimation(socket, sender, "📭 *No newsletter channels followed!*", logo);
            return;
          }
          
          let text = `╭━━━❰ *📰 FOLLOWED CHANNELS* ❱━━━❍\n┃\n`;
          newsletters.forEach((n, i) => {
            text += `┃ ${i+1}. 📡 ${n.jid}\n┃    💀 Reactions: ${n.emojis.length ? n.emojis.join(' ') : '(default)'}\n┃\n`;
          });
          text += `╰━━━━━━━━━━━━━━━━━━━━❍`;
          
          await sendLoadingAnimation(socket, sender, text, logo);
          break;
        }
        
        // ==================== AI IMAGE ====================
        case 'aiimg':
        case 'aiimage': {
          const prompt = args.join(" ").trim();
          if (!prompt) {
            await sendLoadingAnimation(socket, sender, '🎨 *Please provide a prompt for AI image!*\n📌 Usage: .aiimg a cat sitting on a throne', logo);
            return;
          }
          
          await socket.sendMessage(sender, { react: { text: '🎨', key: msg.key } });
          
          const apiUrl = `https://api.siputzx.my.id/api/ai/flux?prompt=${encodeURIComponent(prompt)}`;
          const response = await axios.get(apiUrl, { responseType: 'arraybuffer' });
          
          if (!response || !response.data) {
            await sendLoadingAnimation(socket, sender, '❌ *Failed to generate image!*', logo);
            return;
          }
          
          const imageBuffer = Buffer.from(response.data, 'binary');
          await sendLoadingAnimation(socket, sender, `🎨 *AI Generated Image*\n📌 Prompt: ${prompt}\n💀 ${BOT_NAME_FANCY}`, imageBuffer);
          break;
        }
        
        // ==================== DEFAULT ====================
        default:
          break;
      }
    } catch (err) {
      console.error('Command error:', err);
      try { 
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const cfg = await loadUserConfigFromFile(sanitized) || {};
        const logo = cfg.logo || config.RCD_IMAGE_PATH;
        await sendLoadingAnimation(socket, sender, `❌ *Error:* ${err.message}`, logo);
      } catch(e) {}
    }
  });
}

// ==================== Message Handlers ====================

function setupMessageHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;
    if (config.AUTO_RECORDING === 'true') {
      try { await socket.sendPresenceUpdate('recording', msg.key.remoteJid); } catch (e) {}
    }
  });
}

// ==================== Pairing Function ====================

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(SESSIONS_DIR, `session_${sanitizedNumber}`);
  
  try {
    const fileData = await loadCredsFromFile(sanitizedNumber);
    if (fileData && fileData.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(fileData.creds, null, 2));
      if (fileData.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(fileData.keys, null, 2));
    }
  } catch (e) {}

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

  try {
    const socket = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      printQRInTerminal: false,
      logger,
      browser: ["Ubuntu","Chrome","20.0.04"],
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    setupStatusHandlers(socket);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000); }
      }
      if (!res.headersSent) res.send({ code });
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const fileContent = await fs.readFile(path.join(sessionPath, 'creds.json'), 'utf8');
        const credsObj = JSON.parse(fileContent);
        const keysObj = state.keys || null;
        await saveCredsToFile(sanitizedNumber, credsObj, keysObj);
      } catch (err) {}
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          
          activeSockets.set(sanitizedNumber, socket);
          
          const userConfig = await loadUserConfigFromFile(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;
          
          const initialCaption = formatMessage(useBotName,
            `✅ *Successfully Connected!*\n\n📞 *Number:* ${sanitizedNumber}\n🕐 *Time:* ${getSriLankaTimestamp()}\n💀 *Hacker Bot Active*`,
            useBotName
          );
          
          await sendLoadingAnimation(socket, userJid, initialCaption, useLogo);
          await addNumberToFile(sanitizedNumber);
          
        } catch (e) { console.error('Connection open error:', e); }
      }
    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}

// ==================== Express Routes ====================

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) {
    return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  }
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ 
    botName: BOT_NAME_FANCY, 
    count: activeSockets.size, 
    numbers: Array.from(activeSockets.keys()), 
    timestamp: getSriLankaTimestamp() 
  });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromFile();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { 
        results.push({ number, status: 'already_connected' }); 
        continue; 
      }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { 
    res.status(500).send({ error: 'Failed to connect all bots' }); 
  }
});

router.get('/api/sessions', async (req, res) => {
  try {
    const sessionsDir = SESSIONS_DIR;
    const files = fs.readdirSync(sessionsDir).filter(f => f.startsWith('session_') && f.endsWith('.json'));
    const sessions = [];
    for (const file of files) {
      const data = JSON.parse(fs.readFileSync(path.join(sessionsDir, file), 'utf8'));
      sessions.push({ number: data.number, updatedAt: data.updatedAt });
    }
    res.json({ ok: true, sessions: sessions.sort((a,b) => new Date(b.updatedAt) - new Date(a.updatedAt)) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get('/api/active', async (req, res) => {
  res.json({ ok: true, active: Array.from(activeSockets.keys()), count: activeSockets.size });
});

router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { await running.logout(); } catch(e){}
      try { running.ws?.close(); } catch(e){}
      activeSockets.delete(sanitized);
    }
    await removeSessionFromFile(sanitized);
    await removeNumberFromFile(sanitized);
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Admin endpoints
router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  await addAdminToFile(jid);
  res.status(200).send({ status: 'ok', jid });
});

router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  await removeAdminFromFile(jid);
  res.status(200).send({ status: 'ok', jid });
});

router.get('/admin/list', async (req, res) => {
  const list = await loadAdminsFromFile();
  res.status(200).send({ status: 'ok', admins: list });
});

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  await addNewsletterToFile(jid, Array.isArray(emojis) ? emojis : []);
  res.status(200).send({ status: 'ok', jid });
});

router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  await removeNewsletterFromFile(jid);
  res.status(200).send({ status: 'ok', jid });
});

router.get('/newsletter/list', async (req, res) => {
  const list = await listNewslettersFromFile();
  res.status(200).send({ status: 'ok', channels: list });
});

// ==================== Cleanup ====================

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
  });
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

initMongo();

module.exports = router;
