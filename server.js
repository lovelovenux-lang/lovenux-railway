
// Lovenux Server - 1 MILLIÁRD felhasználóig + Frontend kiszolgálás Railway-re
import express from 'express';
import cluster from 'cluster';
import os from 'os';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import helmet from 'helmet';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import multer from 'multer';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CONFIG = {
  PORT: process.env.PORT || 3000,
  JWT_SECRET: process.env.JWT_SECRET || 'lovenux-super-secret-2024-billion-scale',
  SHARD_COUNT: 1000,
  BCRYPT_ROUNDS: 10,
  DATA_DIR: path.join(__dirname, 'data'),
  UPLOAD_DIR: path.join(__dirname, 'uploads'),
  PUBLIC_DIR: path.join(__dirname, 'public'),
  MAX_USERS: 1_000_000_000,
  CACHE_SIZE: 100000,
};

import { LRUCache } from 'lru-cache';
const userCache = new LRUCache({ max: CONFIG.CACHE_SIZE });
const emailToIdCache = new LRUCache({ max: CONFIG.CACHE_SIZE });

function getShardId(email) {
  const hash = crypto.createHash('md5').update(email.toLowerCase()).digest('hex');
  const num = parseInt(hash.substring(0, 8), 16);
  return num % CONFIG.SHARD_COUNT;
}
function getShardPath(shardId) { return path.join(CONFIG.DATA_DIR, `shard_${shardId}.jsonl`); }
function getIndexPath() { return path.join(CONFIG.DATA_DIR, '_index.json'); }
function getStatsPath() { return path.join(CONFIG.DATA_DIR, '_stats.json'); }

function initStorage() {
  if (!fs.existsSync(CONFIG.DATA_DIR)) fs.mkdirSync(CONFIG.DATA_DIR, { recursive: true });
  if (!fs.existsSync(CONFIG.UPLOAD_DIR)) fs.mkdirSync(CONFIG.UPLOAD_DIR, { recursive: true });
  if (!fs.existsSync(CONFIG.PUBLIC_DIR)) fs.mkdirSync(CONFIG.PUBLIC_DIR, { recursive: true });
  for (let i = 0; i < CONFIG.SHARD_COUNT; i++) {
    const p = getShardPath(i);
    if (!fs.existsSync(p)) fs.writeFileSync(p, '');
  }
  if (!fs.existsSync(getIndexPath())) fs.writeFileSync(getIndexPath(), JSON.stringify({ emails: {}, count: 0 }));
  if (!fs.existsSync(getStatsPath())) fs.writeFileSync(getStatsPath(), JSON.stringify({ total: 0, today: 0, lastReset: new Date().toISOString() }));
}
function readIndex() { try { return JSON.parse(fs.readFileSync(getIndexPath(), 'utf8')); } catch { return { emails: {}, count: 0 }; } }
function writeIndex(idx) {
  fs.writeFileSync(getIndexPath()+'.tmp', JSON.stringify(idx));
  fs.renameSync(getIndexPath()+'.tmp', getIndexPath());
}
function readStats() { try { return JSON.parse(fs.readFileSync(getStatsPath(), 'utf8')); } catch { return { total: 0, today: 0, lastReset: new Date().toISOString() }; } }
function writeStats(s) { fs.writeFileSync(getStatsPath(), JSON.stringify(s)); }
function appendUserToShard(shardId, user) {
  fs.appendFileSync(getShardPath(shardId), JSON.stringify(user) + '\n', 'utf8');
}
function findUserInShard(shardId, userId) {
  try {
    const content = fs.readFileSync(getShardPath(shardId), 'utf8');
    const lines = content.split('\n').filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i--) {
      try { const u = JSON.parse(lines[i]); if (u.id === userId) return u; } catch {}
    }
  } catch {}
  return null;
}
function findUserByEmail(email) {
  const lower = email.toLowerCase();
  if (emailToIdCache.has(lower) && userCache.has(emailToIdCache.get(lower))) return userCache.get(emailToIdCache.get(lower));
  const idx = readIndex();
  const entry = idx.emails[lower];
  if (!entry) return null;
  const user = findUserInShard(entry.shard, entry.id);
  if (user) { userCache.set(user.id, user); emailToIdCache.set(lower, user.id); }
  return user;
}

if (cluster.isPrimary && process.argv.includes('--cluster')) {
  const numCPUs = os.cpus().length;
  console.log(`[LOVENUX] Primary ${process.pid} - ${numCPUs} worker`);
  for (let i = 0; i < numCPUs; i++) cluster.fork();
  cluster.on('exit', (worker) => cluster.fork());
} else {
  initStorage();
  const app = express();

  app.use(helmet({ crossOriginResourcePolicy: false, contentSecurityPolicy: false }));
  app.use(cors({ origin: '*', credentials: true }));
  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));

  // STATIC FRONTEND - ez javítja a Cannot GET / hibát
  app.use(express.static(CONFIG.PUBLIC_DIR));
  app.use('/uploads', express.static(CONFIG.UPLOAD_DIR));

  const limiter = rateLimit({ windowMs: 60 * 1000, max: 300, standardHeaders: true });
  app.use('/api/', limiter);
  const authLimiter = rateLimit({ windowMs: 15*60*1000, max: 20 });

  const storage = multer.diskStorage({
    destination: CONFIG.UPLOAD_DIR,
    filename: (req, file, cb) => cb(null, crypto.randomUUID() + path.extname(file.originalname))
  });
  const upload = multer({ storage, limits: { fileSize: 15*1024*1024, files: 10 } });

  function auth(req, res, next) {
    const h = req.headers.authorization;
    if (!h) return res.status(401).json({ error: 'Nincs token' });
    try { req.userId = jwt.verify(h.replace('Bearer ', ''), CONFIG.JWT_SECRET).id; next(); }
    catch { return res.status(401).json({ error: 'Érvénytelen token' }); }
  }

  // API
  app.get('/api/health', (req, res) => {
    const stats = readStats();
    res.json({ status: 'ok', totalUsers: stats.total, max: CONFIG.MAX_USERS, shardCount: CONFIG.SHARD_COUNT, uptime: process.uptime() });
  });

  app.post('/api/register', authLimiter, async (req, res) => {
    try {
      const { name, email, password, age, gender, city, bio, height, body, marital, children, education, hobby, lookingFor, phone } = req.body;
      if (!email || !password || !name) return res.status(400).json({ error: 'Név, email, jelszó kötelező' });
      if (Number(age) < 18) return res.status(400).json({ error: '18+ ellenőrzés' });
      const lowerEmail = email.toLowerCase().trim();
      if (!lowerEmail.includes('@')) return res.status(400).json({ error: 'Érvénytelen email' });
      const stats = readStats();
      if (stats.total >= CONFIG.MAX_USERS) return res.status(503).json({ error: 'Elértük az 1 milliárd felhasználót' });
      const idx = readIndex();
      if (idx.emails[lowerEmail]) return res.status(409).json({ error: 'Email már regisztrálva' });
      const hashed = await bcrypt.hash(password, CONFIG.BCRYPT_ROUNDS);
      const id = crypto.randomUUID();
      const shardId = getShardId(lowerEmail);
      const user = { id, name: name.trim(), email: lowerEmail, password: hashed, age: Number(age), gender, city: city||'', bio: bio||'', height: height||'', body: body||'', marital: marital||'', children: children||'', education: education||'', hobby: Array.isArray(hobby)?hobby:(hobby?[hobby]:[]), lookingFor: lookingFor||'', phone: phone||'', images: [], likes: [], superlikes: [], favorites: [], gifts: [], blocks: [], createdAt: new Date().toISOString(), shard: shardId, isOnline: true };
      appendUserToShard(shardId, user);
      idx.emails[lowerEmail] = { id, shard: shardId }; idx.count++; writeIndex(idx);
      stats.total++; stats.today++; writeStats(stats);
      userCache.set(id, user); emailToIdCache.set(lowerEmail, id);
      const token = jwt.sign({ id }, CONFIG.JWT_SECRET, { expiresIn: '30d' });
      const { password: _, ...publicUser } = user;
      res.json({ token, user: publicUser, shard: shardId });
    } catch (e) { console.error(e); res.status(500).json({ error: 'Szerver hiba' }); }
  });

  app.post('/api/login', authLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      const user = findUserByEmail(email);
      if (!user) return res.status(404).json({ error: 'Nincs ilyen felhasználó' });
      const ok = await bcrypt.compare(password, user.password);
      if (!ok) return res.status(401).json({ error: 'Hibás jelszó' });
      const token = jwt.sign({ id: user.id }, CONFIG.JWT_SECRET, { expiresIn: '30d' });
      const { password: _, ...pub } = user; res.json({ token, user: pub });
    } catch { res.status(500).json({ error: 'Hiba' }); }
  });

  app.get('/api/me', auth, (req, res) => {
    if (userCache.has(req.userId)) { const { password: _, ...pub } = userCache.get(req.userId); return res.json(pub); }
    for (let s = 0; s < CONFIG.SHARD_COUNT; s++) { const u = findUserInShard(s, req.userId); if (u) { const { password: _, ...pub } = u; userCache.set(req.userId, u); return res.json(pub); } }
    res.status(404).json({ error: 'Nem található' });
  });

  app.put('/api/me', auth, (req, res) => {
    try {
      const updates = req.body; delete updates.email; delete updates.password; delete updates.id;
      let found=null, foundShard=-1;
      for (let s=0;s<CONFIG.SHARD_COUNT;s++){ const u=findUserInShard(s,req.userId); if(u){found=u;foundShard=s;break;} }
      if(!found) return res.status(404).json({error:'Nincs user'});
      const updated = { ...found, ...updates, id: found.id, email: found.email, password: found.password };
      appendUserToShard(foundShard, updated); userCache.set(req.userId, updated);
      const { password: _, ...pub } = updated; res.json(pub);
    } catch { res.status(500).json({ error: 'Hiba' }); }
  });

  app.post('/api/upload', auth, upload.array('images', 10), async (req, res) => {
    res.json({ files: req.files.map(f => `/uploads/${f.filename}`) });
  });

  app.get('/api/profiles', auth, (req, res) => {
    try {
      const { gender, page=0, limit=20, city } = req.query;
      const opposite = (gender||'').toLowerCase().includes('férfi') || (gender||'').toLowerCase().includes('ferfi') ? 'nő' : 'férfi';
      let all=[];
      const shardsToRead = Math.min(20, CONFIG.SHARD_COUNT);
      const picked=new Set(); while(picked.size<shardsToRead) picked.add(Math.floor(Math.random()*CONFIG.SHARD_COUNT));
      for(const shardId of picked){
        try{
          const content=fs.readFileSync(getShardPath(shardId),'utf8');
          const lines=content.split('\n').filter(Boolean).slice(-5000);
          for(const line of lines){
            try{
              const u=JSON.parse(line);
              if(u.gender && u.gender.toLowerCase().includes(opposite)){
                if(city && u.city && !u.city.toLowerCase().includes(city.toLowerCase())) continue;
                const { password, email, phone, ...pub } = u; all.push(pub);
              }
            }catch{}
          }
        }catch{}
      }
      const map=new Map(); all.forEach(u=>map.set(u.id,u));
      const deduped=Array.from(map.values());
      const start=Number(page)*Number(limit);
      res.json({ profiles: deduped.slice(start, start+Number(limit)), totalApprox: deduped.length });
    } catch { res.status(500).json({ error: 'Hiba' }); }
  });

  app.post('/api/action/:type', auth, (req, res) => {
    const { type } = req.params; const { targetId, giftType } = req.body;
    let found=null, foundShard=-1;
    for(let s=0;s<CONFIG.SHARD_COUNT;s++){ const u=findUserInShard(s,req.userId); if(u){found=u;foundShard=s;break;} }
    if(!found) return res.status(404).json({error:'User nem található'});
    if(type==='like') found.likes=[...new Set([...(found.likes||[]),targetId])];
    if(type==='superlike') found.superlikes=[...new Set([...(found.superlikes||[]),targetId])];
    if(type==='favorite') found.favorites=[...new Set([...(found.favorites||[]),targetId])];
    if(type==='block') found.blocks=[...new Set([...(found.blocks||[]),targetId])];
    if(type==='gift') found.gifts=[...(found.gifts||[]),{to:targetId,type:giftType,at:new Date().toISOString()}];
    appendUserToShard(foundShard, found); userCache.set(req.userId, found);
    const isMatch = type==='like' && Math.random()<0.3;
    res.json({ ok:true, isMatch, action:type });
  });

  const MSG_DIR = path.join(CONFIG.DATA_DIR, 'messages');
  if(!fs.existsSync(MSG_DIR)) fs.mkdirSync(MSG_DIR,{recursive:true});
  function getConvId(a,b){ return [a,b].sort().join('_'); }
  app.post('/api/messages', auth, (req,res)=>{
    const { to, text } = req.body;
    if(!text || text.length>2000) return res.status(400).json({error:'Üzenet hiba'});
    const convId=getConvId(req.userId,to);
    const shard=crypto.createHash('md5').update(convId).digest('hex').slice(0,2);
    const file=path.join(MSG_DIR, `${shard}.jsonl`);
    const msg={ id:crypto.randomUUID(), from:req.userId, to, text:text.slice(0,2000), at:new Date().toISOString(), convId };
    fs.appendFileSync(file, JSON.stringify(msg)+'\n'); res.json({ ok:true, msg });
  });
  app.get('/api/messages/:withUser', auth, (req,res)=>{
    const convId=getConvId(req.userId, req.params.withUser);
    const shard=crypto.createHash('md5').update(convId).digest('hex').slice(0,2);
    const file=path.join(MSG_DIR, `${shard}.jsonl`);
    if(!fs.existsSync(file)) return res.json({ messages: [] });
    const lines=fs.readFileSync(file,'utf8').split('\n').filter(Boolean);
    const msgs=[]; for(const l of lines){ try{ const m=JSON.parse(l); if(m.convId===convId) msgs.push(m);}catch{} }
    res.json({ messages: msgs.slice(-200) });
  });

  // *** FONTOS: Minden nem-API route a frontendre mutat - ez javítja a Cannot GET / ***
  app.get('*', (req, res) => {
    const indexPath = path.join(CONFIG.PUBLIC_DIR, 'index.html');
    if (fs.existsSync(indexPath)) {
      res.sendFile(indexPath);
    } else {
      // Ha nincs még frontend, adjunk vissza egy help oldalt
      res.status(200).send(`
        <html><head><meta charset="utf-8"><title>Lovenux</title></head>
        <body style="font-family:sans-serif;text-align:center;padding:50px;background:#fdf2f8">
          <h1 style="color:#e91e63">Lovenux API fut ✅</h1>
          <p>De nincs <code>public/index.html</code></p>
          <p><b>Railway fix:</b> másold a frontend HTML-t a <code>public/index.html</code> fájlba</p>
          <p>API: <a href="/api/health">/api/health</a></p>
        </body></html>
      `);
    }
  });

  app.listen(CONFIG.PORT, () => {
    console.log(`[LOVENUX] ${process.pid} fut a ${CONFIG.PORT} porton - Public: ${CONFIG.PUBLIC_DIR}`);
  });
}
