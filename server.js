require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const compression = require('compression');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'Lovenux2026-BILLIO-SECRET-CHANGE-ME';
const ADMIN_EMAIL = (process.env.ADMIN_EMAIL || 'love.lovenux@gmail.com').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const UPLOAD_DIR = path.join(__dirname, 'public/uploads');

const SHARD_COUNT = parseInt(process.env.SHARD_COUNT || '16', 10);
const DATABASE_URLS = (process.env.DATABASE_URLS || process.env.DATABASE_URL || '').split(',').map(s=>s.trim()).filter(Boolean);
let DB_MODE = DATABASE_URLS.length > 0 ? 'SHARDED' : 'JSON';
console.log(`💘 Lovenux 1B Mode: ${DB_MODE} | Shards: ${SHARD_COUNT}`);

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, {recursive:true});
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, {recursive:true});

function hashEmail(email){
  const h = crypto.createHash('md5').update(email.toLowerCase()).digest('hex');
  return parseInt(h.slice(0,8),16) % SHARD_COUNT;
}
function getShardIndex(email){ return hashEmail(email); }

let pgPools = [];
async function initPG(){
  if(DB_MODE!=='SHARDED') return;
  const { Pool } = require('pg');
  pgPools = DATABASE_URLS.map((url,i)=>{
    const actualUrl = DATABASE_URLS[i] || DATABASE_URLS[0];
    return new Pool({ connectionString: actualUrl, max: 20, idleTimeoutMillis: 30000 });
  });
  for(let i=0;i<Math.min(SHARD_COUNT, pgPools.length || SHARD_COUNT); i++){
    const pool = pgPools[i % pgPools.length];
    try{
      await pool.query(`
        CREATE TABLE IF NOT EXISTS users_${i} (
          id BIGSERIAL PRIMARY KEY,
          email TEXT UNIQUE NOT NULL,
          password TEXT NOT NULL,
          city TEXT,
          birth DATE,
          age INT,
          child TEXT,
          bio TEXT,
          photos JSONB DEFAULT '[]',
          is_paid BOOLEAN DEFAULT FALSE,
          paid_at TIMESTAMPTZ,
          shard INT DEFAULT ${i},
          created_at TIMESTAMPTZ DEFAULT NOW(),
          last_active TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_users_${i}_email ON users_${i}(email);
        CREATE TABLE IF NOT EXISTS payments_${i} (
          id BIGSERIAL PRIMARY KEY,
          payment_id TEXT UNIQUE NOT NULL,
          user_id BIGINT NOT NULL,
          email TEXT NOT NULL,
          amount INT DEFAULT 1000,
          status TEXT DEFAULT 'Prepared',
          gateway_url TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          succeeded_at TIMESTAMPTZ
        );
      `);
    }catch(e){ console.error(`Shard ${i} init error:`, e.message); }
  }
}

const DATA_FILE = path.join(DATA_DIR, 'db.json');
let dbCache = null;
let saveLock = false;
function loadDB(){
  try{
    if(!fs.existsSync(DATA_FILE)){
      const init = {users:[], payments:[], likes:[], matches:[], reports:[], blocks:[], messages:[]};
      fs.writeFileSync(DATA_FILE, JSON.stringify(init));
      return init;
    }
    return JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));
  }catch(e){ return {users:[], payments:[], likes:[], matches:[], reports:[], blocks:[], messages:[]}; }
}
function saveDB(db){
  if(saveLock) return;
  saveLock = true;
  try{
    const tmp = DATA_FILE+'.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA_FILE);
    dbCache = db;
  }finally{ saveLock = false; }
}
if(DB_MODE==='JSON'){ dbCache = loadDB(); setInterval(()=>{ if(dbCache) saveDB(dbCache); }, 30000); }

app.use(helmet({contentSecurityPolicy:false, crossOriginEmbedderPolicy:false}));
app.use(compression());
app.use(morgan(DB_MODE==='JSON'?'dev':'combined'));
app.use(cors({origin:true, credentials:true}));
app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true, limit:'2mb'}));
app.use('/uploads', express.static(UPLOAD_DIR, {maxAge:'7d'}));
app.use(express.static(path.join(__dirname,'public'), {maxAge:'0', index:'index.html'}));
app.use(express.static(__dirname, {maxAge:'0', index:'index.html'}));

const registerLimiter = rateLimit({windowMs: 15*60*1000, max: 20});
const loginLimiter = rateLimit({windowMs: 15*60*1000, max: 100});
app.use('/api/register', registerLimiter);
app.use('/api/login', loginLimiter);

const storage = multer.diskStorage({
  destination: (req,file,cb)=> cb(null, UPLOAD_DIR),
  filename: (req,file,cb)=>{
    const ext = path.extname(file.originalname).toLowerCase() || '.jpg';
    cb(null, Date.now()+'-'+crypto.randomBytes(6).toString('hex')+ext);
  }
});
const upload = multer({storage, limits:{fileSize:5*1024*1024, files:10}});

function auth(req,res,next){
  const token = (req.headers.authorization||'').replace('Bearer ','');
  if(!token) return res.status(401).json({error:'Nincs token'});
  try{ req.user = jwt.verify(token, JWT_SECRET); next(); }catch(e){ return res.status(401).json({error:'Token hiba'}); }
}
function adminAuth(req,res,next){
  const token = (req.headers.authorization||'').replace('Bearer ','');
  if(!token) return res.status(401).json({error:'Nincs admin token'});
  try{
    const u = jwt.verify(token, JWT_SECRET);
    if((u.email||'').toLowerCase()!==ADMIN_EMAIL && u.role!=='admin') return res.status(403).json({error:'Nem admin'});
    req.user = u; next();
  }catch(e){ return res.status(401).json({error:'Admin token hiba'}); }
}

async function findUserByEmail(email){
  email = email.toLowerCase();
  if(DB_MODE==='JSON') return dbCache.users.find(u=>u.email===email) || null;
  const shard = getShardIndex(email);
  const pool = pgPools[shard % pgPools.length];
  if(pgPools.length===1){
    for(let i=0;i<SHARD_COUNT;i++){
      try{ const r = await pool.query(`SELECT * FROM users_${i} WHERE email=$1 LIMIT 1`, [email]); if(r.rows[0]) return r.rows[0]; }catch(_){}
    }
    return null;
  } else {
    const r = await pool.query(`SELECT * FROM users_${shard % SHARD_COUNT} WHERE email=$1 LIMIT 1`, [email]);
    return r.rows[0] || null;
  }
}

app.get('/api/test',(req,res)=>res.json({message:'Lovenux 1B API LIVE', mode: DB_MODE, shards: SHARD_COUNT}));
app.get('/api/beat',(req,res)=>res.json({ok:true, beat:'💘 LIVE', time:new Date().toISOString(), mode:DB_MODE, shards:SHARD_COUNT, email:ADMIN_EMAIL}));

app.post('/api/register', upload.array('photos',10), async (req,res)=>{
  try{
    const email=(req.body.email||'').toLowerCase().trim();
    const password=req.body.password||'';
    const city=req.body.city||'';
    const birth=req.body.birth||null;
    const bio=req.body.bio||'';
    if(!email||!password) return res.status(400).json({error:'Email és jelszó kell'});
    if(await findUserByEmail(email)) return res.status(400).json({error:'Már van ilyen email'});
    const hash=await bcrypt.hash(password,10);
    const photos=(req.files||[]).map(f=>'/uploads/'+path.basename(f.path));
    const user={email,password:hash,city,birth,bio,photos,is_paid:false,createdAt:new Date().toISOString()};
    if(DB_MODE==='JSON'){
      user.id=Date.now()+Math.floor(Math.random()*1000);
      dbCache.users.push(user); saveDB(dbCache);
    } else {
      const shard=getShardIndex(email);
      const pool=pgPools[shard % pgPools.length];
      const r=await pool.query(`INSERT INTO users_${shard % SHARD_COUNT}(email,password,city,birth,bio,photos,is_paid,shard) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`, [email,hash,city,birth,bio,JSON.stringify(photos),false,shard]);
      Object.assign(user, r.rows[0]);
    }
    res.json({success:true, email:user.email, needPayment:true});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/barion/start', async (req,res)=>{
  const email=(req.body.email||'').toLowerCase();
  if(!email) return res.status(400).json({error:'Email kell'});
  const paymentId='BARION-'+Date.now()+'-'+crypto.randomBytes(4).toString('hex');
  const gatewayUrl=`https://mypos.barion.com/pay/${paymentId}`;
  res.json({paymentId,gatewayUrl,alreadyPaid:false,testMode:!process.env.BARION_KEY});
});

app.post('/api/barion/confirm', async (req,res)=>{
  const {paymentId,email} = req.body;
  if(!email) return res.status(400).json({error:'Email kell'});
  let user=await findUserByEmail(email);
  if(!user) return res.status(404).json({error:'Nincs user'});
  if(DB_MODE==='JSON'){ user.is_paid=true; user.paid_at=new Date().toISOString(); saveDB(dbCache); }
  else { const shard=getShardIndex(email); const pool=pgPools[shard % pgPools.length]; for(let i=0;i<SHARD_COUNT;i++){ try{ await pool.query(`UPDATE users_${i} SET is_paid=true, paid_at=NOW() WHERE email=$1`, [email]); }catch(_){}} }
  const token=jwt.sign({id:user.id||1,email:user.email||email,role:'user'}, JWT_SECRET, {expiresIn:'30d'});
  res.json({success:true, token});
});

app.post('/api/login', async (req,res)=>{
  try{
    const email=(req.body.email||'').toLowerCase();
    const password=req.body.password||'';
    const user=await findUserByEmail(email);
    if(!user) return res.status(400).json({error:'Nincs ilyen user'});
    const ok=await bcrypt.compare(password, user.password);
    if(!ok) return res.status(400).json({error:'Hibás jelszó'});
    if(!user.is_paid) return res.status(402).json({error:'Még nem fizettél 1000 Ft-ot!', needPayment:true, email:user.email});
    const token=jwt.sign({id:user.id||1,email:user.email,role:'user'}, JWT_SECRET, {expiresIn:'30d'});
    res.json({success:true, token, user:{id:user.id,email:user.email,is_paid:true,city:user.city}});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/forgot', async (req,res)=>{ res.json({message:'Ha van ilyen e-mail, küldtünk levelet'}); });

app.get('/api/discover', auth, async (req,res)=>{
  try{
    let list = DB_MODE==='JSON' ? dbCache.users.filter(u=>u.email!==req.user.email && u.is_paid) : [];
    if(DB_MODE!=='JSON'){
      const pool=pgPools[0];
      for(let i=0;i<SHARD_COUNT;i++){ try{ const r=await pool.query(`SELECT id,email,city,bio,photos,is_paid FROM users_${i} WHERE is_paid=true LIMIT 50`); list=list.concat(r.rows); }catch(_){}}
    }
    const users = list.slice(0,50).map(u=>{ const {password,...safe}=u; return safe; });
    res.json({total: users.length, users});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/admin/login', async (req,res)=>{
  const {email,password}=req.body;
  if(email.toLowerCase()!==ADMIN_EMAIL) return res.status(403).json({error:'Nem admin email'});
  const adminPass = process.env.ADMIN_PASS || 'LovenuxAdmin2026!';
  if(password!==adminPass) return res.status(400).json({error:'Hibás admin jelszó'});
  const token = jwt.sign({email: email.toLowerCase(), role:'admin'}, JWT_SECRET, {expiresIn:'12h'});
  res.json({success:true, token});
});

app.get('/api/admin/stats', adminAuth, async (req,res)=>{
  try{
    let total=0, paid=0, today=0;
    if(DB_MODE==='JSON'){
      total=dbCache.users.length;
      paid=dbCache.users.filter(u=>u.is_paid).length;
      today=dbCache.users.filter(u=> (new Date(u.createdAt).toDateString()===new Date().toDateString())).length;
    } else {
      const pool=pgPools[0];
      for(let i=0;i<SHARD_COUNT;i++){
        try{
          const r=await pool.query(`SELECT COUNT(*) as c FROM users_${i}`);
          total+=parseInt(r.rows[0].c||0);
          const rp=await pool.query(`SELECT COUNT(*) as c FROM users_${i} WHERE is_paid=true`);
          paid+=parseInt(rp.rows[0].c||0);
        }catch(_){}
      }
    }
    res.json({total, totalUsers:total, paidUsers:paid, todayUsers:today, today, totalIncome:paid*1000, online:Math.floor(total*0.3), mode:DB_MODE, shards:SHARD_COUNT});
  }catch(e){ res.status(500).json({error:e.message}); }
});

// 1B PAGINATED USERS - NEM FAGY LE 1 MILLIÁRDNÁL
app.get('/api/admin/users', adminAuth, async (req,res)=>{
  try{
    const page=parseInt(req.query.page||'1'); const limit=Math.min(parseInt(req.query.limit||'100'), 500);
    const search=(req.query.search||'').toLowerCase(); const paidFilter=req.query.paid;
    let all=[];
    if(DB_MODE==='JSON'){
      all=dbCache.users;
      if(search) all=all.filter(u=>u.email.toLowerCase().includes(search));
      if(paidFilter==='true') all=all.filter(u=>u.is_paid);
      if(paidFilter==='false') all=all.filter(u=>!u.is_paid);
    } else {
      const pool=pgPools[0];
      for(let i=0;i<SHARD_COUNT;i++){
        try{
          const r=await pool.query(`SELECT id,email,city,age,photos,is_paid,created_at,shard FROM users_${i} ORDER BY id DESC LIMIT 500`);
          all=all.concat(r.rows.map(x=>({...x, createdAt:x.created_at, created_at:x.created_at})));
        }catch(_){}
      }
      if(search) all=all.filter(u=>(u.email||'').toLowerCase().includes(search));
      if(paidFilter==='true') all=all.filter(u=>u.is_paid);
      if(paidFilter==='false') all=all.filter(u=>!u.is_paid);
      all.sort((a,b)=> (b.id||0)-(a.id||0));
    }
    const total=all.length;
    const start=(page-1)*limit;
    const users=all.slice(start, start+limit).map(u=>{ const {password,...safe}=u; return safe; });
    res.json({page, limit, total, users});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/admin/toggle-paid/:id', adminAuth, async (req,res)=>{
  try{
    const id=req.params.id;
    if(DB_MODE==='JSON'){
      const u=dbCache.users.find(x=> String(x.id)===String(id));
      if(!u) return res.status(404).json({error:'Nincs user'});
      u.is_paid=!u.is_paid;
      saveDB(dbCache);
      res.json({success:true, is_paid:u.is_paid});
    } else {
      const pool=pgPools[0];
      for(let i=0;i<SHARD_COUNT;i++){
        try{
          const r=await pool.query(`UPDATE users_${i} SET is_paid = NOT is_paid WHERE id=$1 RETURNING is_paid`, [id]);
          if(r.rows[0]) return res.json({success:true, is_paid:r.rows[0].is_paid});
        }catch(_){}
      }
      res.status(404).json({error:'Nincs user'});
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});

// PHONE PERFECT APP compatibility
app.get('/api/users/discovery', auth, async (req,res)=>{
  try{
    let list = DB_MODE==='JSON' ? dbCache.users.filter(u=>u.email!==req.user.email) : [];
    if(DB_MODE!=='JSON'){
      const pool=pgPools[0];
      for(let i=0;i<SHARD_COUNT;i++){ try{ const r=await pool.query(`SELECT id,email,city,age,bio,photos,is_paid FROM users_${i} WHERE is_paid=true LIMIT 100`); list=list.concat(r.rows); }catch(_){}}
    }
    const safe=list.slice(0,100).map(u=>{ const {password,...s}=u; return {id:u.id, name:(u.email||'').split('@')[0], age:u.age||25, city:u.city||'Bp', dist: Math.floor(Math.random()*20)+' km', online:1, verified: u.is_paid?1:0, bio:u.bio||'', ...s}; });
    res.json(safe);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/heart', (req,res)=>res.json({ok:true, beat:'💘 LIVE', time:new Date().toISOString(), mode:DB_MODE, shards:SHARD_COUNT, email:ADMIN_EMAIL}));

app.get('*',(req,res)=>{
  // Never show API LIVE - always serve frontend
  const candidates = [
    path.join(__dirname,'public','index.html'),
    path.join(__dirname,'index.html'),
    path.join(process.cwd(),'public','index.html'),
    path.join(process.cwd(),'index.html'),
    '/opt/render/project/src/public/index.html',
    '/opt/render/project/src/index.html'
  ];
  for(let p of candidates){
    if(fs.existsSync(p)){
      console.log('✅ Serving frontend:', p, 'for', req.path);
      return res.sendFile(p);
    }
  }
  console.error('❌ No index.html found, candidates checked:', candidates);
  // Last resort - serve the public/index.html content directly if we can read it
  try{
    const fallback = path.join(__dirname,'public','index.html');
    if(fs.existsSync(fallback)){
      return res.send(fs.readFileSync(fallback,'utf8'));
    }
  }catch(e){}
  res.status(200).send('<!DOCTYPE html><html><head><meta http-equiv="refresh" content="2"></head><body style="font-family:sans-serif;text-align:center;padding:50px"><h1>💘 Lovenux ébred... 30 mp</h1><p>Free szerver ébred, frissítsd az oldalt!</p><script>setTimeout(()=>location.reload(),2500)</script></body></html>');
});

(async()=>{
  if(DB_MODE==='SHARDED'){ try{ await initPG(); }catch(e){ console.error('PG init failed, fallback to JSON', e.message); DB_MODE='JSON'; dbCache=loadDB(); } }
  app.listen(PORT, ()=>console.log(`💘 Lovenux 1B fut: ${PORT} - MODE:${DB_MODE} - ${ADMIN_EMAIL}`));
})();
