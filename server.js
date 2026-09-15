
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

const app = express();
const PORT = process.env.PORT || 10000;
const JWT_SECRET = process.env.JWT_SECRET || 'Lovenux2026-BILLIO-SECRET-CHANGE-ME';
const ADMIN_EMAIL = (process.env.ADMIN_EMAIL || 'robi19920508@gmail.com').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const UPLOAD_DIR = path.join(__dirname, 'public/uploads');
const SHARD_COUNT = parseInt(process.env.SHARD_COUNT || '16', 10);
const DATABASE_URLS = (process.env.DATABASE_URLS || process.env.DATABASE_URL || '').split(',').map(s=>s.trim()).filter(Boolean);
let DB_MODE = DATABASE_URLS.length > 0 ? 'SHARDED' : 'JSON';
console.log(`Lovenux STABLE Mode: ${DB_MODE} | Port ${PORT}`);

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, {recursive:true});
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, {recursive:true});
function hashEmail(email){ const h=crypto.createHash('md5').update(email.toLowerCase()).digest('hex'); return parseInt(h.slice(0,8),16)%SHARD_COUNT; }
function getShardIndex(email){ return hashEmail(email); }
let pgPools=[];
async function initPG(){
  if(DB_MODE!=='SHARDED') return;
  const {Pool}=require('pg');
  pgPools=DATABASE_URLS.map(u=>new Pool({connectionString:u,max:20}));
  for(let i=0;i<SHARD_COUNT;i++){
    const pool=pgPools[i%pgPools.length];
    try{
      await pool.query(`CREATE TABLE IF NOT EXISTS users_${i}(id BIGSERIAL PRIMARY KEY,email TEXT UNIQUE NOT NULL,password TEXT NOT NULL,name TEXT,city TEXT,birth DATE,age INT,gender TEXT DEFAULT 'ferfi',looking_for TEXT DEFAULT 'noket',child TEXT,bio TEXT,photos JSONB DEFAULT '[]',is_paid BOOLEAN DEFAULT FALSE,paid_at TIMESTAMPTZ,shard INT DEFAULT ${i},created_at TIMESTAMPTZ DEFAULT NOW(),last_active TIMESTAMPTZ DEFAULT NOW()); CREATE TABLE IF NOT EXISTS payments_${i}(id BIGSERIAL PRIMARY KEY,payment_id TEXT UNIQUE,email TEXT,amount INT DEFAULT 1000,status TEXT DEFAULT 'Prepared',created_at TIMESTAMPTZ DEFAULT NOW(),succeeded_at TIMESTAMPTZ); CREATE TABLE IF NOT EXISTS likes_${i}(id BIGSERIAL PRIMARY KEY,from_email TEXT,to_email TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),UNIQUE(from_email,to_email)); CREATE TABLE IF NOT EXISTS matches_${i}(id BIGSERIAL PRIMARY KEY,user1 TEXT,user2 TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),UNIQUE(user1,user2)); CREATE TABLE IF NOT EXISTS messages_${i}(id BIGSERIAL PRIMARY KEY,from_email TEXT,to_email TEXT,text TEXT,at TIMESTAMPTZ DEFAULT NOW());`);
    }catch(e){ console.error(e.message); }
  }
}
const DATA_FILE=path.join(DATA_DIR,'db.json');
let dbCache=null;
function loadDB(){ try{ if(!fs.existsSync(DATA_FILE)){ const init={users:[],payments:[],likes:[],matches:[],messages:[]}; fs.writeFileSync(DATA_FILE,JSON.stringify(init)); return init; } return JSON.parse(fs.readFileSync(DATA_FILE,'utf8')); }catch(_){ return {users:[],payments:[],likes:[],matches:[],messages:[]}; } }
function saveDB(db){ try{ fs.writeFileSync(DATA_FILE+'.tmp',JSON.stringify(db)); fs.renameSync(DATA_FILE+'.tmp',DATA_FILE); dbCache=db; }catch(_){} }
if(DB_MODE==='JSON'){ dbCache=loadDB(); setInterval(()=>{ if(dbCache) saveDB(dbCache); },15000); }
app.use(helmet({contentSecurityPolicy:false}));
app.use(compression());
app.use(morgan('dev'));
app.use(cors({origin:true,credentials:true}));
app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true}));
app.use('/uploads', express.static(UPLOAD_DIR));
app.use(express.static(path.join(__dirname,'public')));
const storage=multer.diskStorage({destination:(r,f,cb)=>cb(null,UPLOAD_DIR),filename:(r,f,cb)=>cb(null,Date.now()+'-'+crypto.randomBytes(6).toString('hex')+path.extname(f.originalname||'.jpg'))});
const upload=multer({storage,limits:{fileSize:8*1024*1024,files:10}});
function auth(req,res,next){ const h=req.headers.authorization; if(!h) return res.status(401).json({error:'No token'}); try{ req.user=jwt.verify(h.replace('Bearer ',''),JWT_SECRET); next(); }catch(e){ return res.status(401).json({error:'Invalid'}); } }
function calcAge(b){ try{ const birth=new Date(b); const n=new Date(); let a=n.getFullYear()-birth.getFullYear(); if(n.getMonth()<birth.getMonth() || (n.getMonth()===birth.getMonth() && n.getDate()<birth.getDate())) a--; return a; }catch(_){ return 25; } }
async function findUserByEmail(email){ email=email.toLowerCase(); if(DB_MODE==='JSON') return dbCache.users.find(u=>u.email===email)||null; const shard=getShardIndex(email); for(let i=0;i<SHARD_COUNT;i++){ const idx=(shard+i)%SHARD_COUNT; try{ const pool=pgPools[idx%pgPools.length]; const r=await pool.query(`SELECT * FROM users_${idx} WHERE email=$1`,[email]); if(r.rows[0]){ r.rows[0].photos=r.rows[0].photos||[]; return r.rows[0]; } }catch(_){} } return null; }
async function listUsersPaginated({paid,page=1,limit=20,search='',excludeEmail='',filterGender=null}){
  page=parseInt(page)||1; limit=Math.min(50,parseInt(limit)||20);
  if(DB_MODE==='JSON'){
    let list=dbCache.users.filter(u=>u.email!==excludeEmail);
    if(paid===true) list=list.filter(u=>u.is_paid);
    if(search) list=list.filter(u=>(u.city||'').toLowerCase().includes(search.toLowerCase())||(u.name||'').toLowerCase().includes(search.toLowerCase()));
    if(filterGender){ let filtered=list.filter(u=>{ const g=(u.gender||'').toLowerCase(); if(!g) return true; return g===filterGender; }); if(filtered.length>0) list=filtered; }
    for(let i=list.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [list[i],list[j]]=[list[j],list[i]]; }
    const total=list.length; const slice=list.slice((page-1)*limit,page*limit).map(u=>{ const {password,...s}=u; return s; }); return {total,users:slice};
  } else {
    let all=[]; for(let i=0;i<SHARD_COUNT;i++){ try{ const pool=pgPools[i%pgPools.length]; let q=`SELECT * FROM users_${i} WHERE email!=$1`; let params=[excludeEmail]; if(paid===true){ q+=` AND is_paid=true`; } if(search){ q+=` AND (LOWER(city) LIKE LOWER('%'||$2||'%') OR LOWER(name) LIKE LOWER('%'||$2||'%'))`; params.push(search); } const r=await pool.query(q,params); all.push(...r.rows); }catch(_){} }
    if(filterGender){ let f=all.filter(u=>{ const g=(u.gender||'').toLowerCase(); if(!g) return true; return g===filterGender; }); if(f.length>0) all=f; }
    for(let i=all.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [all[i],all[j]]=[all[j],all[i]]; }
    const total=all.length; const slice=all.slice((page-1)*limit,page*limit).map(u=>{ const {password,...s}=u; s.photos=s.photos||[]; return s; }); return {total,users:slice};
  }
}
app.post('/api/register', upload.array('photos',10), async(req,res)=>{
  try{
    const {email,password,city,birth,child,bio,name,gender,looking_for}=req.body;
    if(!email||!password||!city||!birth||!name) return res.status(400).json({error:'Minden *-os kell'});
    if(password.length<6) return res.status(400).json({error:'Jelszó rövid'});
    const age=calcAge(birth); if(age<18) return res.status(400).json({error:'18+'});
    const emailLow=email.toLowerCase().trim();
    if(await findUserByEmail(emailLow)) return res.status(400).json({error:'Email már van'});
    const hashed=await bcrypt.hash(password,10);
    const photos=(req.files||[]).map(f=>'/uploads/'+path.basename(f.path));
    const g=(gender||'ferfi').toLowerCase(); const lf=(looking_for|| (g==='no'?'ferfiakat':'noket')).toLowerCase();
    const user={id:Date.now(),email:emailLow,password:hashed,name:name.trim(),city:city.trim(),birth,age,gender:g,looking_for:lf,child:child||'',bio:bio||'',photos,is_paid:false,createdAt:new Date().toISOString()};
    if(DB_MODE==='JSON'){ dbCache.users.push(user); saveDB(dbCache); } else { const shard=getShardIndex(emailLow); const pool=pgPools[shard%pgPools.length]; await pool.query(`INSERT INTO users_${shard}(email,password,name,city,birth,age,gender,looking_for,child,bio,photos,is_paid,shard) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,[user.email,user.password,user.name,user.city,user.birth,user.age,user.gender,user.looking_for,user.child,user.bio,JSON.stringify(user.photos),false,shard]); }
    res.json({success:true,email:user.email});
  }catch(e){ console.error(e); res.status(500).json({error:e.message}); }
});
app.post('/api/barion/start', async(req,res)=>{
  try{
    const {email}=req.body; const user=await findUserByEmail(email); if(!user) return res.status(404).json({error:'Nincs user'}); if(user.is_paid) return res.json({alreadyPaid:true,paymentId:'paid'});
    const pid='LOVENUX-'+Date.now()+'-'+crypto.randomBytes(3).toString('hex'); if(DB_MODE==='JSON'){ dbCache.payments=dbCache.payments||[]; dbCache.payments.push({payment_id:pid,email:user.email,amount:1000,status:'Prepared'}); saveDB(dbCache); }
    res.json({paymentId:pid,testMode:!process.env.BARION_KEY});
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/barion/confirm', async(req,res)=>{
  try{
    const {paymentId,email}=req.body; const emailLow=(email||'').toLowerCase(); const user=await findUserByEmail(emailLow); if(!user) return res.status(404).json({error:'Nincs user'});
    if(DB_MODE==='JSON'){ user.is_paid=true; user.paid_at=new Date().toISOString(); const pay=(dbCache.payments||[]).find(p=>p.payment_id===paymentId); if(pay) pay.status='Succeeded'; saveDB(dbCache); } else { const shard=getShardIndex(emailLow); const pool=pgPools[shard%pgPools.length]; await pool.query(`UPDATE users_${shard} SET is_paid=true,paid_at=NOW() WHERE email=$1`,[emailLow]); }
    const token=jwt.sign({id:user.id,email:user.email,role:'user'},JWT_SECRET,{expiresIn:'30d'}); res.json({success:true,token});
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/login', async(req,res)=>{
  try{
    const {email,password}=req.body; const user=await findUserByEmail(email); if(!user) return res.status(400).json({error:'Nincs user'}); const ok=await bcrypt.compare(password,user.password); if(!ok) return res.status(400).json({error:'Hibás jelszó'}); if(!user.is_paid) return res.status(402).json({error:'Még nem fizettél',needPayment:true,email:user.email});
    const token=jwt.sign({id:user.id,email:user.email,role:'user'},JWT_SECRET,{expiresIn:'30d'}); res.json({success:true,token,user:{email:user.email,is_paid:true,gender:user.gender,looking_for:user.looking_for}});
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/discover', auth, async(req,res)=>{
  try{
    const me=await findUserByEmail(req.user.email); if(!me) return res.status(400).json({error:'No user'});
    const myGender=(me.gender||'ferfi').toLowerCase(); const filterGender=myGender==='no'?'ferfi':'no';
    const data=await listUsersPaginated({paid:true,page:req.query.page||1,limit:req.query.limit||20,search:req.query.search||'',excludeEmail:me.email,filterGender}); res.json(data);
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/like', auth, async(req,res)=>{
  try{
    const from=req.user.email.toLowerCase(); const to=(req.body.toEmail||'').toLowerCase(); if(!to) return res.status(400).json({error:'Hiányzik'}); if(from===to) return res.status(400).json({error:'Magad nem'});
    if(DB_MODE==='JSON'){ dbCache.likes=dbCache.likes||[]; if(!dbCache.likes.find(l=>l.from===from&&l.to===to)) dbCache.likes.push({from,to,at:new Date().toISOString()}); const other=dbCache.likes.find(l=>l.from===to&&l.to===from); if(other){ dbCache.matches=dbCache.matches||[]; if(!dbCache.matches.find(m=>(m.user1===from&&m.user2===to)||(m.user1===to&&m.user2===from))) dbCache.matches.push({user1:from,user2:to,at:new Date().toISOString()}); saveDB(dbCache); return res.json({success:true,match:true}); } saveDB(dbCache); return res.json({success:true,match:false}); } else { const shard=getShardIndex(from); const pool=pgPools[shard%pgPools.length]; await pool.query(`INSERT INTO likes_${shard}(from_email,to_email) VALUES($1,$2) ON CONFLICT DO NOTHING`,[from,to]); let found=false; for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM likes_${i} WHERE from_email=$1 AND to_email=$2`,[to,from]); if(r.rows.length){ found=true; break; } }catch(_){} } if(found){ const shardM=getShardIndex(from); const poolM=pgPools[shardM%pgPools.length]; const u1=from<to?from:to; const u2=from<to?to:from; await poolM.query(`INSERT INTO matches_${shardM}(user1,user2) VALUES($1,$2) ON CONFLICT DO NOTHING`,[u1,u2]); return res.json({success:true,match:true}); } return res.json({success:true,match:false}); }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/matches', auth, async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    if(DB_MODE==='JSON'){ const ms=(dbCache.matches||[]).filter(m=>m.user1===me||m.user2===me); const emails=ms.map(m=>m.user1===me?m.user2:m.user1); const users=emails.map(em=>dbCache.users.find(u=>u.email===em)).filter(Boolean).map(u=>{ const {password,...s}=u; return s; }); return res.json(users); } else { let emails=[]; for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM matches_${i} WHERE user1=$1 OR user2=$1`,[me]); r.rows.forEach(row=>emails.push(row.user1===me?row.user2:row.user1)); }catch(_){} } let users=[]; for(let em of emails){ const u=await findUserByEmail(em); if(u){ const {password,...s}=u; s.photos=s.photos||[]; users.push(s); } } res.json(users); }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/messages', auth, async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    if(DB_MODE==='JSON'){ const msgs=(dbCache.messages||[]).filter(m=>m.from===me||m.to===me).sort((a,b)=>new Date(a.at)-new Date(b.at)); return res.json(msgs); } else { let all=[]; for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM messages_${i} WHERE from_email=$1 OR to_email=$1 ORDER BY at ASC`,[me]); all.push(...r.rows.map(row=>({from:row.from_email,to:row.to_email,text:row.text,at:row.at}))); }catch(_){} } all.sort((a,b)=>new Date(a.at)-new Date(b.at)); res.json(all); }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/messages/:email', auth, async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase(); const other=req.params.email.toLowerCase();
    if(DB_MODE==='JSON'){ const msgs=(dbCache.messages||[]).filter(m=>(m.from===me&&m.to===other)||(m.from===other&&m.to===me)).sort((a,b)=>new Date(a.at)-new Date(b.at)); return res.json(msgs); } else { let all=[]; for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM messages_${i} WHERE (from_email=$1 AND to_email=$2) OR (from_email=$2 AND to_email=$1) ORDER BY at ASC`,[me,other]); all.push(...r.rows.map(row=>({from:row.from_email,to:row.to_email,text:row.text,at:row.at}))); }catch(_){} } all.sort((a,b)=>new Date(a.at)-new Date(b.at)); res.json(all); }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/messages', auth, async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase(); const {to,text}=req.body; if(!to||!text) return res.status(400).json({error:'Hiányzik'}); const other=to.toLowerCase();
    const newMsg={from:me,to:other,text,at:new Date().toISOString()};
    if(DB_MODE==='JSON'){ dbCache.messages=dbCache.messages||[]; dbCache.messages.push(newMsg); saveDB(dbCache); return res.json({success:true,message:newMsg}); } else { const shard=getShardIndex(me); const pool=pgPools[shard%pgPools.length]; await pool.query(`INSERT INTO messages_${shard}(from_email,to_email,text) VALUES($1,$2,$3)`,[me,other,text]); return res.json({success:true,message:newMsg}); }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/heart', (req,res)=>res.json({ok:true,mode:DB_MODE,shards:SHARD_COUNT}));
app.post('/api/admin/login', async(req,res)=>{
  const {email,password}=req.body; const adminEmail = (process.env.ADMIN_EMAIL || 'robi19920508@gmail.com').toLowerCase(); if(email.toLowerCase()!==adminEmail) return res.status(403).json({error:'Nem admin'}); const adminPass=process.env.ADMIN_PASS||'LovenuxAdmin2026!'; if(password!==adminPass) return res.status(400).json({error:'Hibás jelszó'}); const token=jwt.sign({email:email.toLowerCase(),role:'admin'},JWT_SECRET,{expiresIn:'12h'}); res.json({success:true,token});
});
app.get('*',(req,res)=>{
  const candidates=[path.join(__dirname,'public','index.html'),path.join(__dirname,'index.html'),path.join(process.cwd(),'public','index.html'),path.join(process.cwd(),'index.html'),'/opt/render/project/src/public/index.html','/opt/render/project/src/index.html'];
  for(let p of candidates){ if(fs.existsSync(p)){ return res.sendFile(p); } }
  try{ const fb=path.join(__dirname,'public','index.html'); if(fs.existsSync(fb)) return res.send(fs.readFileSync(fb,'utf8')); }catch(_){} res.send('Lovenux STABLE LIVE');
});
(async()=>{
  if(DB_MODE==='SHARDED'){ try{ await initPG(); }catch(e){ console.error('PG fail',e.message); DB_MODE='JSON'; dbCache=loadDB(); } }
  app.listen(PORT,()=>console.log(`Lovenux STABLE fut:${PORT} MODE:${DB_MODE}`));
})();
