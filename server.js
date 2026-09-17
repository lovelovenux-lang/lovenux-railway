
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
const DATA_DIR = path.join(__dirname, 'data');
const UPLOAD_DIR = path.join(__dirname, 'public/uploads');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, {recursive:true});
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, {recursive:true});

// === 1 MILLIARD - SHARDING CONFIG ===
const SHARD_COUNT = parseInt(process.env.SHARD_COUNT || '16', 10);
const DATABASE_URLS = (process.env.DATABASE_URLS || process.env.DATABASE_URL || '').split(',').map(s=>s.trim()).filter(Boolean);
let DB_MODE = DATABASE_URLS.length > 0 ? 'SHARDED' : 'JSON';
let pgPools = [];
let dbCache = null;

function hashEmail(email){
  const h = crypto.createHash('md5').update(email.toLowerCase()).digest('hex');
  return parseInt(h.slice(0,8),16) % SHARD_COUNT;
}
function getShardIndex(email){ return hashEmail(email); }

async function initPG(){
  if(DB_MODE!=='SHARDED') return;
  console.log(`[1B] Init ${SHARD_COUNT} shards from ${DATABASE_URLS.length} URLs`);
  try{
    const {Pool} = require('pg');
    pgPools = DATABASE_URLS.map(u=>new Pool({connectionString:u,max:20,idleTimeoutMillis:30000}));
    for(let i=0;i<SHARD_COUNT;i++){
      const pool = pgPools[i % pgPools.length];
      try{
        await pool.query(`
          CREATE TABLE IF NOT EXISTS users_${i}(
            id BIGSERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            name TEXT, city TEXT, birth DATE, age INT,
            gender TEXT DEFAULT 'ferfi', looking_for TEXT DEFAULT 'noket',
            child TEXT, bio TEXT, photos JSONB DEFAULT '[]', hobbies JSONB DEFAULT '[]',
            height INT, body_type TEXT, eye_color TEXT, hair_color TEXT,
            smoking TEXT, drinking TEXT, education TEXT, job TEXT,
            music TEXT, movies TEXT,
            is_paid BOOLEAN DEFAULT FALSE, paid_at TIMESTAMPTZ,
            shard INT DEFAULT ${i}, created_at TIMESTAMPTZ DEFAULT NOW(), last_active TIMESTAMPTZ DEFAULT NOW()
          );
          CREATE TABLE IF NOT EXISTS payments_${i}(id BIGSERIAL PRIMARY KEY,payment_id TEXT UNIQUE,email TEXT,amount INT DEFAULT 1000,status TEXT DEFAULT 'Prepared',created_at TIMESTAMPTZ DEFAULT NOW(),succeeded_at TIMESTAMPTZ);
          CREATE TABLE IF NOT EXISTS likes_${i}(id BIGSERIAL PRIMARY KEY,from_email TEXT,to_email TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),UNIQUE(from_email,to_email));
          CREATE TABLE IF NOT EXISTS matches_${i}(id BIGSERIAL PRIMARY KEY,user1 TEXT,user2 TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),UNIQUE(user1,user2));
          CREATE TABLE IF NOT EXISTS messages_${i}(id BIGSERIAL PRIMARY KEY,from_email TEXT,to_email TEXT,text TEXT,at TIMESTAMPTZ DEFAULT NOW());
          CREATE INDEX IF NOT EXISTS idx_users_${i}_email ON users_${i}(email);
          CREATE INDEX IF NOT EXISTS idx_users_${i}_paid ON users_${i}(is_paid);
          CREATE INDEX IF NOT EXISTS idx_users_${i}_city ON users_${i}(city);
          CREATE INDEX IF NOT EXISTS idx_users_${i}_gender ON users_${i}(gender);
        `);
        // add missing cols
        const cols=['hobbies','height','body_type','eye_color','hair_color','smoking','drinking','education','job','music','movies'];
        for(let col of cols){
          try{ await pool.query(`ALTER TABLE users_${i} ADD COLUMN IF NOT EXISTS ${col} ${col==='height'?'INT': col==='hobbies'?'JSONB DEFAULT \'[]\'' : 'TEXT'}`); }catch(_){}
        }
      }catch(e){ console.error(`[Shard ${i}] init error:`,e.message); }
    }
    console.log(`[1B] Shards ready`);
  }catch(e){
    console.error('[1B] PG init failed, fallback to JSON:',e.message);
    DB_MODE='JSON';
    loadJSON();
  }
}

function loadJSON(){
  try{
    const DATA_FILE = path.join(DATA_DIR,'db.json');
    if(!fs.existsSync(DATA_FILE)){
      const init={users:[],payments:[],likes:[],matches:[],messages:[]};
      fs.writeFileSync(DATA_FILE,JSON.stringify(init));
      dbCache=init; return init;
    }
    dbCache=JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));
    return dbCache;
  }catch(_){
    dbCache={users:[],payments:[],likes:[],matches:[],messages:[]};
    return dbCache;
  }
}
function saveJSON(){
  try{
    const DATA_FILE = path.join(DATA_DIR,'db.json');
    fs.writeFileSync(DATA_FILE+'.tmp',JSON.stringify(dbCache));
    fs.renameSync(DATA_FILE+'.tmp',DATA_FILE);
  }catch(e){ console.error('saveJSON',e.message); }
}
if(DB_MODE==='JSON'){ loadJSON(); setInterval(()=>{ if(dbCache) saveJSON(); },15000); }

console.log(`Lovenux 1B Mode: ${DB_MODE} | Shards:${SHARD_COUNT} | Port:${PORT}`);

app.use(helmet({contentSecurityPolicy:false}));
app.use(compression());
app.use(morgan('dev'));
app.use(cors({origin:true,credentials:true}));
app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true}));
app.use('/uploads', express.static(UPLOAD_DIR));
app.use(express.static(path.join(__dirname,'public')));

const storage=multer.diskStorage({
  destination:(r,f,cb)=>cb(null,UPLOAD_DIR),
  filename:(r,f,cb)=>cb(null,Date.now()+'-'+crypto.randomBytes(6).toString('hex')+path.extname(f.originalname||'.jpg'))
});
const upload=multer({storage,limits:{fileSize:8*1024*1024,files:10}});

function auth(req,res,next){
  const h=req.headers.authorization;
  if(!h) return res.status(401).json({error:'No token'});
  try{ req.user=jwt.verify(h.replace('Bearer ',''),JWT_SECRET); next(); }catch(e){ return res.status(401).json({error:'Invalid'}); }
}
function calcAge(b){
  try{ const birth=new Date(b); const n=new Date(); let a=n.getFullYear()-birth.getFullYear(); if(n.getMonth()<birth.getMonth() || (n.getMonth()===birth.getMonth() && n.getDate()<birth.getDate())) a--; return a; }catch(_){ return 25; }
}

async function findUserByEmail(email){
  email=email.toLowerCase();
  if(DB_MODE==='JSON') return dbCache.users.find(u=>u.email===email)||null;
  const shard=getShardIndex(email);
  for(let i=0;i<SHARD_COUNT;i++){
    const idx=(shard+i)%SHARD_COUNT;
    try{
      const pool=pgPools[idx%pgPools.length];
      const r=await pool.query(`SELECT * FROM users_${idx} WHERE email=$1`,[email]);
      if(r.rows[0]) return r.rows[0];
    }catch(_){}
  }
  return null;
}

app.get('/api/heart',(req,res)=>res.json({ok:true,mode:DB_MODE,shards:SHARD_COUNT,users:DB_MODE==='JSON'?dbCache.users.length:'sharded'}));

app.get('/api/hobbies',(req,res)=>res.json(["⚽ Foci","🏀 Kosár","🎾 Tenisz","💪 Edzőterem","🧘 Jóga","🏃 Futás","🚴 Bicikli","🏊 Úszás","🥾 Túrázás","⛷ Síelés","🏂 Snowboard","🧗 Mászás","🥊 Box","🥋 Küzdősport","🎯 Darts","♟ Sakk","🎸 Gitár","🎹 Zongora","🎤 Éneklés","🎧 DJ","🎶 Koncert","🎨 Festés","✏ Rajzolás","📸 Fotózás","🎬 Filmezés","🎭 Színház","💃 Tánc","📚 Olvasás","✍ Írás","✈ Utazás","⛺ Kemping","🏖 Strand","🚗 Road trip","🍳 Főzés","🧁 Sütés","🍷 Bor","☕ Kávé","🍣 Sushi","🥬 Vegán","🔥 BBQ","🎮 Gamer","💻 Programozás","🤖 AI","📱 Tech","🔨 Barkács","🐕 Kutya","🐈 Macska","🐴 Ló","🌱 Kert","🛍 Shopping","👗 Divat","💄 Smink","💆 Spa","🎉 Buli","🍸 Koktél","🎲 Társas","♠ Póker","🎤 Karaoke","🏎 Autó","🏍 Motor","⛵ Hajó","🧩 Puzzle","🎳 Bowling","⛳ Golf","🎣 Horgászat","🏹 Íjászat","📈 Tőzsde","₿ Kriptó","🧠 Pszichológia","🧘 Meditáció","🔮 Spirituális"]));

app.post('/api/register',upload.array('photos',10),async(req,res)=>{
  try{
    const {email,password,city,birth,child,bio,name,gender,looking_for,height,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,hobbies}=req.body;
    if(!email||!password||!city||!birth||!name) return res.status(400).json({error:'Minden *-os mező kell!'});
    const emailL=email.toLowerCase().trim();
    if(await findUserByEmail(emailL)) return res.status(400).json({error:'Már regisztráltál'});
    if(password.length<8) return res.status(400).json({error:'Jelszó min 8'});
    const age=calcAge(birth); if(age<18) return res.status(400).json({error:'18+ kell'});
    const hashed=await bcrypt.hash(password,10);
    let photos=[]; if(req.files){ photos=req.files.map(f=>'/uploads/'+f.filename); }
    let hobbyArr=[]; try{ hobbyArr=JSON.parse(hobbies||'[]'); }catch(_){}
    if(DB_MODE==='JSON'){
      const user={email:emailL,password:hashed,name,city,birth,age,gender:gender||'ferfi',looking_for:looking_for||'noket',child,bio,photos,hobbies:hobbyArr,height:height?parseInt(height):null,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,is_paid:false,created_at:new Date().toISOString(),last_active:new Date().toISOString()};
      dbCache.users.push(user); saveJSON();
      return res.json({success:true,email:emailL,needPayment:true});
    }else{
      const shard=getShardIndex(emailL);
      const pool=pgPools[shard%pgPools.length];
      await pool.query(`INSERT INTO users_${shard}(email,password,name,city,birth,age,gender,looking_for,child,bio,photos,hobbies,height,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,is_paid,shard) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,false,$24)`,
      [emailL,hashed,name,city,birth,age,gender,looking_for,child,bio,JSON.stringify(photos),JSON.stringify(hobbyArr),height?parseInt(height):null,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,shard]);
      return res.json({success:true,email:emailL,needPayment:true});
    }
  }catch(e){ console.error(e); res.status(500).json({error:e.message}); }
});

app.post('/api/barion/start',async(req,res)=>{
  try{
    const emailL=req.body.email.toLowerCase().trim();
    const u=await findUserByEmail(emailL);
    if(!u) return res.status(404).json({error:'Nincs user'});
    if(u.is_paid) return res.json({alreadyPaid:true});
    const paymentId='PAY-'+Date.now()+'-'+crypto.randomBytes(4).toString('hex');
    if(DB_MODE==='JSON'){
      dbCache.payments=dbCache.payments||[];
      dbCache.payments.push({paymentId,email:emailL,amount:1000,status:'Prepared',created_at:new Date().toISOString()}); saveJSON();
    }else{
      const shard=getShardIndex(emailL); const pool=pgPools[shard%pgPools.length];
      await pool.query(`INSERT INTO payments_${shard}(payment_id,email,amount,status) VALUES($1,$2,1000,'Prepared') ON CONFLICT DO NOTHING`,[paymentId,emailL]);
    }
    res.json({paymentId,testMode:true});
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/barion/confirm',async(req,res)=>{
  try{
    const {paymentId,email}=req.body; const emailL=email.toLowerCase().trim();
    if(DB_MODE==='JSON'){
      const u=dbCache.users.find(x=>x.email===emailL); if(!u) return res.status(404).json({error:'Nincs user'});
      u.is_paid=true; u.paid_at=new Date().toISOString();
      const p=(dbCache.payments||[]).find(x=>x.paymentId===paymentId); if(p){ p.status='Succeeded'; p.succeeded_at=new Date().toISOString(); }
      saveJSON();
    }else{
      const shard=getShardIndex(emailL); const pool=pgPools[shard%pgPools.length];
      await pool.query(`UPDATE users_${shard} SET is_paid=true, paid_at=NOW() WHERE email=$1`,[emailL]);
      await pool.query(`UPDATE payments_${shard} SET status='Succeeded', succeeded_at=NOW() WHERE payment_id=$1`,[paymentId]);
    }
    const token=jwt.sign({email:emailL},JWT_SECRET,{expiresIn:'30d'});
    res.json({success:true,token,user:{email:emailL}});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/login',async(req,res)=>{
  try{
    const emailL=req.body.email.toLowerCase().trim();
    const u=await findUserByEmail(emailL);
    if(!u) return res.status(400).json({error:'Nincs email'});
    const ok=await bcrypt.compare(req.body.password,u.password);
    if(!ok) return res.status(400).json({error:'Hibás jelszó'});
    if(!u.is_paid) return res.status(402).json({error:'Még nem fizettél',needPayment:true,email:emailL});
    const token=jwt.sign({email:emailL},JWT_SECRET,{expiresIn:'30d'});
    if(DB_MODE==='JSON'){ const uu=dbCache.users.find(x=>x.email===emailL); if(uu) uu.last_active=new Date().toISOString(); saveJSON(); }
    else{ const shard=getShardIndex(emailL); const pool=pgPools[shard%pgPools.length]; await pool.query(`UPDATE users_${shard} SET last_active=NOW() WHERE email=$1`,[emailL]); }
    res.json({success:true,token,user:{email:emailL,name:u.name}});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/me',auth,async(req,res)=>{
  try{
    const u=await findUserByEmail(req.user.email);
    if(!u) return res.status(404).json({error:'Nincs user'});
    const {password,...safe}=u; res.json(safe);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/discover',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const {minAge=18,maxAge=99,city='',gender='auto',body_type='',education='',smoking='',drinking='',hobbies='[]'}=req.query;
    if(DB_MODE==='JSON'){
      let list=dbCache.users.filter(u=>u.email!==me && u.is_paid);
      if(gender==='ferfi') list=list.filter(u=>u.gender==='ferfi');
      else if(gender==='no') list=list.filter(u=>u.gender==='no');
      else { const my=dbCache.users.find(x=>x.email===me); if(my){ if(my.looking_for==='noket') list=list.filter(u=>u.gender==='no'); else if(my.looking_for==='ferfiakat') list=list.filter(u=>u.gender==='ferfi'); } }
      list=list.filter(u=>u.age>=parseInt(minAge)&&u.age<=parseInt(maxAge));
      if(city) list=list.filter(u=>u.city&&u.city.toLowerCase().includes(city.toLowerCase()));
      if(body_type) list=list.filter(u=>u.body_type===body_type);
      if(education) list=list.filter(u=>u.education===education);
      if(smoking) list=list.filter(u=>u.smoking===smoking);
      if(drinking) list=list.filter(u=>u.drinking===drinking);
      try{ const hArr=JSON.parse(hobbies); if(hArr.length) list=list.filter(u=>u.hobbies&&hArr.some(h=>u.hobbies.includes(h))); }catch(_){}
      list=list.sort(()=>Math.random()-0.5).slice(0,50);
      const safe=list.map(u=>{ const {password,...s}=u; return s; });
      return res.json({users:safe});
    }else{
      // SHARDED - query all shards for demo, in production use search service
      let all=[];
      for(let i=0;i<SHARD_COUNT;i++){
        try{
          const pool=pgPools[i%pgPools.length];
          // === NŐK FÉRFIAKAT, FÉRFIAK NŐKET ===
          let meGender = null;
          try{
            const mePool = pgPools[getShardIndex(me)%pgPools.length];
            const meR = await mePool.query(`SELECT gender FROM users_${getShardIndex(me)} WHERE email=$1`,[me]);
            if(meR.rows[0]) meGender = meR.rows[0].gender;
          }catch(_){}
          let q=`SELECT * FROM users_${i} WHERE is_paid=true AND email!=$1 AND age BETWEEN $2 AND $3`;
          let params=[me,parseInt(minAge),parseInt(maxAge)]; let idx=4;
          if(gender==='ferfi'){ q+=` AND gender='ferfi'`; }
          else if(gender==='no'){ q+=` AND gender='no'`; }
          else {
            // auto -> ellenkező nem
            if(meGender==='ferfi') q+=` AND gender='no'`;
            else if(meGender==='no') q+=` AND gender='ferfi'`;
          }
          if(city){ q+=` AND city ILIKE $${idx}`; params.push(`%${city}%`); idx++; }
          if(body_type){ q+=` AND body_type=$${idx}`; params.push(body_type); idx++; }
          q+=` LIMIT 10`;
          const r=await pool.query(q,params);
          all.push(...r.rows);
        }catch(_){}
      }
      all=all.sort(()=>Math.random()-0.5).slice(0,50);
      res.json({users:all.map(u=>{ const {password,...s}=u; return s; })});
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/like',auth,async(req,res)=>{
  try{
    const from=req.user.email.toLowerCase(); const to=(req.body.toEmail||'').toLowerCase();
    if(!to||from===to) return res.status(400).json({error:'Hiba'});
    if(DB_MODE==='JSON'){
      dbCache.likes=dbCache.likes||[];
      if(!dbCache.likes.find(l=>l.from===from&&l.to===to)) dbCache.likes.push({from,to,at:new Date().toISOString()});
      const other=dbCache.likes.find(l=>l.from===to&&l.to===from);
      if(other){
        dbCache.matches=dbCache.matches||[];
        if(!dbCache.matches.find(m=>(m.user1===from&&m.user2===to)||(m.user1===to&&m.user2===from))) dbCache.matches.push({user1:from,user2:to,at:new Date().toISOString()});
        saveJSON(); return res.json({success:true,match:true});
      }
      saveJSON(); return res.json({success:true,match:false});
    }else{
      const shard=getShardIndex(from); const pool=pgPools[shard%pgPools.length];
      await pool.query(`INSERT INTO likes_${shard}(from_email,to_email) VALUES($1,$2) ON CONFLICT DO NOTHING`,[from,to]);
      let found=false;
      for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM likes_${i} WHERE from_email=$1 AND to_email=$2`,[to,from]); if(r.rows.length){ found=true; break; } }catch(_){} }
      if(found){
        const shardM=getShardIndex(from); const poolM=pgPools[shardM%pgPools.length];
        const u1=from<to?from:to; const u2=from<to?to:from;
        await poolM.query(`INSERT INTO matches_${shardM}(user1,user2) VALUES($1,$2) ON CONFLICT DO NOTHING`,[u1,u2]);
        return res.json({success:true,match:true});
      }
      return res.json({success:true,match:false});
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/matches',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    if(DB_MODE==='JSON'){
      const ms=(dbCache.matches||[]).filter(m=>m.user1===me||m.user2===me);
      const emails=ms.map(m=>m.user1===me?m.user2:m.user1);
      const users=emails.map(em=>dbCache.users.find(u=>u.email===em)).filter(Boolean).map(u=>{ const {password,...s}=u; return s; });
      return res.json(users);
    }else{
      let emails=[];
      for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM matches_${i} WHERE user1=$1 OR user2=$1`,[me]); r.rows.forEach(row=>emails.push(row.user1===me?row.user2:row.user1)); }catch(_){} }
      let users=[];
      for(let em of emails){ const u=await findUserByEmail(em); if(u){ const {password,...s}=u; users.push(s); } }
      return res.json(users);
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/messages',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    if(DB_MODE==='JSON'){
      const msgs=(dbCache.messages||[]).filter(m=>m.from===me||m.to===me).sort((a,b)=>new Date(a.at)-new Date(b.at));
      return res.json(msgs);
    }else{
      let all=[];
      for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM messages_${i} WHERE from_email=$1 OR to_email=$1 ORDER BY at ASC`,[me]); all.push(...r.rows.map(row=>({from:row.from_email,to:row.to_email,text:row.text,at:row.at}))); }catch(_){} }
      all.sort((a,b)=>new Date(a.at)-new Date(b.at)); return res.json(all);
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/messages/:email',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase(); const other=req.params.email.toLowerCase();
    if(DB_MODE==='JSON'){
      const msgs=(dbCache.messages||[]).filter(m=>(m.from===me&&m.to===other)||(m.from===other&&m.to===me)).sort((a,b)=>new Date(a.at)-new Date(b.at));
      return res.json(msgs);
    }else{
      let all=[];
      for(let i=0;i<SHARD_COUNT;i++){ try{ const p=pgPools[i%pgPools.length]; const r=await p.query(`SELECT * FROM messages_${i} WHERE (from_email=$1 AND to_email=$2) OR (from_email=$2 AND to_email=$1) ORDER BY at ASC`,[me,other]); all.push(...r.rows.map(row=>({from:row.from_email,to:row.to_email,text:row.text,at:row.at}))); }catch(_){} }
      all.sort((a,b)=>new Date(a.at)-new Date(b.at)); return res.json(all);
    }
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/messages',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase(); const {to,text}=req.body; const other=to.toLowerCase();
    const newMsg={from:me,to:other,text,at:new Date().toISOString()};
    if(DB_MODE==='JSON'){ dbCache.messages=dbCache.messages||[]; dbCache.messages.push(newMsg); saveJSON(); return res.json({success:true,message:newMsg}); }
    else{ const shard=getShardIndex(me); const pool=pgPools[shard%pgPools.length]; await pool.query(`INSERT INTO messages_${shard}(from_email,to_email,text) VALUES($1,$2,$3)`,[me,other,text]); return res.json({success:true,message:newMsg}); }
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/forgot-password',(req,res)=>res.json({success:true}));

app.get('*',(req,res)=>{
  const candidates=[path.join(__dirname,'public','index.html'),path.join(__dirname,'index.html')];
  for(let p of candidates){ if(fs.existsSync(p)) return res.sendFile(p); }
  res.send('Lovenux 1B LIVE');
});

(async()=>{
  if(DB_MODE==='SHARDED'){ try{ await initPG(); }catch(e){ console.error(e); DB_MODE='JSON'; loadJSON(); } }
  app.listen(PORT,()=>console.log(`Lovenux 1B ${DB_MODE} fut:${PORT} shards:${SHARD_COUNT} users:${DB_MODE==='JSON'?dbCache.users.length:'sharded'}`));
})();
