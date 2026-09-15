
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

console.log(`Lovenux 1000Ft starting on ${PORT}`);

const DATA_FILE = path.join(DATA_DIR,'db.json');
function loadDB(){
  try{
    if(!fs.existsSync(DATA_FILE)){
      const init={users:[],payments:[],likes:[],matches:[],messages:[]};
      fs.writeFileSync(DATA_FILE,JSON.stringify(init));
      return init;
    }
    return JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));
  }catch(_){
    return {users:[],payments:[],likes:[],matches:[],messages:[]};
  }
}
function saveDB(db){
  try{
    fs.writeFileSync(DATA_FILE+'.tmp',JSON.stringify(db));
    fs.renameSync(DATA_FILE+'.tmp',DATA_FILE);
    dbCache=db;
  }catch(e){ console.error('saveDB err',e.message); }
}
let dbCache = loadDB();
setInterval(()=>{ if(dbCache) saveDB(dbCache); },15000);

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
  try{
    req.user=jwt.verify(h.replace('Bearer ',''),JWT_SECRET);
    next();
  }catch(e){
    return res.status(401).json({error:'Invalid token'});
  }
}
function calcAge(b){
  try{
    const birth=new Date(b);
    const n=new Date();
    let a=n.getFullYear()-birth.getFullYear();
    if(n.getMonth()<birth.getMonth() || (n.getMonth()===birth.getMonth() && n.getDate()<birth.getDate())) a--;
    return a;
  }catch(_){ return 25; }
}

// HEALTH - must respond fast for Render
app.get('/api/heart',(req,res)=>res.json({ok:true,mode:'JSON',users:dbCache.users.length}));

// hobbies
app.get('/api/hobbies',(req,res)=>res.json(["⚽ Foci","🏀 Kosár","🎾 Tenisz","💪 Edzőterem","🧘 Jóga","🏃 Futás","🚴 Bicikli","🏊 Úszás","🥾 Túrázás","⛷ Síelés","🏂 Snowboard","🧗 Mászás","🥊 Box","🥋 Küzdősport","🎯 Darts","♟ Sakk","🎸 Gitár","🎹 Zongora","🎤 Éneklés","🎧 DJ","🎶 Koncert","🎨 Festés","✏ Rajzolás","📸 Fotózás","🎬 Filmezés","🎭 Színház","💃 Tánc","📚 Olvasás","✍ Írás","✈ Utazás","⛺ Kemping","🏖 Strand","🚗 Road trip","🍳 Főzés","🧁 Sütés","🍷 Bor","☕ Kávé","🍣 Sushi","🥬 Vegán","🔥 BBQ","🎮 Gamer","💻 Programozás","🤖 AI","📱 Tech","🔨 Barkács","🐕 Kutya","🐈 Macska","🐴 Ló","🌱 Kert","🛍 Shopping","👗 Divat","💄 Smink","💆 Spa","🎉 Buli","🍸 Koktél","🎲 Társas","♠ Póker","🎤 Karaoke","🏎 Autó","🏍 Motor","⛵ Hajó","🧩 Puzzle","🎳 Bowling","⛳ Golf","🎣 Horgászat","🏹 Íjászat","📈 Tőzsde","₿ Kriptó","💼 Befektetés","🧠 Pszichológia","🧘 Meditáció","🔮 Spirituális"]));

// register
app.post('/api/register',upload.array('photos',10),async(req,res)=>{
  try{
    const {email,password,city,birth,child,bio,name,gender,looking_for,height,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,hobbies}=req.body;
    if(!email||!password||!city||!birth||!name) return res.status(400).json({error:'Minden *-os mező kell!'});
    const emailL=email.toLowerCase().trim();
    if(dbCache.users.find(u=>u.email===emailL)) return res.status(400).json({error:'Már regisztráltál ezzel az emaillel'});
    if(password.length<8) return res.status(400).json({error:'Jelszó min 8 karakter'});
    const age=calcAge(birth);
    if(age<18) return res.status(400).json({error:'18+ kell legyél'});
    const hashed=await bcrypt.hash(password,10);
    let photos=[];
    if(req.files){ photos=req.files.map(f=>'/uploads/'+f.filename); }
    let hobbyArr=[];
    try{ hobbyArr=JSON.parse(hobbies||'[]'); }catch(_){ hobbyArr=[]; }
    const user={
      email:emailL,password:hashed,name,city,birth,age,gender:gender||'ferfi',looking_for:looking_for||'noket',
      child:child||'Nincs gyerekem',bio:bio||'',photos,hobbies:hobbyArr,
      height:height?parseInt(height):null,body_type,eye_color,hair_color,smoking,drinking,education,job,music,movies,
      is_paid:false,created_at:new Date().toISOString(),last_active:new Date().toISOString()
    };
    dbCache.users.push(user);
    saveDB(dbCache);
    res.json({success:true,email:emailL,needPayment:true});
  }catch(e){ console.error(e); res.status(500).json({error:e.message}); }
});

// barion start
app.post('/api/barion/start',async(req,res)=>{
  try{
    const {email}=req.body;
    const emailL=email.toLowerCase().trim();
    const u=dbCache.users.find(x=>x.email===emailL);
    if(!u) return res.status(404).json({error:'Nincs ilyen user'});
    if(u.is_paid) return res.json({alreadyPaid:true});
    const paymentId='PAY-'+Date.now()+'-'+crypto.randomBytes(4).toString('hex');
    dbCache.payments=dbCache.payments||[];
    dbCache.payments.push({paymentId,email:emailL,amount:1000,status:'Prepared',created_at:new Date().toISOString()});
    saveDB(dbCache);
    res.json({paymentId,testMode:true});
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/barion/confirm',async(req,res)=>{
  try{
    const {paymentId,email}=req.body;
    const emailL=email.toLowerCase().trim();
    const u=dbCache.users.find(x=>x.email===emailL);
    if(!u) return res.status(404).json({error:'Nincs user'});
    u.is_paid=true;
    u.paid_at=new Date().toISOString();
    dbCache.payments=dbCache.payments||[];
    const p=dbCache.payments.find(x=>x.paymentId===paymentId);
    if(p){ p.status='Succeeded'; p.succeeded_at=new Date().toISOString(); }
    saveDB(dbCache);
    const token=jwt.sign({email:emailL},JWT_SECRET,{expiresIn:'30d'});
    res.json({success:true,token,user:{email:emailL}});
  }catch(e){ res.status(500).json({error:e.message}); }
});

// login
app.post('/api/login',async(req,res)=>{
  try{
    const {email,password}=req.body;
    const emailL=email.toLowerCase().trim();
    const u=dbCache.users.find(x=>x.email===emailL);
    if(!u) return res.status(400).json({error:'Nincs ilyen email'});
    const ok=await bcrypt.compare(password,u.password);
    if(!ok) return res.status(400).json({error:'Hibás jelszó'});
    if(!u.is_paid) return res.status(402).json({error:'Még nem fizettél',needPayment:true,email:emailL});
    const token=jwt.sign({email:emailL},JWT_SECRET,{expiresIn:'30d'});
    u.last_active=new Date().toISOString();
    saveDB(dbCache);
    res.json({success:true,token,user:{email:emailL,name:u.name}});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/me',auth,async(req,res)=>{
  try{
    const email=req.user.email.toLowerCase();
    const u=dbCache.users.find(x=>x.email===email);
    if(!u) return res.status(404).json({error:'Nincs user'});
    const {password,...safe}=u;
    res.json(safe);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/discover',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const {minAge=18,maxAge=99,city='',gender='auto',body_type='',education='',smoking='',drinking='',hobbies='[]'}=req.query;
    let list=dbCache.users.filter(u=>u.email!==me && u.is_paid);
    // gender filter
    if(gender==='ferfi') list=list.filter(u=>u.gender==='ferfi');
    else if(gender==='no') list=list.filter(u=>u.gender==='no');
    else {
      // auto: opposite of me
      const myUser=dbCache.users.find(x=>x.email===me);
      if(myUser){
        if(myUser.looking_for==='noket') list=list.filter(u=>u.gender==='no');
        else if(myUser.looking_for==='ferfiakat') list=list.filter(u=>u.gender==='ferfi');
      }
    }
    list=list.filter(u=>u.age>=parseInt(minAge) && u.age<=parseInt(maxAge));
    if(city) list=list.filter(u=>u.city && u.city.toLowerCase().includes(city.toLowerCase()));
    if(body_type) list=list.filter(u=>u.body_type===body_type);
    if(education) list=list.filter(u=>u.education===education);
    if(smoking) list=list.filter(u=>u.smoking===smoking);
    if(drinking) list=list.filter(u=>u.drinking===drinking);
    try{
      const hArr=JSON.parse(hobbies);
      if(hArr.length){
        list=list.filter(u=>u.hobbies && hArr.some(h=>u.hobbies.includes(h)));
      }
    }catch(_){}
    // shuffle a bit
    list=list.sort(()=>Math.random()-0.5).slice(0,50);
    const safe=list.map(u=>{ const {password,...s}=u; return s; });
    res.json({users:safe});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/like',auth,async(req,res)=>{
  try{
    const from=req.user.email.toLowerCase();
    const to=(req.body.toEmail||'').toLowerCase();
    if(!to) return res.status(400).json({error:'Hiányzik'});
    if(from===to) return res.status(400).json({error:'Magad nem'});
    dbCache.likes=dbCache.likes||[];
    if(!dbCache.likes.find(l=>l.from===from && l.to===to)){
      dbCache.likes.push({from,to,at:new Date().toISOString()});
    }
    const other=dbCache.likes.find(l=>l.from===to && l.to===from);
    if(other){
      dbCache.matches=dbCache.matches||[];
      if(!dbCache.matches.find(m=>(m.user1===from && m.user2===to)||(m.user1===to && m.user2===from))){
        dbCache.matches.push({user1:from,user2:to,at:new Date().toISOString()});
      }
      saveDB(dbCache);
      return res.json({success:true,match:true});
    }
    saveDB(dbCache);
    res.json({success:true,match:false});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/matches',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const ms=(dbCache.matches||[]).filter(m=>m.user1===me||m.user2===me);
    const emails=ms.map(m=>m.user1===me?m.user2:m.user1);
    const users=emails.map(em=>dbCache.users.find(u=>u.email===em)).filter(Boolean).map(u=>{ const {password,...s}=u; return s; });
    res.json(users);
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/messages',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const msgs=(dbCache.messages||[]).filter(m=>m.from===me||m.to===me).sort((a,b)=>new Date(a.at)-new Date(b.at));
    res.json(msgs);
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.get('/api/messages/:email',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const other=req.params.email.toLowerCase();
    const msgs=(dbCache.messages||[]).filter(m=>(m.from===me && m.to===other)||(m.from===other && m.to===me)).sort((a,b)=>new Date(a.at)-new Date(b.at));
    res.json(msgs);
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/messages',auth,async(req,res)=>{
  try{
    const me=req.user.email.toLowerCase();
    const {to,text}=req.body;
    if(!to||!text) return res.status(400).json({error:'Hiányzik'});
    const other=to.toLowerCase();
    const newMsg={from:me,to:other,text,at:new Date().toISOString()};
    dbCache.messages=dbCache.messages||[];
    dbCache.messages.push(newMsg);
    saveDB(dbCache);
    res.json({success:true,message:newMsg});
  }catch(e){ res.status(500).json({error:e.message}); }
});

app.post('/api/forgot-password',async(req,res)=>{
  res.json({success:true,message:'Ha létezik az email, küldtünk levelet'});
});

// serve frontend
app.get('*',(req,res)=>{
  const candidates=[
    path.join(__dirname,'public','index.html'),
    path.join(__dirname,'index.html'),
    path.join(process.cwd(),'public','index.html'),
    path.join(process.cwd(),'index.html')
  ];
  for(let p of candidates){
    if(fs.existsSync(p)) return res.sendFile(p);
  }
  res.send('Lovenux 1000Ft LIVE - index.html not found');
});

app.listen(PORT,()=>console.log(`Lovenux 1000Ft fut:${PORT} users:${dbCache.users.length}`));
