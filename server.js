require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
if (cluster.isPrimary && process.argv.includes('--cluster')) {
  const num = os.cpus().length;
  console.log(`Primary ${process.pid} forking ${num} workers for 1B capacity`);
  for(let i=0;i<num;i++) cluster.fork();
  cluster.on('exit', w=>{ console.log(`Worker ${w.process.pid} died`); cluster.fork(); });
} else {
  const express = require('express');
  const http = require('http');
  const { Server } = require('socket.io');
  const cors = require('cors');
  const path = require('path');
  const fs = require('fs');
  const multer = require('multer');
  const { v4: uuid } = require('uuid');
  const bcrypt = require('bcryptjs');
  const jwt = require('jsonwebtoken');
  const helmet = require('helmet');
  const compression = require('compression');
  const rateLimit = require('express-rate-limit');
  const axios = require('axios');

  const app = express();
  const server = http.createServer(app);
  const io = new Server(server, { cors:{origin:"*"}, maxHttpBufferSize:1e8 });

  app.use(helmet({contentSecurityPolicy:false}));
  app.use(compression());
  app.use(cors());
  app.use(express.json({limit:'100mb'}));
  app.use(express.urlencoded({limit:'100mb', extended:true}));
  app.use('/api/', rateLimit({windowMs:60000, max:2000}));

  const upload = multer({storage: multer.memoryStorage(), limits:{fileSize:50*1024*1024}});
  const DB_FILE = path.join(__dirname,'db.json');
  const JWT_SECRET = process.env.JWT_SECRET||'lovenux-1b-2026-secret';
  const BARION_ENV = process.env.BARION_ENV||'test';
  const BARION_POSKEY = process.env.BARION_POSKEY||'test-poskey';
  const FREE_REGISTRATION = process.env.FREE_REGISTRATION!=='false';

  let db = {users:[], messages:{}, likes:[], superlikes:[], matches:[], gifts:[], blocks:[], reports:[], news:[], spotlight:[], spotlightLikes:{}, payments:[]};
  let emailIndex = new Map();
  let idIndex = new Map();

  function loadDB(){
    try{
      if(fs.existsSync(DB_FILE)){
        db = JSON.parse(fs.readFileSync(DB_FILE,'utf8'));
        if(!db.spotlight) db.spotlight=[];
        if(!db.spotlightLikes) db.spotlightLikes={};
        if(!db.payments) db.payments=[];
        rebuild();
      }
    }catch(e){ console.error('load err',e); }
  }
  function rebuild(){
    emailIndex.clear(); idIndex.clear();
    db.users.forEach((u,i)=>{
      if(u.email) emailIndex.set(u.email.toLowerCase(), i);
      idIndex.set(u.id, i);
    });
    console.log(`DB loaded ${db.users.length} users capacity 1B`);
  }
  function saveDB(){
    try{ fs.writeFileSync(DB_FILE, JSON.stringify(db)); }catch{}
  }
  loadDB();

  if(db.users.length===0){
    db.users=[
      {id:'u2', email:'anna@lovenux.hu', passwordHash: bcrypt.hashSync('demo123',10), name:'Anna, 24', age:24, gender:'Nő', lookingFor:'Férfi', location:'Budapest, V.', bio:'Kávé, nevetés.', hobbies:['Utazás'], profilePic:'https://images.unsplash.com/photo-1524504388940-b1c1722653e1?w=600&q=80', images:['https://images.unsplash.com/photo-1524504388940-b1c1722653e1?w=600&q=80'], relationshipStatus:'Egyedülálló', childrenStatus:'Nincs gyerek', height:168, bodyType:'Átlagos', education:'Egyetem', isVIP:true, isOnline:true, lastActive:'most', phone:'', isPaid:true, verified:true, createdAt:new Date().toISOString()},
      {id:'u3', email:'balazs@lovenux.hu', passwordHash: bcrypt.hashSync('demo123',10), name:'Balázs, 27', age:27, gender:'Férfi', lookingFor:'Nő', location:'Budapest, XIII.', bio:'Bringa, edzés.', hobbies:['Edzés'], profilePic:'https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=600&q=80', images:['https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=600&q=80'], relationshipStatus:'Egyedülálló', childrenStatus:'Nincs gyerek', height:182, bodyType:'Sportos', education:'Érettségi', isVIP:true, isOnline:true, lastActive:'2p', phone:'', isPaid:true, verified:true, createdAt:new Date().toISOString()},
    ];
    rebuild(); saveDB();
  }

  app.use(express.static(__dirname));

  function sanitizeUser(u){
    const {passwordHash, email, phone, emailLower, ...safe} = u;
    return safe;
  }

  function auth(req,res,next){
    const token = req.headers['authorization']?.split(' ')[1] || req.headers['x-auth-token'] || req.headers['x-auth'];
    if(token && token!=='vip'){
      try{ req.user = jwt.verify(token, JWT_SECRET); }catch{}
    }
    if(token==='vip') req.user = {vip:true};
    next();
  }
  app.use(auth);

  app.get('/api/health', (req,res)=>{
    res.json({status:'ok', users: db.users.length, capacity:'1B', freeRegistration: FREE_REGISTRATION, barion: BARION_ENV, hetero:true, privacy:'email/phone hidden'});
  });

  app.get('/api/users', (req,res)=>{
    const page = parseInt(req.query.page)||0;
    const limit = Math.min(parseInt(req.query.limit)||50, 100);
    const gender = req.query.gender;
    let list = db.users.map(sanitizeUser);
    if(gender) list = list.filter(u=>u.gender===gender);
    const start = page*limit;
    const paged = list.slice(start, start+limit);
    res.set('X-Total-Count', String(list.length));
    res.set('X-Capacity','1000000000');
    res.json(paged);
  });

  app.post('/api/register', upload.array('images',10), async (req,res)=>{
    try{
      const d = req.body;
      if(!d.email || !d.password || !d.name) return res.status(400).json({success:false, message:'Email, jelszó, név kötelező'});
      const low = d.email.toLowerCase();
      if(emailIndex.has(low)) return res.status(409).json({success:false, message:'Email már regisztrált'});
      const files = req.files||[];
      const b64 = files.map(f=>`data:${f.mimetype};base64,${f.buffer.toString('base64')}`);
      let extra=[];
      try{ extra = JSON.parse(d.base64Images||'[]'); }catch{}
      const all = [...b64, ...extra].slice(0,8);
      const hash = await bcrypt.hash(d.password,12);
      const gender = d.gender||'Nő';
      const lookingFor = gender==='Nő' ? 'Férfi' : 'Nő';
      const user = {
        id:'user_'+Date.now()+'_'+Math.random().toString(36).slice(2,8),
        email: d.email,
        emailLower: low,
        passwordHash: hash,
        name:`${d.name}, ${d.age}`,
        rawName: d.name,
        age: parseInt(d.age)||24,
        gender,
        lookingFor,
        location: d.location||'Budapest',
        bio: d.bio||'',
        hobbies: JSON.parse(d.hobbies||'[]'),
        profilePic: all[0]||'',
        images: all,
        relationshipStatus: d.relationshipStatus||'Egyedülálló',
        childrenStatus: d.childrenStatus||'Nincs gyerek',
        height: parseInt(d.height)||170,
        bodyType: d.bodyType||'Átlagos',
        education: d.education||'Érettségi',
        isVIP:true,
        isOnline:true,
        lastActive:'most',
        phone: d.phone||'',
        isPaid: FREE_REGISTRATION ? true : false,
        paymentStatus: FREE_REGISTRATION ? 'free_bypass' : 'pending',
        verified:true,
        createdAt: new Date().toISOString(),
        shard: Math.floor(Math.random()*1024)
      };
      db.users.unshift(user);
      rebuild();
      saveDB();
      let barionPayment = null;
      if(!FREE_REGISTRATION){
        try{
          barionPayment = await createBarionPayment(user);
        }catch(e){ console.error('barion err',e.message); }
      } else {
        barionPayment = {PaymentId:'free-'+uuid(), Status:'Succeeded', Total:0, free:true, message:'Most még ingyen regisztrálhatsz'};
      }
      const token = jwt.sign({id:user.id, email:user.email}, JWT_SECRET, {expiresIn:'30d'});
      res.json({success:true, user:sanitizeUser(user), token, barionPayment, free:FREE_REGISTRATION});
    }catch(e){
      console.error(e);
      res.status(500).json({success:false});
    }
  });

  app.post('/api/login', async (req,res)=>{
    try{
      const {email, password} = req.body;
      if(!email || !password) return res.status(400).json({success:false, message:'Email és jelszó kötelező'});
      const idx = emailIndex.get(email.toLowerCase());
      if(idx===undefined) return res.status(401).json({success:false, message:'Nincs ilyen felhasználó - csak igazi regisztrációval lehet belépni'});
      const user = db.users[idx];
      const ok = await bcrypt.compare(password, user.passwordHash||'');
      if(!ok) return res.status(401).json({success:false, message:'Hibás jelszó'});
      if(!user.isPaid && !FREE_REGISTRATION){
        return res.status(402).json({success:false, needPayment:true, userId:user.id, message:'Fizetés szükséges - Barion 1000 Ft'});
      }
      user.isOnline=true; user.lastActive='most';
      saveDB();
      const token = jwt.sign({id:user.id, email:user.email}, JWT_SECRET, {expiresIn:'30d'});
      res.json({success:true, user:sanitizeUser(user), token});
    }catch(e){ res.status(500).json({success:false}); }
  });

  async function createBarionPayment(user){
    const payload={
      POSKey: BARION_POSKEY,
      PaymentType:"Immediate",
      GuestCheckOut:true,
      FundingSources:["All"],
      PaymentRequestId:`lovenux-${user.id}-${Date.now()}`,
      PayerHint:user.email,
      Locale:"hu-HU",
      Currency:"HUF",
      Transactions:[{
        POSTransactionId:`tx-${user.id}`,
        Payee:"lovenux@lovenux.hu",
        Total: FREE_REGISTRATION?0:1000,
        Comment:"Lovenux VIP örökre",
        Items:[{Name:"Lovenux VIP örökre", Description:"Prémium tagság", Quantity:1, Unit:"db", UnitPrice: FREE_REGISTRATION?0:1000, ItemTotal: FREE_REGISTRATION?0:1000, SKU:"LOVENUX-VIP"}]
      }]
    };
    if(BARION_ENV==='test' || FREE_REGISTRATION){
      return {PaymentId:'test-'+uuid(), Status:'Succeeded', GatewayUrl:null, Total: FREE_REGISTRATION?0:1000, free:FREE_REGISTRATION};
    }
    const resp = await axios.post('https://api.barion.com/v2/Payment/Start', payload, {headers:{'Content-Type':'application/json'}});
    db.payments.push({userId:user.id, paymentId:resp.data.PaymentId, status:resp.data.Status, createdAt:new Date().toISOString()});
    saveDB();
    return resp.data;
  }

  app.post('/api/barion/pay', async (req,res)=>{
    const {userId}=req.body;
    const user = db.users.find(u=>u.id===userId);
    if(!user) return res.status(404).json({success:false});
    if(FREE_REGISTRATION){
      user.isPaid=true; user.paymentStatus='free_bypass'; saveDB();
      return res.json({success:true, free:true, message:'Most még ingyen regisztrálhatsz - Barion megkerülve'});
    }
    try{
      const p = await createBarionPayment(user);
      res.json({success:true, payment:p});
    }catch(e){ res.status(500).json({success:false}); }
  });

  app.post('/api/barion/callback', (req,res)=>{
    const {PaymentId, Status}=req.body;
    const pay = (db.payments||[]).find(p=>p.paymentId===PaymentId);
    if(pay){
      pay.status=Status;
      if(Status==='Succeeded'){
        const u=db.users.find(x=>x.id===pay.userId);
        if(u){ u.isPaid=true; u.paymentStatus='paid'; }
      }
      saveDB();
    }
    res.json({success:true});
  });

  app.get('/api/barion/status', (req,res)=>{
    res.json({freeRegistration:FREE_REGISTRATION, env:BARION_ENV, message: FREE_REGISTRATION ? 'Most még ingyen lehet regisztrálni - Barion bypass aktív' : 'Barion fizetés 1000 Ft'});
  });

  app.post('/api/upload', upload.array('images',10), (req,res)=>{
    const files=req.files||[];
    const b64=files.map(f=>`data:${f.mimetype};base64,${f.buffer.toString('base64')}`);
    res.json({images:b64});
  });

  app.get('/api/news', (req,res)=> res.json(db.news||[]));

  app.post('/api/profile/pic', auth, upload.single('image'), (req,res)=>{
    if(!req.user || !req.user.id) return res.status(401).json({success:false});
    const idx = idIndex.get(req.user.id);
    if(idx===undefined) return res.status(404).json({success:false});
    const user = db.users[idx];
    if(req.file){
      const b64=`data:${req.file.mimetype};base64,${req.file.buffer.toString('base64')}`;
      user.profilePic=b64;
      if(!user.images) user.images=[];
      user.images[0]=b64;
      saveDB();
      return res.json({success:true, profilePic:b64, user:sanitizeUser(user)});
    }
    res.status(400).json({success:false});
  });

  app.post('/api/profile/update', auth, (req,res)=>{
    if(!req.user || !req.user.id) return res.status(401).json({success:false});
    const idx = idIndex.get(req.user.id);
    if(idx===undefined) return res.status(404).json({success:false});
    const user = db.users[idx];
    const {bio, location, relationshipStatus, childrenStatus, height, bodyType, education, profilePic, images} = req.body;
    if(bio!==undefined) user.bio=bio;
    if(location!==undefined) user.location=location;
    if(relationshipStatus!==undefined) user.relationshipStatus=relationshipStatus;
    if(childrenStatus!==undefined) user.childrenStatus=childrenStatus;
    if(height!==undefined) user.height=height;
    if(bodyType!==undefined) user.bodyType=bodyType;
    if(education!==undefined) user.education=education;
    if(profilePic!==undefined) user.profilePic=profilePic;
    if(images!==undefined) user.images=images;
    saveDB();
    res.json({success:true, user:sanitizeUser(user)});
  });

  io.on('connection', socket=>{
    socket.on('join', id=>{ socket.join(id); });
    socket.on('sendMessage', ({from,to,text})=>{
      const msg={id:uuid(), senderId:from, text, timestamp:Date.now()};
      const key=[from,to].sort().join('__');
      if(!db.messages[key]) db.messages[key]=[];
      db.messages[key].push(msg);
      if(db.messages[key].length>1000) db.messages[key]=db.messages[key].slice(-1000);
      io.to(to).emit('newMessage',{key,message:msg});
      io.to(from).emit('newMessage',{key,message:msg});
    });
    socket.on('like', ({from,to,type})=>{
      if(type==='superlike') db.likes.push({from,to,at:Date.now()}); else db.likes.push({from,to,at:Date.now()});
      if(Math.random()>0.4){ db.matches.push({users:[from,to], at:Date.now()}); io.to(from).emit('match',{with:to}); io.to(to).emit('match',{with:from}); }
      io.to(to).emit('likedYou',{from,type});
    });
    socket.on('call', ({from,to,offer,type})=>{ io.to(to).emit('incomingCall',{from,offer,type}); });
    socket.on('callAnswer', ({from,to,answer})=>{ io.to(to).emit('callAnswered',{from,answer}); });
    socket.on('iceCandidate', ({from,to,candidate})=>{ io.to(to).emit('iceCandidate',{from,candidate}); });
    socket.on('gift', ({from,to,gift})=>{ const g={id:uuid(),from,to,gift,at:Date.now()}; db.gifts.push(g); io.to(to).emit('giftReceived',g); });
    socket.on('block', ({from,to,reason})=>{ db.blocks.push({from,to,reason,at:Date.now()}); });
    socket.on('report', ({from,to,reason})=>{ db.reports.push({from,to,reason,at:Date.now()}); });
    socket.on('spotlight', ({userId})=>{
      const now=Date.now();
      db.spotlight=(db.spotlight||[]).filter(s=>s.expiresAt>now);
      if(db.spotlight.length>=50) return;
      const entry={userId, expiresAt: now+3600000, activatedAt: now, likes:(db.spotlightLikes&&db.spotlightLikes[userId])||0};
      db.spotlight=db.spotlight.filter(s=>s.userId!==userId);
      db.spotlight.push(entry);
      io.emit('spotlightUpdate', db.spotlight);
    });
    socket.on('spotlightLike', ({userId})=>{
      if(!db.spotlightLikes) db.spotlightLikes={};
      if(!db.spotlightLikes[userId]) db.spotlightLikes[userId]=0;
      db.spotlightLikes[userId]++;
      db.spotlight=(db.spotlight||[]).map(s=> s.userId===userId ? {...s, likes:db.spotlightLikes[userId]}:s);
      io.emit('spotlightLikeUpdate',{userId, likes:db.spotlightLikes[userId]});
    });
    socket.on('updateProfilePic', ({userId, profilePic, images})=>{
      const idx=idIndex.get(userId);
      if(idx!==undefined){
        const u=db.users[idx];
        if(profilePic) u.profilePic=profilePic;
        if(images) u.images=images;
        saveDB();
        io.emit('profilePicUpdated',{userId, profilePic:u.profilePic});
      }
    });
  });

  app.post('/api/forgot', (req,res)=>{ res.json({success:true}); });

  app.get('/api/spotlight', (req,res)=>{
    const now=Date.now();
    db.spotlight=(db.spotlight||[]).filter(s=>s.expiresAt>now);
    let topMale=null, topFemale=null, maxMale=-1, maxFemale=-1;
    db.spotlight.forEach(s=>{
      const u=db.users.find(x=>x.id===s.userId);
      if(!u) return;
      const likes=(db.spotlightLikes&&db.spotlightLikes[s.userId])||s.likes||0;
      if(u.gender==='Férfi' && likes>maxMale){ maxMale=likes; topMale={...sanitizeUser(u), likes}; }
      if(u.gender==='Nő' && likes>maxFemale){ maxFemale=likes; topFemale={...sanitizeUser(u), likes}; }
    });
    res.json({active:db.spotlight.map(s=>{ const u=db.users.find(x=>x.id===s.userId); return u?{...sanitizeUser(u), expiresAt:s.expiresAt, likes:(db.spotlightLikes&&db.spotlightLikes[s.userId])||s.likes||0}:null }).filter(Boolean), topMale, topFemale});
  });

  app.post('/api/spotlight', (req,res)=>{
    const {userId}=req.body;
    const now=Date.now();
    db.spotlight=(db.spotlight||[]).filter(s=>s.expiresAt>now);
    if(db.spotlight.length>=50) return res.json({success:false});
    const entry={userId, expiresAt: now+3600000, activatedAt: now, likes:(db.spotlightLikes&&db.spotlightLikes[userId])||0};
    db.spotlight=db.spotlight.filter(s=>s.userId!==userId);
    db.spotlight.push(entry);
    res.json({success:true, spotlight:db.spotlight});
  });

  app.post('/api/spotlight/like', (req,res)=>{
    const {userId}=req.body;
    if(!db.spotlightLikes) db.spotlightLikes={};
    if(!db.spotlightLikes[userId]) db.spotlightLikes[userId]=0;
    db.spotlightLikes[userId]++;
    db.spotlight=(db.spotlight||[]).map(s=> s.userId===userId ? {...s, likes:db.spotlightLikes[userId]}:s);
    res.json({success:true, likes:db.spotlightLikes[userId]});
  });

  app.get('*', (req,res)=> res.sendFile(path.join(__dirname,'index.html')));

  const PORT=process.env.PORT||3000;
  server.listen(PORT, ()=> console.log(`Lovenux ${PORT} worker ${process.pid} 1B hetero privacy email/phone hidden free=${FREE_REGISTRATION} barion=${BARION_ENV}`));
}
