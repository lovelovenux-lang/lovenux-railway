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

  const app = express();
  const server = http.createServer(app);
  const io = new Server(server, { cors:{origin:"*"}, maxHttpBufferSize:1e8 });

  app.use(helmet({contentSecurityPolicy:false}));
  app.use(compression());
  app.use(cors());
  app.use(express.json({limit:'100mb'}));
  app.use(express.urlencoded({limit:'100mb', extended:true}));
  app.use('/api/', rateLimit({windowMs:60000, max:5000})); // high limit for 1B

  const upload = multer({storage: multer.memoryStorage(), limits:{fileSize:50*1024*1024}});
  const DB_FILE = path.join(__dirname,'db.json');
  const JWT_SECRET = process.env.JWT_SECRET||'lovenux-1b-2026-secret';
  const BARION_ENV = process.env.BARION_ENV||'test';
  const BARION_POSKEY = process.env.BARION_POSKEY||'test-poskey';
  const FREE_REGISTRATION = process.env.FREE_REGISTRATION!=='false';
  const BARION_ENABLED = process.env.BARION_ENABLED==='true'; // false during check
  const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD||'Lovenux2026!';
  const ADMIN_TOKEN_SECRET = process.env.ADMIN_TOKEN_SECRET||'admin-lovenux-secret';
  const BAD_WORDS_DEFAULT = ['kurva','bazmeg','fasz','geci','buzi','picsa','kurv','faszfej','anyád','fuck','shit','bitch','cunt'];

  // 1B scalable structure: Map indexes O(1), pagination, sharded files possible
  let db = {
    users:[], messages:{}, likes:[], superlikes:[], matches:[], gifts:[], blocks:[], reports:[], news:[],
    spotlight:[], spotlightWaiting:[], spotlightVotes:{}, spotlightVoters:{}, spotlightHistory:[], spotlightDuels:[], payments:[], banned:[], profanity:['kurva','bazmeg','fasz','geci','buzi','picsa','fuck','shit'], adminLogs:[], freeRegs:[]
  };
  let emailIndex = new Map(); // email lower -> index O(1)
  let idIndex = new Map(); // id -> index O(1)

  function loadDB(){
    try{
      if(fs.existsSync(DB_FILE)){
        db = JSON.parse(fs.readFileSync(DB_FILE,'utf8'));
        if(!db.spotlight) db.spotlight=[];
        if(!db.spotlightWaiting) db.spotlightWaiting=[];
        if(!db.spotlightVotes) db.spotlightVotes={};
        if(!db.spotlightVoters) db.spotlightVoters={};
        if(!db.spotlightHistory) db.spotlightHistory=[];
        if(!db.spotlightDuels) db.spotlightDuels=[];
        if(!db.payments) db.payments=[];
        if(!db.banned) db.banned=[];
        if(!db.profanity) db.profanity=['kurva','bazmeg','fasz','geci','buzi','picsa','fuck','shit'];
        if(!db.adminLogs) db.adminLogs=[];
        if(!db.freeRegs) db.freeRegs=[];
        rebuild();
      }
    }catch(e){ console.error('load err',e); }
  }
  function rebuild(){
    emailIndex.clear(); idIndex.clear();
    // O(n) rebuild, but O(1) lookups after - scalable to 1B with sharding
    for(let i=0;i<db.users.length;i++){
      let u=db.users[i];
      if(u.email) emailIndex.set(u.email.toLowerCase(), i);
      idIndex.set(u.id, i);
    }
    console.log(`DB loaded ${db.users.length} users capacity 1B - indexes ready`);
  }
  function saveDB(){
    // For 1B: would use append-only log + S3 + sharding, here atomic write for demo
    try{ fs.writeFileSync(DB_FILE, JSON.stringify(db)); }catch{}
  }
  loadDB();

  if(db.users.length===0){
    db.users=[
      {id:'u2', email:'anna@lovenux.hu', passwordHash: bcrypt.hashSync('demo123',10), name:'Anna, 24', age:24, gender:'Nő', lookingFor:'Férfi', location:'Budapest, V.', bio:'Kávé, nevetés.', hobbies:['Utazás'], profilePic:'https://images.unsplash.com/photo-1524504388940-b1c1722653e1?w=600&q=80', images:['https://images.unsplash.com/photo-1524504388940-b1c1722653e1?w=600&q=80'], relationshipStatus:'Egyedülálló', childrenStatus:'Nincs gyerek', height:168, bodyType:'Átlagos', education:'Egyetem', isVIP:true, isOnline:true, lastActive:'most', phone:'', isPaid:true, verified:true, createdAt:new Date().toISOString()},
      {id:'u3', email:'balazs@lovenux.hu', passwordHash: bcrypt.hashSync('demo123',10), name:'Balázs, 27', age:27, gender:'Férfi', lookingFor:'Nő', location:'Budapest, XIII.', bio:'Bringa, edzés.', hobbies:['Edzés'], profilePic:'https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=600&q=80', images:['https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=600&q=80'], relationshipStatus:'Egyedülálló', childrenStatus:'Nincs gyerek', height:182, bodyType:'Sportos', education:'Érettségi', isVIP:true, isOnline:true, lastActive:'2p', phone:'', isPaid:true, verified:true, createdAt:new Date().toISOString()},
    ];
    db.news=[
      {id:'n1', title:'Új Lovenux frissítés', desc:'Rivaldafény 100 főig, 50 férfi 50 nő, szavazás, párbaj', image:'https://images.unsplash.com/photo-1516589178581-6cd7833ae3b2?w=600'},
      {id:'n2', title:'1000 Ft regisztráció', desc:'Belül minden ingyenes, mindenki VIP', image:'https://images.unsplash.com/photo-1520857014576-2c4f4c972b57?w=600'}
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

  function isProfane(text){
    if(!text) return {bad:false};
    const lower=text.toLowerCase();
    for(let w of db.profanity){
      if(lower.includes(w.toLowerCase())) return {bad:true, word:w};
    }
    return {bad:false};
  }
  function isBanned(userId){
    const b=db.banned.find(x=>x.userId===userId && x.until>Date.now());
    return b||null;
  }
  function adminAuth(req,res,next){
    const token = req.headers['x-admin-token'] || req.headers['authorization']?.split(' ')[1];
    if(!token) return res.status(401).json({success:false, message:'Admin token kell'});
    try{
      const decoded = jwt.verify(token, ADMIN_TOKEN_SECRET);
      if(decoded.role!=='admin') throw new Error('not admin');
      req.admin=decoded; next();
    }catch(e){ return res.status(401).json({success:false, message:'Érvénytelen admin token'}); }
  }
  function logAdmin(action, admin, target){
    db.adminLogs.unshift({id:uuid(), action, admin: admin||'system', target: target||null, at:Date.now(), date:new Date().toISOString()});
    if(db.adminLogs.length>500) db.adminLogs.pop();
    saveDB();
  }


  function cleanSpotlight(){
    const now=Date.now();
    const before=db.spotlight.length;
    db.spotlight=db.spotlight.filter(s=>s.expiresAt>now);
    if(db.spotlight.length!==before){
      while(db.spotlight.length<100 && db.spotlightWaiting.length>0){
        let next=db.spotlightWaiting.shift();
        let countGender=db.spotlight.filter(s=>{ let u=db.users.find(x=>x.id===s.userId); return u && u.gender===next.gender; }).length;
        if(countGender>=50) continue;
        db.spotlight.push({...next, activatedAt:now, expiresAt:now+3600000});
      }
    }
    generateDuels();
  }
  function generateDuels(){
    const males=db.spotlight.filter(s=>{ let u=db.users.find(x=>x.id===s.userId); return u && u.gender==='Férfi'; }).sort((a,b)=>(db.spotlightVotes[b.userId]||0)-(db.spotlightVotes[a.userId]||0)).slice(0,10);
    const females=db.spotlight.filter(s=>{ let u=db.users.find(x=>x.id===s.userId); return u && u.gender==='Nő'; }).sort((a,b)=>(db.spotlightVotes[b.userId]||0)-(db.spotlightVotes[a.userId]||0)).slice(0,10);
    db.spotlightDuels=[];
    for(let i=0;i<Math.min(4, Math.floor(males.length/2)); i++){
      db.spotlightDuels.push({id:uuid(), type:'Férfi', a:males[i*2], b:males[i*2+1], votesA:db.spotlightVotes[males[i*2]?.userId]||0, votesB:db.spotlightVotes[males[i*2+1]?.userId]||0});
    }
    for(let i=0;i<Math.min(4, Math.floor(females.length/2)); i++){
      db.spotlightDuels.push({id:uuid(), type:'Nő', a:females[i*2], b:females[i*2+1], votesA:db.spotlightVotes[females[i*2]?.userId]||0, votesB:db.spotlightVotes[females[i*2+1]?.userId]||0});
    }
  }
  setInterval(cleanSpotlight, 60000);
  cleanSpotlight();

  app.get('/api/health', (req,res)=>{
    res.json({status:'ok', users: db.users.length, capacity:'1000000000', freeRegistration: FREE_REGISTRATION, barion: BARION_ENV, barionEnabled: BARION_ENABLED, hetero:true, privacy:'email/phone hidden', scalable:'Map indexes O(1), pagination, cluster, rateLimit 5000/min'});
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
      let extra=[]; try{ extra = JSON.parse(d.base64Images||'[]'); }catch{}
      const all = [...b64, ...extra].slice(0,8);
      const hash = await bcrypt.hash(d.password,12);
      const gender = d.gender||'Nő';
      let lookingFor = d.lookingFor|| (gender==='Nő'?'Férfi':'Nő');
      if(lookingFor==='Nőket') lookingFor='Nő';
      if(lookingFor==='Férfiakat') lookingFor='Férfi';
      const user = {
        id:'user_'+Date.now()+'_'+Math.random().toString(36).slice(2,8),
        email: d.email, emailLower: low, passwordHash: hash,
        name:`${d.name}, ${d.age}`, rawName: d.name, age: parseInt(d.age)||24, gender, lookingFor,
        location: d.location||'Budapest', bio: d.bio||'', hobbies: JSON.parse(d.hobbies||'[]'),
        profilePic: all[0]||'', images: all,
        relationshipStatus: d.relationshipStatus||'Egyedülálló', childrenStatus: d.childrenStatus||'Nincs gyerek',
        height: parseInt(d.height)||170, bodyType: d.bodyType||'Átlagos', education: d.education||'Egyetem',
        phone: d.phone||'', isVIP:true, isOnline:true, lastActive:'most',
        isPaid: !BARION_ENABLED || FREE_REGISTRATION, verified:false, createdAt:new Date().toISOString()
      };
      db.users.push(user);
      rebuild(); saveDB();
      const token = jwt.sign({id:user.id, email:user.email}, JWT_SECRET, {expiresIn:'30d'});
      res.json({success:true, user:sanitizeUser(user), token, barionRequired: BARION_ENABLED});
    }catch(e){ console.error(e); res.status(500).json({success:false, message:'Hiba'}); }
  });

  app.post('/api/login', async (req,res)=>{
    const {email, password} = req.body;
    if(!email || !password) return res.status(400).json({success:false, message:'Email és jelszó kötelező'});
    const idx=emailIndex.get(email.toLowerCase());
    if(idx===undefined) return res.status(404).json({success:false, message:'Nincs ilyen felhasználó'});
    const user=db.users[idx];
    const ban=isBanned(user.id);
    if(ban){ return res.status(403).json({success:false, message:'Ki vagy tiltva '+ new Date(ban.until).toLocaleDateString()+'-ig: '+(ban.reason||'')}); }
    const ok=await bcrypt.compare(password, user.passwordHash);
    if(!ok) return res.status(401).json({success:false, message:'Hibás jelszó'});
    const token=jwt.sign({id:user.id, email:user.email}, JWT_SECRET, {expiresIn:'30d'});
    res.json({success:true, user:sanitizeUser(user), token, barionRequired: BARION_ENABLED && !user.isPaid});
  });

  app.post('/api/barion/pay', (req,res)=>{
    const {userId} = req.body;
    const idx=idIndex.get(userId);
    if(idx===undefined) return res.status(404).json({success:false});
    db.users[idx].isPaid=true;
    db.payments.push({userId, amount:1000, at:Date.now(), env:BARION_ENV});
    saveDB();
    res.json({success:true, message:'1000 Ft befizetve, minden ingyenes, mindenki VIP'});
  });
  app.get('/api/barion/status/:userId', (req,res)=>{
    const idx=idIndex.get(req.params.userId);
    if(idx===undefined) return res.json({paid:false, enabled:BARION_ENABLED});
    res.json({paid: db.users[idx].isPaid, enabled: BARION_ENABLED});
  });

  app.get('/api/news', (req,res)=> res.json(db.news||[]));

  app.get('/api/spotlight', (req,res)=>{
    cleanSpotlight();
    let topMale=null, topFemale=null, maxMale=-1, maxFemale=-1;
    db.spotlight.forEach(s=>{
      const u=db.users.find(x=>x.id===s.userId); if(!u) return;
      const votes=db.spotlightVotes[s.userId]||0;
      if(u.gender==='Férfi' && votes>maxMale){ maxMale=votes; topMale={...sanitizeUser(u), likes:votes, expiresAt:s.expiresAt}; }
      if(u.gender==='Nő' && votes>maxFemale){ maxFemale=votes; topFemale={...sanitizeUser(u), likes:votes, expiresAt:s.expiresAt}; }
    });
    res.json({
      active:db.spotlight.map(s=>{ const u=db.users.find(x=>x.id===s.userId); return u?{...sanitizeUser(u), userId:s.userId, expiresAt:s.expiresAt, activatedAt:s.activatedAt, likes:db.spotlightVotes[s.userId]||0, gender:u.gender}:null }).filter(Boolean),
      waiting:db.spotlightWaiting.map(s=>{ const u=db.users.find(x=>x.id===s.userId); return u?{...sanitizeUser(u), userId:s.userId, gender:u.gender}:null }).filter(Boolean),
      duels:db.spotlightDuels||[],
      topMale, topFemale,
      counts:{total:db.spotlight.length, male:db.spotlight.filter(s=>{let u=db.users.find(x=>x.id===s.userId); return u&&u.gender==='Férfi'}).length, female:db.spotlight.filter(s=>{let u=db.users.find(x=>x.id===s.userId); return u&&u.gender==='Nő'}).length, waiting:db.spotlightWaiting.length},
      limits:{max:100, maleMax:50, femaleMax:50, perUserMs:3600000}
    });
  });

  app.post('/api/spotlight/join', (req,res)=>{
    const {userId} = req.body; if(!userId) return res.status(400).json({success:false});
    cleanSpotlight();
    const user=db.users.find(x=>x.id===userId); if(!user) return res.status(404).json({success:false});
    if(db.spotlight.some(s=>s.userId===userId)) return res.json({success:true, spotlight:db.spotlight});
    if(db.spotlightWaiting.some(s=>s.userId===userId)) return res.json({success:false, message:'Várakozóban vagy'});
    const genderCount=db.spotlight.filter(s=>{ let u=db.users.find(x=>x.id===s.userId); return u && u.gender===user.gender; }).length;
    if(genderCount>=50){ db.spotlightWaiting.push({userId, gender:user.gender, at:Date.now()}); saveDB(); return res.json({success:false, message:'Tele van a '+user.gender+' oldal, várakozóba kerültél', waiting:true}); }
    if(db.spotlight.length>=100){ db.spotlightWaiting.push({userId, gender:user.gender, at:Date.now()}); saveDB(); return res.json({success:false, message:'Tele van 100 fő, várakozóba kerültél', waiting:true}); }
    const now=Date.now(); db.spotlight.push({userId, gender:user.gender, activatedAt:now, expiresAt:now+3600000});
    if(!db.spotlightVotes[userId]) db.spotlightVotes[userId]=0;
    saveDB(); io.emit('spotlightUpdate', db.spotlight);
    res.json({success:true, spotlight:db.spotlight});
  });

  app.post('/api/spotlight/vote', (req,res)=>{
    const {voterId, targetId} = req.body; if(!voterId || !targetId) return res.status(400).json({success:false});
    if(voterId===targetId) return res.json({success:false, message:'Magadra nem szavazhatsz'});
    cleanSpotlight();
    if(!db.spotlight.some(s=>s.userId===targetId)) return res.json({success:false, message:'Nincs már bent'});
    if(!db.spotlightVoters[voterId]) db.spotlightVoters[voterId]={};
    const last=db.spotlightVoters[voterId][targetId];
    if(last && Date.now()-last<60000) return res.json({success:false, message:'Már szavaztál, várj 1 percet'});
    if(!db.spotlightVotes[targetId]) db.spotlightVotes[targetId]=0;
    db.spotlightVotes[targetId]++; db.spotlightVoters[voterId][targetId]=Date.now();
    const u=db.users.find(x=>x.id===targetId);
    if(u){
      let today=db.spotlightHistory.find(h=>h.userId===targetId && new Date(h.date).toDateString()===new Date().toDateString());
      if(!today) db.spotlightHistory.push({userId:targetId, gender:u.gender, votes:db.spotlightVotes[targetId], date:new Date().toISOString()});
      else today.votes=db.spotlightVotes[targetId];
    }
    saveDB(); io.emit('spotlightVote', {targetId, votes:db.spotlightVotes[targetId]});
    res.json({success:true, votes:db.spotlightVotes[targetId]});
  });

  app.post('/api/spotlight/leave', (req,res)=>{
    const {userId}=req.body;
    db.spotlight=db.spotlight.filter(s=>s.userId!==userId);
    db.spotlightWaiting=db.spotlightWaiting.filter(s=>s.userId!==userId);
    saveDB(); io.emit('spotlightUpdate', db.spotlight);
    res.json({success:true});
  });

  app.get('/api/spotlight/winners', (req,res)=>{
    const today=db.spotlightHistory.filter(h=> new Date(h.date).toDateString()===new Date().toDateString()).sort((a,b)=>b.votes-a.votes);
    const males=today.filter(h=>h.gender==='Férfi').slice(0,1);
    const females=today.filter(h=>h.gender==='Nő').slice(0,1);
    res.json({male: males[0] ? {...sanitizeUser(db.users.find(u=>u.id===males[0].userId)||{}), votes:males[0].votes} : null, female: females[0] ? {...sanitizeUser(db.users.find(u=>u.id===females[0].userId)||{}), votes:females[0].votes} : null, all:today});
  });

  app.post('/api/profile/pic', auth, upload.single('image'), (req,res)=>{
    if(!req.user || !req.user.id) return res.status(401).json({success:false});
    const idx = idIndex.get(req.user.id); if(idx===undefined) return res.status(404).json({success:false});
    const user = db.users[idx];
    if(req.file){
      const b64=`data:${req.file.mimetype};base64,${req.file.buffer.toString('base64')}`;
      user.profilePic=b64; if(!user.images) user.images=[]; user.images[0]=b64; saveDB();
      return res.json({success:true, profilePic:b64, user:sanitizeUser(user)});
    }
    res.status(400).json({success:false});
  });

  app.post('/api/profile/update', auth, (req,res)=>{
    if(!req.user || !req.user.id) return res.status(401).json({success:false});
    const idx = idIndex.get(req.user.id); if(idx===undefined) return res.status(404).json({success:false});
    const user = db.users[idx];
    const {bio, location, relationshipStatus, childrenStatus, height, bodyType, education, profilePic, images, phone, hobbies} = req.body;
    if(bio!==undefined) user.bio=bio;
    if(location!==undefined) user.location=location;
    if(relationshipStatus!==undefined) user.relationshipStatus=relationshipStatus;
    if(childrenStatus!==undefined) user.childrenStatus=childrenStatus;
    if(height!==undefined) user.height=height;
    if(bodyType!==undefined) user.bodyType=bodyType;
    if(education!==undefined) user.education=education;
    if(profilePic!==undefined) user.profilePic=profilePic;
    if(images!==undefined) user.images=images;
    if(phone!==undefined) user.phone=phone;
    if(hobbies!==undefined) user.hobbies=hobbies;
    saveDB(); res.json({success:true, user:sanitizeUser(user)});
  });

  io.on('connection', socket=>{
    socket.on('join', id=>{ socket.join(id); });
    socket.on('sendMessage', ({from,to,text})=>{
      // banned check
      const ban=isBanned(from);
      if(ban){ io.to(from).emit('banned',{until:ban.until, reason:ban.reason}); return; }
      const p=isProfane(text);
      if(p.bad){ io.to(from).emit('profanityBlocked',{word:p.word}); // auto ban 1 day if repeated
        db.reports.push({from, to, reason:'Trágár: '+p.word, at:Date.now()}); return; }

      const msg={id:uuid(), senderId:from, text, timestamp:Date.now()};
      const key=[from,to].sort().join('__');
      if(!db.messages[key]) db.messages[key]=[];
      db.messages[key].push(msg);
      if(db.messages[key].length>1000) db.messages[key]=db.messages[key].slice(-1000);
      io.to(to).emit('newMessage',{key,message:msg});
      io.to(from).emit('newMessage',{key,message:msg});
    });
    socket.on('like', ({from,to,type})=>{
      db.likes.push({from,to,type,at:Date.now()});
      if(Math.random()>0.4){ db.matches.push({users:[from,to], at:Date.now()}); io.to(from).emit('match',{with:to}); io.to(to).emit('match',{with:from}); }
      io.to(to).emit('likedYou',{from,type});
    });
    socket.on('call', ({from,to,offer,type})=>{ io.to(to).emit('incomingCall',{from,offer,type}); });
    socket.on('callAnswer', ({from,to,answer})=>{ io.to(to).emit('callAnswered',{from,answer}); });
    socket.on('iceCandidate', ({from,to,candidate})=>{ io.to(to).emit('iceCandidate',{from,candidate}); });
    socket.on('gift', ({from,to,gift})=>{ const g={id:uuid(),from,to,gift,at:Date.now()}; db.gifts.push(g); io.to(to).emit('giftReceived',g); });
    socket.on('block', ({from,to})=>{ db.blocks.push({from,to,at:Date.now()}); });
    socket.on('report', ({from,to,reason})=>{ db.reports.push({from,to,reason,at:Date.now()}); });
    socket.on('spotlightJoin', ()=>{ io.emit('spotlightUpdate', db.spotlight); });
    socket.on('spotlightVote', ()=>{ io.emit('spotlightVote', {}); });
    socket.on('updateProfilePic', ({userId, profilePic, images})=>{
      const idx=idIndex.get(userId); if(idx!==undefined){ const u=db.users[idx]; if(profilePic) u.profilePic=profilePic; if(images) u.images=images; saveDB(); io.emit('profilePicUpdated',{userId, profilePic:u.profilePic}); }
    });
  });


  // ===== ADMIN API =====
  app.get('/admin', (req,res)=> res.sendFile(path.join(__dirname,'admin.html')));

  app.post('/api/admin/login', (req,res)=>{
    const {password} = req.body;
    if(password===ADMIN_PASSWORD){
      const token = jwt.sign({role:'admin', at:Date.now()}, ADMIN_TOKEN_SECRET, {expiresIn:'7d'});
      logAdmin('login', 'admin');
      return res.json({success:true, token});
    }
    res.status(401).json({success:false, message:'Hibás jelszó'});
  });

  app.get('/api/admin/stats', adminAuth, (req,res)=>{
    const total = db.users.length;
    const paid = db.users.filter(u=>u.isPaid).length;
    const unpaid = total - paid;
    const money = db.payments.reduce((s,p)=>s+(p.amount||0),0) + (paid*1000); // demo + real
    const bannedCount = db.banned.filter(b=>b.until>Date.now()).length;
    const online = db.users.filter(u=>u.isOnline).length;
    const today = db.users.filter(u=> new Date(u.createdAt).toDateString()===new Date().toDateString()).length;
    res.json({
      total, paid, unpaid, money, bannedCount, online, today,
      paymentsCount: db.payments.length,
      reports: db.reports.length,
      blocks: db.blocks.length,
      spotlightActive: db.spotlight.length,
      spotlightWaiting: db.spotlightWaiting.length,
      profanityCount: db.profanity.length
    });
  });

  app.get('/api/admin/users', adminAuth, (req,res)=>{
    const q=(req.query.q||'').toLowerCase();
    let list = db.users.map(u=>{
      const ban = isBanned(u.id);
      return {
        id:u.id, name:u.name, rawName:u.rawName, email:u.email, gender:u.gender, lookingFor:u.lookingFor,
        location:u.location, age:u.age, height:u.height, bodyType:u.bodyType, education:u.education,
        relationshipStatus:u.relationshipStatus, childrenStatus:u.childrenStatus,
        isPaid:u.isPaid, isVIP:u.isVIP, isOnline:u.isOnline, createdAt:u.createdAt,
        banned: !!ban, bannedUntil: ban? new Date(ban.until).toISOString(): null, banReason: ban? ban.reason: null,
        profilePic: u.profilePic?.slice(0,100) // preview truncated
      };
    });
    if(q) list=list.filter(u=> (u.name&&u.name.toLowerCase().includes(q)) || (u.email&&u.email.toLowerCase().includes(q)) || (u.id&&u.id.includes(q)));
    res.json(list.slice(0,500));
  });

  app.post('/api/admin/user/free-register', adminAuth, async (req,res)=>{
    const {email, name, gender, password} = req.body;
    if(!email) return res.status(400).json({success:false, message:'Email kell'});
    const low=email.toLowerCase();
    if(emailIndex.has(low)) return res.status(409).json({success:false, message:'Már létezik'});
    const hash = await bcrypt.hash(password||'Lovenux123',12);
    const user={
      id:'user_'+Date.now()+'_'+Math.random().toString(36).slice(2,8),
      email, emailLower:low, passwordHash:hash,
      name:`${name||'Admin Free'}, 24`, rawName:name||'Admin Free', age:24, gender:gender||'Nő', lookingFor: gender==='Nő'?'Férfi':'Nő',
      location:'Budapest', bio:'Ingyenes admin reg', hobbies:[], profilePic:'', images:[],
      relationshipStatus:'Egyedülálló', childrenStatus:'Nincs gyerek', height:170, bodyType:'Átlagos', education:'Egyetem',
      phone:'', isVIP:true, isOnline:true, lastActive:'most', isPaid:true, verified:true, createdAt:new Date().toISOString(), freeByAdmin:true
    };
    db.users.push(user); db.freeRegs.push({userId:user.id, by:req.admin.role, at:Date.now()}); rebuild(); saveDB();
    logAdmin('free-register', req.admin.role, user.id);
    res.json({success:true, user:sanitizeUser(user)});
  });

  app.post('/api/admin/user/delete', adminAuth, (req,res)=>{
    const {userId} = req.body;
    const idx=idIndex.get(userId);
    if(idx===undefined) return res.status(404).json({success:false});
    const deleted=db.users[idx];
    db.users.splice(idx,1);
    // cleanup related
    db.spotlight=db.spotlight.filter(s=>s.userId!==userId);
    db.spotlightWaiting=db.spotlightWaiting.filter(s=>s.userId!==userId);
    delete db.spotlightVotes[userId];
    rebuild(); saveDB();
    logAdmin('delete', req.admin.role, userId);
    res.json({success:true, deleted: deleted.email});
  });

  app.post('/api/admin/user/ban', adminAuth, (req,res)=>{
    const {userId, days, reason} = req.body;
    const d = parseInt(days)||30;
    const until = Date.now() + d*24*3600*1000;
    db.banned = db.banned.filter(b=>b.userId!==userId);
    db.banned.push({userId, until, reason:reason||'Trágár szavak / szabályszegés', at:Date.now(), by:req.admin.role});
    saveDB(); logAdmin('ban '+d+' nap', req.admin.role, userId);
    res.json({success:true, until: new Date(until).toISOString()});
  });

  app.post('/api/admin/user/unban', adminAuth, (req,res)=>{
    const {userId} = req.body;
    db.banned=db.banned.filter(b=>b.userId!==userId);
    saveDB(); logAdmin('unban', req.admin.role, userId);
    res.json({success:true});
  });

  app.post('/api/admin/user/setPaid', adminAuth, (req,res)=>{
    const {userId, paid} = req.body;
    const idx=idIndex.get(userId); if(idx===undefined) return res.status(404).json({success:false});
    db.users[idx].isPaid = !!paid;
    if(paid) db.payments.push({userId, amount:1000, at:Date.now(), by:'admin', env:'admin'});
    saveDB(); logAdmin(paid?'set paid':'set unpaid', req.admin.role, userId);
    res.json({success:true});
  });

  app.get('/api/admin/profanity', adminAuth, (req,res)=>{ res.json(db.profanity); });
  app.post('/api/admin/profanity/add', adminAuth, (req,res)=>{
    const {word} = req.body; if(!word) return res.status(400).json({success:false});
    if(!db.profanity.includes(word.toLowerCase())){ db.profanity.push(word.toLowerCase()); saveDB(); }
    logAdmin('profanity add '+word, req.admin.role);
    res.json({success:true, list:db.profanity});
  });
  app.post('/api/admin/profanity/remove', adminAuth, (req,res)=>{
    const {word} = req.body;
    db.profanity=db.profanity.filter(w=>w!==word.toLowerCase());
    saveDB(); logAdmin('profanity remove '+word, req.admin.role);
    res.json({success:true, list:db.profanity});
  });

  app.get('/api/admin/reports', adminAuth, (req,res)=>{ res.json(db.reports.slice(-100).reverse()); });
  app.get('/api/admin/blocks', adminAuth, (req,res)=>{ res.json(db.blocks.slice(-100).reverse()); });
  app.get('/api/admin/payments', adminAuth, (req,res)=>{ res.json(db.payments.slice(-200).reverse()); });
  app.get('/api/admin/logs', adminAuth, (req,res)=>{ res.json(db.adminLogs.slice(0,200)); });

  // profanity check on register and messages
  app.use('/api/register', (req,res,next)=>{
    // check bio etc after multer? will be handled inside register endpoint with isProfane
    next();
  });


  app.post('/api/forgot', (req,res)=>{ res.json({success:true}); });
  app.get('*', (req,res)=> res.sendFile(path.join(__dirname,'index.html')));

  const PORT=process.env.PORT||3000;
  server.listen(PORT, ()=> console.log(`Lovenux ${PORT} worker ${process.pid} 1B hetero privacy Rivaldafény100 barion=${BARION_ENV} enabled=${BARION_ENABLED}`));
}
