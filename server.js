const express=require('express');
const cors=require('cors');
const bcrypt=require('bcryptjs');
const jwt=require('jsonwebtoken');
const multer=require('multer');
const fs=require('fs');
const path=require('path');

const app=express();
const PORT=process.env.PORT||3000;
const JWT_SECRET=process.env.JWT_SECRET||'lovenux-secret-2026';
const DATA_DIR=path.join(__dirname,'data');
const UPLOAD_DIR=path.join(__dirname,'uploads');

if(!fs.existsSync(DATA_DIR))fs.mkdirSync(DATA_DIR,{recursive:true});
if(!fs.existsSync(UPLOAD_DIR))fs.mkdirSync(UPLOAD_DIR,{recursive:true});

const storage=multer.diskStorage({
 destination:(req,file,cb)=>cb(null,UPLOAD_DIR),
 filename:(req,file,cb)=>{const ext=path.extname(file.originalname)||'.jpg';cb(null,Date.now()+'-'+Math.random().toString(36).slice(2)+ext)}
});
const upload=multer({storage,limits:{fileSize:5*1024*1024}});

app.use(cors());
app.use(express.json({limit:'10mb'}));
app.use('/uploads',express.static(UPLOAD_DIR));
app.use(express.static(__dirname));

let users=[];let likes=[];let matches=[];let messages=[];let payments={};

const USERS_FILE=path.join(DATA_DIR,'users.json');
const LIKES_FILE=path.join(DATA_DIR,'likes.json');
const MATCHES_FILE=path.join(DATA_DIR,'matches.json');
const MESSAGES_FILE=path.join(DATA_DIR,'messages.json');

function loadData(){
 try{if(fs.existsSync(USERS_FILE))users=JSON.parse(fs.readFileSync(USERS_FILE,'utf8'))}catch(e){users=[]}
 try{if(fs.existsSync(LIKES_FILE))likes=JSON.parse(fs.readFileSync(LIKES_FILE,'utf8'))}catch(e){likes=[]}
 try{if(fs.existsSync(MATCHES_FILE))matches=JSON.parse(fs.readFileSync(MATCHES_FILE,'utf8'))}catch(e){matches=[]}
 try{if(fs.existsSync(MESSAGES_FILE))messages=JSON.parse(fs.readFileSync(MESSAGES_FILE,'utf8'))}catch(e){messages=[]}
}
function saveUsers(){fs.writeFileSync(USERS_FILE,JSON.stringify(users,null,2))}
function saveLikes(){fs.writeFileSync(LIKES_FILE,JSON.stringify(likes,null,2))}
function saveMatches(){fs.writeFileSync(MATCHES_FILE,JSON.stringify(matches,null,2))}
function saveMessages(){fs.writeFileSync(MESSAGES_FILE,JSON.stringify(messages,null,2))}
loadData();

function genToken(email){return jwt.sign({email},JWT_SECRET,{expiresIn:'30d'})}
function auth(req,res,next){
 const h=req.headers.authorization;
 if(!h||!h.startsWith('Bearer '))return res.status(401).json({error:'Nincs token'});
 const token=h.split(' ')[1];
 try{const d=jwt.verify(token,JWT_SECRET);req.userEmail=d.email;next()}catch(e){return res.status(401).json({error:'Érvénytelen token'})}
}
function calcAge(birth){if(!birth)return 0;const b=new Date(birth);const now=new Date();let age=now.getFullYear()-b.getFullYear();const m=now.getMonth()-b.getMonth();if(m<0||(m===0&&now.getDate()<b.getDate()))age--;return age}
function publicUser(u){
 if(!u)return null;
 let photos=u.photos||[];if(typeof photos==='string')try{photos=JSON.parse(photos)}catch(e){}
 return {email:u.email,name:u.name,age:calcAge(u.birth),city:u.city,gender:u.gender,looking_for:u.looking_for,bio:u.bio,height:u.height,body_type:u.body_type,eye_color:u.eye_color,hair_color:u.hair_color,smoking:u.smoking,drinking:u.drinking,education:u.education,job:u.job,music:u.music,hobbies:u.hobbies||[],photos}
}

app.post('/api/register',upload.array('photos',6),async(req,res)=>{
 try{
  const {email,password,city,birth,bio,name,gender,looking_for,height,body_type,eye_color,hair_color,smoking,drinking,education,job,music,hobbies}=req.body;
  if(!email||!password||!city||!birth||!name)return res.status(400).json({error:'Minden *-os mező kell'});
  const em=email.toLowerCase().trim();
  if(users.find(u=>u.email===em))return res.status(400).json({error:'E-mail már regisztrálva'});
  if(password.length<8)return res.status(400).json({error:'Jelszó min 8'});
  const age=calcAge(birth);if(age<18)return res.status(400).json({error:'18+ kell'});
  const hash=await bcrypt.hash(password,10);
  let photoUrls=[];
  if(req.files){photoUrls=req.files.map(f=>`/uploads/${f.filename}`)}
  let hb=[];try{hb=JSON.parse(hobbies||'[]')}catch(e){hb=[]}
  const user={email:em,password:hash,name,city,birth,bio:bio||'',gender:gender||'ferfi',looking_for:looking_for||'noket',height:height||'',body_type:body_type||'',eye_color:eye_color||'',hair_color:hair_color||'',smoking:smoking||'',drinking:drinking||'',education:education||'',job:job||'',music:music||'',hobbies:hb,photos:photoUrls,paid:true,createdAt:new Date().toISOString()};
  users.push(user);saveUsers();
  const token=genToken(em);
  return res.json({token,user:publicUser(user)});
 }catch(e){console.error(e);return res.status(500).json({error:'Szerver hiba'})}
});

app.post('/api/barion/start',async(req,res)=>{
 const {email}=req.body;if(!email)return res.status(400).json({error:'E-mail kell'});
 const em=email.toLowerCase().trim();
 const u=users.find(x=>x.email===em);
 if(u&&u.paid)return res.json({alreadyPaid:true,paymentId:'already-paid'});
 const pid='pay_'+Date.now()+'_'+Math.random().toString(36).slice(2);
 payments[pid]={email:em,createdAt:new Date().toISOString(),status:'pending'};
 res.json({paymentId:pid,url:'#test-pay'});
});

app.post('/api/barion/confirm',async(req,res)=>{
 const {paymentId,email}=req.body;
 const em=(email||'').toLowerCase().trim();
 const pay=payments[paymentId];
 if(!pay&&paymentId!=='already-paid')return res.status(400).json({error:'Érvénytelen fizetés'});
 const u=users.find(x=>x.email===em);
 if(!u)return res.status(404).json({error:'Felhasználó nem található'});
 u.paid=true;saveUsers();
 if(pay)pay.status='paid';
 const token=genToken(em);
 res.json({token});
});

app.post('/api/login',async(req,res)=>{
 const {email,password}=req.body;
 if(!email||!password)return res.status(400).json({error:'E-mail és jelszó kell'});
 const em=email.toLowerCase().trim();
 const u=users.find(x=>x.email===em);
 if(!u)return res.status(400).json({error:'Nincs ilyen felhasználó'});
 const ok=await bcrypt.compare(password,u.password);
 if(!ok)return res.status(400).json({error:'Hibás jelszó'});
 if(!u.paid)return res.status(402).json({error:'Még nem fizettél',needPayment:true,email:em});
 const token=genToken(em);
 res.json({token,user:publicUser(u)});
});

app.get('/api/me',auth,async(req,res)=>{
 const u=users.find(x=>x.email===req.userEmail);
 if(!u)return res.status(404).json({error:'Nem található'});
 res.json({...publicUser(u),birth:u.birth});
});

app.get('/api/discover',auth,async(req,res)=>{
 const me=users.find(x=>x.email===req.userEmail);
 if(!me)return res.status(401).json({error:'Auth'});
 const minAge=parseInt(req.query.minAge||'18');const maxAge=parseInt(req.query.maxAge||'99');
 const cityQ=(req.query.city||'').toLowerCase().trim();
 const genderQ=(req.query.gender||'auto');
 const bodyQ=req.query.body_type||'';const eduQ=req.query.education||'';const smokingQ=req.query.smoking||'';const drinkingQ=req.query.drinking||'';
 let hobbyQ=[];try{hobbyQ=JSON.parse(req.query.hobbies||'[]')}catch(e){hobbyQ=[]}
 let targetGender=null;
 if(genderQ==='ferfi')targetGender='ferfi';
 else if(genderQ==='no')targetGender='no';
 else{
  if(me.gender==='ferfi')targetGender='no';
  else targetGender='ferfi';
 }
 let list=users.filter(u=>{
  if(u.email===me.email)return false;
  if(targetGender&&u.gender!==targetGender)return false;
  const age=calcAge(u.birth);if(age<minAge||age>maxAge)return false;
  if(cityQ&&!u.city.toLowerCase().includes(cityQ))return false;
  if(bodyQ&&u.body_type!==bodyQ)return false;
  if(eduQ&&u.education!==eduQ)return false;
  if(smokingQ&&u.smoking!==smokingQ)return false;
  if(drinkingQ&&u.drinking!==drinkingQ)return false;
  if(hobbyQ.length>0){
   const uh=u.hobbies||[];
   const has=uh.some(h=>hobbyQ.includes(h));
   if(!has)return false;
  }
  return true;
 });
 list=list.map(publicUser);
 res.json({users:list});
});

app.post('/api/like',auth,async(req,res)=>{
 const {toEmail}=req.body;if(!toEmail)return res.status(400).json({error:'toEmail kell'});
 const from=req.userEmail;const to=toEmail.toLowerCase().trim();
 if(from===to)return res.status(400).json({error:'Magadat nem kedvelheted'});
 if(!users.find(u=>u.email===to))return res.status(404).json({error:'Felhasználó nem található'});
 if(likes.find(l=>l.from===from&&l.to===to))return res.json({liked:true,match:matches.some(m=>(m.a===from&&m.b===to)||(m.a===to&&m.b===from))});
 likes.push({from,to,at:new Date().toISOString()});saveLikes();
 const mutual=likes.find(l=>l.from===to&&l.to===from);
 if(mutual){
  if(!matches.some(m=>(m.a===from&&m.b===to)||(m.a===to&&m.b===from))){
   matches.push({a:from,b:to,at:new Date().toISOString()});saveMatches();
  }
  return res.json({liked:true,match:true});
 }
 res.json({liked:true,match:false});
});

app.get('/api/matches',auth,async(req,res)=>{
 const me=req.userEmail;
 const myMatches=matches.filter(m=>m.a===me||m.b===me);
 const result=myMatches.map(m=>{
  const other=m.a===me?m.b:m.a;
  const u=users.find(x=>x.email===other);
  return u?{...publicUser(u),matchedAt:m.at}:null;
 }).filter(Boolean);
 res.json(result);
});

app.get('/api/messages',auth,async(req,res)=>{
 const me=req.userEmail;
 const list=messages.filter(m=>m.from===me||m.to===me).sort((a,b)=>new Date(a.at)-new Date(b.at));
 res.json(list);
});

app.get('/api/messages/:email',auth,async(req,res)=>{
 const me=req.userEmail;const other=req.params.email.toLowerCase().trim();
 const list=messages.filter(m=>(m.from===me&&m.to===other)||(m.from===other&&m.to===me)).sort((a,b)=>new Date(a.at)-new Date(b.at));
 res.json(list);
});

app.post('/api/messages',auth,async(req,res)=>{
 const {to,text}=req.body;if(!to||!text)return res.status(400).json({error:'to és text kell'});
 const from=req.userEmail;const toEmail=to.toLowerCase().trim();
 const isMatch=matches.some(m=>(m.a===from&&m.b===toEmail)||(m.a===toEmail&&m.b===from));
 if(!isMatch)return res.status(403).json({error:'Csak MATCH után írhatsz'});
 const msg={from,to:toEmail,text:text.slice(0,1000),at:new Date().toISOString()};
 messages.push(msg);saveMessages();
 res.json(msg);
});

app.get('/api/likes',auth,async(req,res)=>{
 const me=req.userEmail;
 const myLikes=likes.filter(l=>l.to===me);
 const result=myLikes.map(l=>{
  const u=users.find(x=>x.email===l.from);
  if(!u)return null;
  return {...publicUser(u),likedAt:l.at};
 }).filter(Boolean);
 res.json(result);
});

app.get('/api/likes/count',auth,async(req,res)=>{
 const me=req.userEmail;
 const count=likes.filter(l=>l.to===me).length;
 res.json({count});
});

app.delete('/api/like/:email',auth,async(req,res)=>{
 const me=req.userEmail;const other=req.params.email.toLowerCase().trim();
 const idx=likes.findIndex(l=>l.from===other&&l.to===me);
 if(idx!==-1){likes.splice(idx,1);saveLikes();}
 const mIdx=matches.findIndex(m=>(m.a===me&&m.b===other)||(m.a===other&&m.b===me));
 if(mIdx!==-1){matches.splice(mIdx,1);saveMatches();}
 res.json({ok:true});
});

app.delete('/api/account',auth,async(req,res)=>{
 const me=req.userEmail;
 users=users.filter(u=>u.email!==me);
 likes=likes.filter(l=>l.from!==me&&l.to!==me);
 matches=matches.filter(m=>m.a!==me&&m.b!==me);
 messages=messages.filter(m=>m.from!==me&&m.to!==me);
 saveUsers();saveLikes();saveMatches();saveMessages();
 res.json({ok:true});
});

app.post('/api/change-password',auth,async(req,res)=>{
 const {oldPassword,newPassword}=req.body;
 if(!oldPassword||!newPassword)return res.status(400).json({error:'Minden mező kell'});
 if(newPassword.length<8)return res.status(400).json({error:'Új jelszó min 8'});
 const u=users.find(x=>x.email===req.userEmail);
 if(!u)return res.status(404).json({error:'Felhasználó nem található'});
 const ok=await bcrypt.compare(oldPassword,u.password);
 if(!ok)return res.status(400).json({error:'Régi jelszó hibás'});
 u.password=await bcrypt.hash(newPassword,10);saveUsers();
 res.json({ok:true});
});

app.put('/api/me',auth,async(req,res)=>{
 const u=users.find(x=>x.email===req.userEmail);
 if(!u)return res.status(404).json({error:'Nem található'});
 const {name,city,bio,height,body_type,eye_color,hair_color,smoking,drinking,education,job,music,hobbies}=req.body;
 if(name!==undefined)u.name=name;
 if(city!==undefined)u.city=city;
 if(bio!==undefined)u.bio=bio;
 if(height!==undefined)u.height=height;
 if(body_type!==undefined)u.body_type=body_type;
 if(eye_color!==undefined)u.eye_color=eye_color;
 if(hair_color!==undefined)u.hair_color=hair_color;
 if(smoking!==undefined)u.smoking=smoking;
 if(drinking!==undefined)u.drinking=drinking;
 if(education!==undefined)u.education=education;
 if(job!==undefined)u.job=job;
 if(music!==undefined)u.music=music;
 if(hobbies!==undefined){try{u.hobbies=Array.isArray(hobbies)?hobbies:JSON.parse(hobbies)}catch(e){}}
 saveUsers();
 res.json(publicUser(u));
});

app.get('/api/messages/count',auth,async(req,res)=>{
 const me=req.userEmail;
 const distinct=new Set(messages.filter(m=>m.to===me).map(m=>m.from));
 res.json({count:distinct.size});
});

app.post('/api/forgot-password',async(req,res)=>{
 const {email}=req.body;if(!email)return res.status(400).json({error:'E-mail kell'});
 const em=email.toLowerCase().trim();
 const u=users.find(x=>x.email===em);
 if(!u)return res.json({ok:true});
 console.log(`[FORGOT] Password reset for ${em} - would send email`);
 res.json({ok:true});
});

app.get('/api/health',(req,res)=>res.json({ok:true,users:users.length}));

app.listen(PORT,()=>console.log(`Lovenux server running on ${PORT}`));
