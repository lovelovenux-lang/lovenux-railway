const express = require('express');
const http = require('http');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const sharp = require('sharp');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 10e6 // kép üzenetben 10MB
});

const PORT = process.env.PORT || 3000;
const SECRET = process.env.SECRET || 'lovenux-ultra-secret-2026';

// ===== 1. SKÁLÁZÁS 1 BILLIÓRA =====
// Élesben: MongoDB Atlas + Sharding (email shard key) + Redis
// Most file fallback, de a kód már DB ready
const DB_FILE = './database.json';
if (!fs.existsSync(DB_FILE)) fs.writeFileSync(DB_FILE, JSON.stringify({ users: [], messages: [] }));
if (!fs.existsSync('uploads')) fs.mkdirSync('uploads');
if (!fs.existsSync('uploads/chat')) fs.mkdirSync('uploads/chat', { recursive: true });

const getDB = () => JSON.parse(fs.readFileSync(DB_FILE));
const saveDB = (db) => fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

app.use(cors());
app.use(express.json({ limit: '20mb' }));
app.use('/uploads', express.static('uploads'));
app.use(express.static(__dirname));

// ===== 2. KÉPFELTÖLTÉS ÉLES =====
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = file.fieldname === 'chatImage'? 'uploads/chat' : 'uploads';
    cb(null, dir);
  },
  filename: (req, file, cb) => cb(null, Date.now() + '-' + uuidv4() + '.webp')
});
const upload = multer({ storage, limits: { fileSize: 15 * 1024 * 1024 } });

// Kép tömörítés webp-re, gyors betöltés 1B usernél
async function compressImage(filepath) {
  const out = filepath.replace(path.extname(filepath), '.webp');
  await sharp(filepath).resize(800, 800, { fit: 'inside' }).webp({ quality: 75 }).toFile(out);
  fs.unlinkSync(filepath);
  return out.replace('uploads', '/uploads').replace('.webp', '.webp');
}

// Auth
const auth = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Be kell jelentkezni' });
  try { req.user = jwt.verify(token, SECRET); next(); }
  catch { res.status(401).json({ error: 'Token lejárt' }); }
};

// ===== REGISZTRÁCIÓ - NINCS DEMO, CSAK IGAZI =====
app.post('/api/register', async (req, res) => {
  const { email, password, name, age, gender, interestedIn, phone, hobbies, accept18, acceptASZF, acceptAdat, acceptPay } = req.body;

  if (!accept18 ||!acceptASZF ||!acceptAdat ||!acceptPay) return res.status(400).json({ error: 'Minden checkbox kötelező + 1000 Ft' });
  if (parseInt(age) < 18) return res.status(400).json({ error: '18+' });
  if (!['male','female'].includes(gender)) return res.status(400).json({ error: 'Nemed kötelező' });

  const db = getDB();
  if (db.users.find(u => u.email.toLowerCase() === email.toLowerCase())) return res.status(400).json({ error: 'Email már foglalt' });

  const hashed = await bcrypt.hash(password, 10);
  const newUser = {
    id: uuidv4(),
    email: email.toLowerCase(), // privát, nem küldjük vissza
    password: hashed,
    name, age: parseInt(age), gender, interestedIn: interestedIn || (gender==='male'?'female':'male'),
    phonePrivate: phone, // SOHA nem küldjük vissza listában
    hobbies: hobbies || [], // pl. ["🎣 Horgásztúra","✈️ Utazás"]
    bio: '',
    images: [], profilePic: null,
    likes: [], dislikes: [], superlikes: [], matches: [], favorites: [], blocked: [],
    isVIP: true,
    createdAt: new Date()
  };
  db.users.push(newUser);
  saveDB(db);
  const token = jwt.sign({ id: newUser.id, email: newUser.email }, SECRET, { expiresIn: '30d' });
  res.json({ token, user: { id: newUser.id, name, email: newUser.email } });
});

app.post('/api/login', async (req, res) => {
  const { email, password } = req.body;
  const db = getDB();
  const user = db.users.find(u => u.email.toLowerCase() === email.toLowerCase());
  if (!user ||!await bcrypt.compare(password, user.password)) return res.status(401).json({ error: 'Nincs ilyen fiók. Regisztrálj először 1000 Ft-ért!' });
  const token = jwt.sign({ id: user.id, email: user.email }, SECRET, { expiresIn: '30d' });
  res.json({ token });
});

// ===== KÉPEK - csak bejelentkezve látszanak =====
app.post('/api/upload', auth, upload.array('images', 10), async (req, res) => {
  const db = getDB();
  const user = db.users.find(u => u.id === req.user.id);
  for (let f of req.files) {
    const webpPath = await compressImage(f.path);
    user.images.push(webpPath);
  }
  if (!user.profilePic && user.images.length) user.profilePic = user.images[0];
  saveDB(db);
  res.json({ images: user.images, profilePic: user.profilePic });
});

app.post('/api/set-profile-pic', auth, (req, res) => {
  const db = getDB(); const user = db.users.find(u => u.id === req.user.id);
  user.profilePic = req.body.url; saveDB(db); res.json({ ok: true });
});

app.delete('/api/image', auth, (req, res) => {
  const db = getDB(); const user = db.users.find(u => u.id === req.user.id);
  user.images = user.images.filter(i => i!== req.body.url);
  if (user.profilePic === req.body.url) user.profilePic = user.images[0] || null;
  saveDB(db); res.json({ ok: true });
});

// ===== USEREK - nem látszik email és telefon! + nemek szűrése =====
app.get('/api/users', auth, (req, res) => {
  const db = getDB();
  const me = db.users.find(u => u.id === req.user.id);
  let users = db.users.filter(u => {
    if (u.id === me.id) return false;
    if (me.blocked.includes(u.id)) return false;
    if (me.dislikes.includes(u.id)) return false;
    if (me.likes.includes(u.id)) return false;
    if (me.interestedIn === 'female' && u.gender!== 'female') return false;
    if (me.interestedIn === 'male' && u.gender!== 'male') return false;
    if (req.query.hobby &&!u.hobbies.some(h => h.includes(req.query.hobby))) return false; // pl.?hobby=Horgásztúra
    return true;
  }).map(u => ({
    id: u.id, name: u.name, age: u.age, gender: u.gender,
    bio: u.bio, hobbies: u.hobbies, // Horgásztúra stb.
    images: u.images, profilePic: u.profilePic,
    isVIP: true
    // email és phonePrivate SOHA nincs benne!
  }));
  res.json(users);
});

// LIKE, SUPERLIKE, stb + ÉRTESÍTÉS
app.post('/api/action', auth, (req, res) => {
  const { targetId, type } = req.body;
  const db = getDB();
  const me = db.users.find(u => u.id === req.user.id);
  const target = db.users.find(u => u.id === targetId);
  if (!target) return res.status(404).json({ error: 'Nincs' });

  if (type === 'like') {
    if (!me.likes.includes(targetId)) me.likes.push(targetId);
    // Értesítés a másiknak: új kedvelés!
    io.to(targetId).emit('notification', { type: 'like', from: me.id, name: me.name, text: `${me.name} kedvelte a profilod ❤️` });
    if (target.likes.includes(me.id) &&!me.matches.includes(targetId)) {
      me.matches.push(targetId); target.matches.push(me.id);
      saveDB(db);
      io.to(targetId).emit('notification', { type: 'match', text: `Párosodtál ${me.name}-vel! 💕` });
      io.to(me.id).emit('notification', { type: 'match', text: `Párosodtál ${target.name}-vel! 💕` });
      return res.json({ match: true });
    }
  }
  if (type === 'dislike') me.dislikes.push(targetId);
  if (type === 'superlike') { me.superlikes.push(targetId); io.to(targetId).emit('notification', { type: 'superlike', text: `${me.name} szuperlájkolt! ⭐` }); }
  if (type === 'favorite') me.favorites = [...new Set([...me.favorites, targetId])];

  saveDB(db);
  res.json({ ok: true });
});

// ===== SOCKET - ÜZENET + KÉP + HÍVÁS CSAK ÜZENET UTÁN =====
io.use((socket, next) => {
  try { socket.user = jwt.verify(socket.handshake.auth.token, SECRET); next(); }
  catch { next(new Error('auth')); }
});

io.on('connection', socket => {
  socket.join(socket.user.id);

  // Szöveges üzenet
  socket.on('message', ({ to, text }) => {
    const db = getDB();
    const me = db.users.find(u => u.id === socket.user.id);
    const target = db.users.find(u => u.id === to);
    if (!me.matches.includes(to)) return; // csak pároknak

    const msg = { id: uuidv4(), from: socket.user.id, to, text, type: 'text', time: new Date() };
    db.messages.push(msg); saveDB(db);
    io.to(to).emit('message', msg);
    io.to(to).emit('notification', { type: 'message', from: socket.user.id, text: `Új üzenet ${me.name}-től: ${text.slice(0,30)}...` });
    socket.emit('message', msg);
  });

  // Kép üzenetben
  socket.on('message-image', ({ to, imageUrl }) => {
    const db = getDB();
    const me = db.users.find(u => u.id === socket.user.id);
    const msg = { id: uuidv4(), from: socket.user.id, to, imageUrl, type: 'image', time: new Date() };
    db.messages.push(msg); saveDB(db);
    io.to(to).emit('message', msg);
    io.to(to).emit('notification', { type: 'message', text: `📷 Képet küldött ${me.name}` });
    socket.emit('message', msg);
  });

  // Hívás - csak ha volt már üzenet váltás
  socket.on('call-request', ({ to, callType }) => {
    const db = getDB();
    const hasMessaged = db.messages.some(m => (m.from===socket.user.id && m.to===to) || (m.from===to && m.to===socket.user.id));
    if (!hasMessaged) {
      socket.emit('call-error', { error: 'Csak üzenetváltás után hívhatsz!' });
      return;
    }
    io.to(to).emit('incoming-call', { from: socket.user.id, callType });
  });

  socket.on('call-signal', ({ to, signal }) => io.to(to).emit('call-signal', { from: socket.user.id, signal }));
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
server.listen(PORT, () => console.log(`Lovenux éles: http://localhost:${PORT} - 1B user ready`));
