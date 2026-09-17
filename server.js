require('dotenv').config();
const cluster = require('cluster');
const os = require('os');

// === 1B OPTIMIZATION === Cluster support for vertical scaling to 1B users
if (cluster.isPrimary) {
  const numCPUs = parseInt(process.env.CLUSTER_WORKERS || '', 10) || os.cpus().length;
  console.log('[Cluster] Primary ' + process.pid + ' is running');
  console.log('[Cluster] Forking ' + numCPUs + ' workers for 1B scale');

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log('[Cluster] Worker ' + worker.process.pid + ' died (' + (signal || code) + '). Restarting...');
    cluster.fork();
  });

  cluster.on('online', (worker) => {
    console.log('[Cluster] Worker ' + worker.process.pid + ' online');
  });
} else {
  const express = require('express');
  const cors = require('cors');
  const helmet = require('helmet');
  const bcrypt = require('bcryptjs');
  const jwt = require('jsonwebtoken');
  const multer = require('multer');
  const compression = require('compression');
  const morgan = require('morgan');
  const { Pool } = require('pg');
  const crypto = require('crypto');
  const fs = require('fs');
  const path = require('path');
  const rateLimit = require('express-rate-limit');

  const app = express();
  const PORT = process.env.PORT || 10000;
  const JWT_SECRET = process.env.JWT_SECRET || 'lovenux-super-secret-1b-ready-jwt-key-change-in-prod';
  const SHARD_COUNT = 16;

  // === 1B OPTIMIZATION === Sharding logic
  const DATABASE_URLS_RAW = process.env.DATABASE_URLS || process.env.DATABASE_URL || '';
  let DATABASE_URLS = [];
  if (DATABASE_URLS_RAW) {
    if (DATABASE_URLS_RAW.includes(',')) {
      DATABASE_URLS = DATABASE_URLS_RAW.split(',').map(s => s.trim()).filter(Boolean);
    } else {
      DATABASE_URLS = [DATABASE_URLS_RAW.trim()];
    }
  }

  const DB_MODE = DATABASE_URLS.length > 0 ? 'postgres' : 'json';
  if (DB_MODE === 'json') {
    console.warn('[DB] WARNING: DATABASE_URLS not set - using JSON fallback. JSON is NOT for 1B scale! Set DATABASE_URLS for production.');
  }

  function hashEmail(email) {
    const hash = crypto.createHash('md5').update(email.toLowerCase().trim()).digest('hex');
    return parseInt(hash.substring(0, 8), 16);
  }

  function getShardIndex(email) {
    return hashEmail(email) % SHARD_COUNT;
  }

  // === 1B OPTIMIZATION === Connection pool tuning for 1B concurrency
  let pools = [];
  if (DB_MODE === 'postgres') {
    for (let i = 0; i < SHARD_COUNT; i++) {
      let connStr;
      if (DATABASE_URLS.length >= SHARD_COUNT) {
        connStr = DATABASE_URLS[i];
      } else if (DATABASE_URLS.length > 0) {
        connStr = DATABASE_URLS[i % DATABASE_URLS.length];
      } else {
        connStr = DATABASE_URLS[0];
      }
      const pool = new Pool({
        connectionString: connStr,
        max: 30,
        idleTimeoutMillis: 10000,
        connectionTimeoutMillis: 5000,
        statement_timeout: 10000,
        query_timeout: 10000,
        keepAlive: true,
        application_name: 'lovenux-1b-shard-' + i
      });
      pool.on('error', (err) => {
        console.error('[Pool shard-' + i + '] Unexpected error', err);
      });
      pools.push(pool);
    }
    console.log('[DB] Initialized ' + SHARD_COUNT + ' shard pools for 1B scale');
  }

  // === 1B OPTIMIZATION === Redis optional cache with in-memory fallback
  let redisClient = null;
  const memCache = new Map();
  let cacheStats = { hits: 0, misses: 0, sets: 0 };

  try {
    if (process.env.REDIS_URL) {
      const Redis = require('ioredis');
      redisClient = new Redis(process.env.REDIS_URL, {
        maxRetriesPerRequest: 2,
        enableReadyCheck: true,
        retryStrategy: (times) => Math.min(times * 50, 2000)
      });
      redisClient.on('connect', () => console.log('[Redis] Connected for 1B cache layer'));
      redisClient.on('error', (err) => console.error('[Redis] Error', err.message));
    } else {
      console.log('[Cache] REDIS_URL not set, using in-memory Map cache (not distributed)');
    }
  } catch (e) {
    console.warn('[Cache] ioredis not available, using in-memory fallback:', e.message);
    redisClient = null;
  }

  async function getCache(key) {
    try {
      if (redisClient) {
        const val = await redisClient.get(key);
        if (val !== null) {
          cacheStats.hits++;
          return JSON.parse(val);
        }
        cacheStats.misses++;
        return null;
      } else {
        const entry = memCache.get(key);
        if (!entry) { cacheStats.misses++; return null; }
        if (Date.now() > entry.exp) { memCache.delete(key); cacheStats.misses++; return null; }
        cacheStats.hits++;
        return entry.val;
      }
    } catch {
      cacheStats.misses++;
      return null;
    }
  }

  async function setCache(key, val, ttlSeconds = 300) {
    try {
      cacheStats.sets++;
      if (redisClient) {
        await redisClient.set(key, JSON.stringify(val), 'EX', ttlSeconds);
      } else {
        memCache.set(key, { val, exp: Date.now() + ttlSeconds * 1000 });
        if (memCache.size > 10000) {
          const firstKey = memCache.keys().next().value;
          memCache.delete(firstKey);
        }
      }
    } catch {}
  }

  async function delCache(key) {
    try {
      if (redisClient) await redisClient.del(key);
      else memCache.delete(key);
    } catch {}
  }

  async function delCachePattern(prefix) {
    try {
      if (redisClient) {
        const keys = await redisClient.keys(prefix + '*');
        if (keys.length) await redisClient.del(...keys);
      } else {
        for (const k of memCache.keys()) {
          if (k.startsWith(prefix)) memCache.delete(k);
        }
      }
    } catch {}
  }

  // === 1B OPTIMIZATION === S3 upload logic
  let s3Client = null;
  let s3Bucket = null;
  const hasS3 = !!(process.env.AWS_S3_BUCKET && process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY);
  if (hasS3) {
    try {
      const { S3Client } = require('@aws-sdk/client-s3');
      s3Client = new S3Client({
        region: process.env.AWS_REGION || 'eu-central-1',
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID,
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
      });
      s3Bucket = process.env.AWS_S3_BUCKET;
      console.log('[S3] S3 upload enabled bucket=' + s3Bucket);
    } catch (e) {
      console.warn('[S3] Failed to init S3 client, fallback to local:', e.message);
      s3Client = null;
    }
  } else {
    console.log('[Upload] S3 env not set, using local disk storage');
  }

  const uploadDir = path.join(__dirname, 'uploads');
  if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

  const storage = s3Client ? multer.memoryStorage() : multer.diskStorage({
    destination: (req, file, cb) => cb(null, uploadDir),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname);
      cb(null, Date.now() + '-' + Math.random().toString(36).substring(7) + ext);
    }
  });
  const upload = multer({
    storage,
    limits: { fileSize: 10 * 1024 * 1024 },
    fileFilter: (req, file, cb) => {
      if (file.mimetype.startsWith('image/')) cb(null, true);
      else cb(new Error('Only images allowed'));
    }
  });

  async function handleUpload(file) {
    if (!file) return null;
    if (s3Client) {
      const { PutObjectCommand } = require('@aws-sdk/client-s3');
      const key = 'avatars/' + Date.now() + '-' + Math.random().toString(36).substring(7) + path.extname(file.originalname);
      await s3Client.send(new PutObjectCommand({
        Bucket: s3Bucket,
        Key: key,
        Body: file.buffer,
        ContentType: file.mimetype,
        ACL: 'public-read'
      }));
      const baseUrl = process.env.AWS_S3_PUBLIC_URL || 'https://' + s3Bucket + '.s3.' + (process.env.AWS_REGION || 'eu-central-1') + '.amazonaws.com';
      return baseUrl + '/' + key;
    } else {
      return '/uploads/' + file.filename;
    }
  }

  // JSON fallback stores
  const JSON_DIR = path.join(__dirname, 'data');
  if (DB_MODE === 'json' && !fs.existsSync(JSON_DIR)) fs.mkdirSync(JSON_DIR, { recursive: true });
  function jsonPath(name) { return path.join(JSON_DIR, name + '.json'); }
  function readJson(name, def) {
    try {
      if (!fs.existsSync(jsonPath(name))) return def;
      return JSON.parse(fs.readFileSync(jsonPath(name), 'utf8'));
    } catch { return def; }
  }
  function writeJson(name, data) {
    try { fs.writeFileSync(jsonPath(name), JSON.stringify(data, null, 2)); } catch {}
  }

  // === 1B OPTIMIZATION === Security & performance middlewares
  app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
    hsts: { maxAge: 31536000, includeSubDomains: true, preload: true },
    noSniff: true,
    frameguard: { action: 'deny' },
    xssFilter: true
  }));
  app.use(cors({
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : '*',
    credentials: true
  }));
  // === 1B OPTIMIZATION === Compression level tuning
  app.use(compression({ level: 6, threshold: 1024, filter: (req, res) => {
    if (req.headers['x-no-compression']) return false;
    return compression.filter(req, res);
  }}));
  app.use(express.json({ limit: '2mb' }));
  app.use(express.urlencoded({ extended: true }));
  if (DB_MODE === 'json' || !s3Client) {
    app.use('/uploads', express.static(uploadDir));
  }

  // === 1B OPTIMIZATION === Request logging skip health checks
  app.use(morgan('combined', {
    skip: (req) => req.path.startsWith('/api/health') || req.path.startsWith('/api/heart') || req.path.startsWith('/api/metrics')
  }));

  // === 1B OPTIMIZATION === Rate limiting
  const generalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 1000,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' }
  });
  const authLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 20,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many auth attempts, try again in a minute' }
  });

  app.use('/api/', generalLimiter);

  // DB init with composite indexes
  async function initDb() {
    if (DB_MODE === 'json') return;
    for (let i = 0; i < SHARD_COUNT; i++) {
      const pool = pools[i];
      try {
        await pool.query(
          'CREATE TABLE IF NOT EXISTS users_' + i + ' (' +
          'id SERIAL PRIMARY KEY, ' +
          'email TEXT UNIQUE NOT NULL, ' +
          'password TEXT NOT NULL, ' +
          'name TEXT, age INT, gender TEXT, city TEXT, bio TEXT, avatar TEXT, ' +
          'interests TEXT[], premium BOOLEAN DEFAULT false, ' +
          'barion_id TEXT, last_active TIMESTAMP DEFAULT NOW(), ' +
          'created_at TIMESTAMP DEFAULT NOW() )'
        );
        await pool.query(
          'CREATE TABLE IF NOT EXISTS likes_' + i + ' (' +
          'id SERIAL PRIMARY KEY, ' +
          'from_email TEXT NOT NULL, to_email TEXT NOT NULL, ' +
          'created_at TIMESTAMP DEFAULT NOW(), UNIQUE(from_email, to_email))'
        );
        await pool.query(
          'CREATE TABLE IF NOT EXISTS matches_' + i + ' (' +
          'id SERIAL PRIMARY KEY, ' +
          'user1 TEXT NOT NULL, user2 TEXT NOT NULL, ' +
          'created_at TIMESTAMP DEFAULT NOW(), UNIQUE(user1, user2))'
        );
        await pool.query(
          'CREATE TABLE IF NOT EXISTS messages_' + i + ' (' +
          'id SERIAL PRIMARY KEY, ' +
          'from_email TEXT NOT NULL, to_email TEXT NOT NULL, ' +
          'content TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())'
        );

        // === 1B OPTIMIZATION === Composite indexes for 1B scale
        await pool.query('CREATE INDEX IF NOT EXISTS idx_users_' + i + '_age_gender_city ON users_' + i + ' (age, gender, city)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_users_' + i + '_last_active ON users_' + i + ' (last_active DESC)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_users_' + i + '_created ON users_' + i + ' (created_at DESC)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_users_' + i + '_email ON users_' + i + ' (email)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_likes_' + i + '_to ON likes_' + i + ' (to_email, created_at DESC)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_likes_' + i + '_from ON likes_' + i + ' (from_email, created_at DESC)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_matches_' + i + '_user1_user2 ON matches_' + i + ' (user1, user2)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_matches_' + i + '_user2_user1 ON matches_' + i + ' (user2, user1)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_messages_' + i + '_from_to_at ON messages_' + i + ' (from_email, to_email, created_at DESC)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_messages_' + i + '_to_at ON messages_' + i + ' (to_email, created_at DESC)');
      } catch (e) {
        console.error('[DB] Shard ' + i + ' init error', e.message);
      }
    }
    console.log('[DB] All shards initialized with 1B indexes');
  }

  // Helpers
  function getPoolForEmail(email) {
    if (DB_MODE === 'json') return null;
    const idx = getShardIndex(email);
    return pools[idx];
  }

  // === 1B OPTIMIZATION === Improved findUserByEmail with Redis cache
  async function findUserByEmail(email) {
    if (!email) return null;
    const normalized = email.toLowerCase().trim();
    const cacheKey = 'user:' + normalized;
    const cached = await getCache(cacheKey);
    if (cached) return cached;

    let user = null;
    if (DB_MODE === 'postgres') {
      const pool = getPoolForEmail(normalized);
      const idx = getShardIndex(normalized);
      try {
        const res = await pool.query('SELECT * FROM users_' + idx + ' WHERE email = $1 LIMIT 1', [normalized]);
        if (res.rows.length) user = res.rows[0];
      } catch (e) {
        console.error('[findUser] shard error', e.message);
      }
    } else {
      const users = readJson('users', []);
      user = users.find(u => u.email.toLowerCase() === normalized) || null;
    }

    if (user) {
      await setCache(cacheKey, user, 300);
    }
    return user;
  }

  async function saveUser(user) {
    const normalized = user.email.toLowerCase().trim();
    await delCache('user:' + normalized);
    await delCachePattern('discover:');
    if (DB_MODE === 'postgres') {
      const idx = getShardIndex(normalized);
      const pool = pools[idx];
      const existing = await findUserByEmail(normalized); // note: will miss cache after del
      // Use direct query to avoid recursion cache miss issue
      if (existing && existing.id) {
        await pool.query(
          'UPDATE users_' + idx + ' SET name=$1, age=$2, gender=$3, city=$4, bio=$5, avatar=$6, interests=$7, premium=$8, last_active=NOW() WHERE email=$9',
          [user.name, user.age, user.gender, user.city, user.bio, user.avatar, user.interests, !!user.premium, normalized]
        );
      } else {
        await pool.query(
          'INSERT INTO users_' + idx + ' (email, password, name, age, gender, city, bio, avatar, interests, premium, last_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW()) ON CONFLICT (email) DO UPDATE SET name=$3, age=$4, gender=$5, city=$6, bio=$7, avatar=$8, interests=$9, premium=$10, last_active=NOW()',
          [normalized, user.password, user.name, user.age, user.gender, user.city, user.bio, user.avatar, user.interests, !!user.premium]
        );
      }
    } else {
      const users = readJson('users', []);
      const idx = users.findIndex(u => u.email.toLowerCase() === normalized);
      if (idx >= 0) users[idx] = { ...users[idx], ...user, email: normalized };
      else users.push({ ...user, email: normalized, created_at: new Date().toISOString() });
      writeJson('users', users);
    }
  }

  function authMiddleware(req, res, next) {
    const header = req.headers.authorization;
    if (!header) return res.status(401).json({ error: 'No token' });
    const token = header.split(' ')[1] || header;
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      req.user = decoded;
      next();
    } catch {
      return res.status(401).json({ error: 'Invalid token' });
    }
  }

  // === 1B OPTIMIZATION === Health endpoints
  app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), worker: process.pid, mode: DB_MODE, shardCount: SHARD_COUNT });
  });
  app.get('/api/heart', (req, res) => {
    res.json({ heart: 'beat', pid: process.pid, uptime: process.uptime() });
  });
  app.get('/api/metrics', async (req, res) => {
    let shardCounts = [];
    if (DB_MODE === 'postgres') {
      try {
        const counts = await Promise.all(pools.map(async (pool, i) => {
          try {
            const r = await pool.query('SELECT COUNT(*) as c FROM users_' + i);
            return { shard: i, users: parseInt(r.rows[0].c, 10), poolTotal: pool.totalCount, poolIdle: pool.idleCount, poolWaiting: pool.waitingCount };
          } catch { return { shard: i, users: -1 }; }
        }));
        shardCounts = counts;
      } catch {}
    } else {
      const users = readJson('users', []);
      shardCounts = [{ shard: 'json', users: users.length }];
    }
    res.json({
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      cache: cacheStats,
      shards: shardCounts,
      redis: !!redisClient,
      s3: !!s3Client,
      worker: process.pid,
      version: '1b-ready'
    });
  });

  // Auth routes
  app.post('/api/register', authLimiter, upload.single('avatar'), async (req, res) => {
    try {
      const { email, password, name, age, gender, city, bio, interests } = req.body;
      if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
      const normalized = email.toLowerCase().trim();
      const existing = await findUserByEmail(normalized);
      if (existing) return res.status(400).json({ error: 'User already exists' });
      const hashed = await bcrypt.hash(password, 12);
      let avatarUrl = null;
      if (req.file) avatarUrl = await handleUpload(req.file);
      const newUser = {
        email: normalized,
        password: hashed,
        name: name || normalized.split('@')[0],
        age: age ? parseInt(age, 10) : null,
        gender: gender || null,
        city: city || null,
        bio: bio || '',
        avatar: avatarUrl,
        interests: interests ? (Array.isArray(interests) ? interests : JSON.parse(interests)) : [],
        premium: false,
        last_active: new Date().toISOString()
      };
      await saveUser(newUser);
      const token = jwt.sign({ email: normalized, name: newUser.name }, JWT_SECRET, { expiresIn: '30d' });
      const { password: _, ...safe } = newUser;
      res.json({ token, user: safe });
    } catch (e) {
      console.error('[register] error', e);
      res.status(500).json({ error: 'Register failed' });
    }
  });

  app.post('/api/login', authLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
      const user = await findUserByEmail(email);
      if (!user) return res.status(400).json({ error: 'User not found' });
      const ok = await bcrypt.compare(password, user.password);
      if (!ok) return res.status(400).json({ error: 'Invalid password' });
      if (DB_MODE === 'postgres') {
        const idx = getShardIndex(email.toLowerCase().trim());
        await pools[idx].query('UPDATE users_' + idx + ' SET last_active=NOW() WHERE email=$1', [email.toLowerCase().trim()]).catch(() => {});
        await delCache('user:' + email.toLowerCase().trim());
      }
      const token = jwt.sign({ email: user.email.toLowerCase().trim(), name: user.name }, JWT_SECRET, { expiresIn: '30d' });
      const { password: _, ...safe } = user;
      res.json({ token, user: safe });
    } catch (e) {
      console.error('[login] error', e);
      res.status(500).json({ error: 'Login failed' });
    }
  });

  app.post('/api/forgot-password', authLimiter, async (req, res) => {
    try {
      const { email } = req.body;
      if (!email) return res.status(400).json({ error: 'Email required' });
      const user = await findUserByEmail(email);
      if (!user) return res.json({ message: 'If user exists, reset link sent' });
      const resetToken = jwt.sign({ email: email.toLowerCase().trim(), purpose: 'reset' }, JWT_SECRET, { expiresIn: '1h' });
      console.log('[forgot-password] Reset token for ' + email + ': ' + resetToken);
      res.json({ message: 'If user exists, reset link sent', debugToken: process.env.NODE_ENV !== 'production' ? resetToken : undefined });
    } catch (e) {
      res.status(500).json({ error: 'Failed' });
    }
  });

  app.post('/api/change-password', authLimiter, authMiddleware, async (req, res) => {
    try {
      const { oldPassword, newPassword } = req.body;
      if (!oldPassword || !newPassword) return res.status(400).json({ error: 'Both passwords required' });
      const user = await findUserByEmail(req.user.email);
      if (!user) return res.status(404).json({ error: 'User not found' });
      const ok = await bcrypt.compare(oldPassword, user.password);
      if (!ok) return res.status(400).json({ error: 'Old password invalid' });
      const hashed = await bcrypt.hash(newPassword, 12);
      user.password = hashed;
      await saveUser(user);
      await delCache('user:' + req.user.email);
      res.json({ message: 'Password changed' });
    } catch (e) {
      res.status(500).json({ error: 'Change password failed' });
    }
  });

  app.get('/api/me', authMiddleware, async (req, res) => {
    try {
      const user = await findUserByEmail(req.user.email);
      if (!user) return res.status(404).json({ error: 'User not found' });
      const { password: _, ...safe } = user;
      res.json(safe);
    } catch (e) {
      res.status(500).json({ error: 'Failed to get me' });
    }
  });

  app.put('/api/me', authMiddleware, upload.single('avatar'), async (req, res) => {
    try {
      const user = await findUserByEmail(req.user.email);
      if (!user) return res.status(404).json({ error: 'User not found' });
      const { name, age, gender, city, bio, interests } = req.body;
      if (name) user.name = name;
      if (age) user.age = parseInt(age, 10);
      if (gender) user.gender = gender;
      if (city) user.city = city;
      if (bio !== undefined) user.bio = bio;
      if (interests) user.interests = Array.isArray(interests) ? interests : JSON.parse(interests);
      if (req.file) {
        user.avatar = await handleUpload(req.file);
      }
      await saveUser(user);
      const { password: _, ...safe } = user;
      res.json(safe);
    } catch (e) {
      console.error('[PUT /me]', e);
      res.status(500).json({ error: 'Update failed' });
    }
  });

  // === 1B OPTIMIZATION === Optimized discover with Promise.all
  app.get('/api/discover', authMiddleware, async (req, res) => {
    try {
      const page = Math.max(1, parseInt(req.query.page || '1', 10));
      const limit = Math.min(50, Math.max(1, parseInt(req.query.limit || '20', 10)));
      const offset = (page - 1) * limit;
      const currentEmail = req.user.email.toLowerCase().trim();
      const cacheKey = 'discover:' + currentEmail + ':' + page + ':' + limit + ':' + (req.query.gender || '') + ':' + (req.query.city || '');
      const cached = await getCache(cacheKey);
      if (cached) return res.json(cached);

      let users = [];
      if (DB_MODE === 'postgres') {
        const genderFilter = req.query.gender;
        const cityFilter = req.query.city;
        // Parallel queries across shards for 1B scale
        const shardQueries = pools.map(async (pool, shardIdx) => {
          try {
            let q = 'SELECT email, name, age, gender, city, bio, avatar, interests, premium, last_active, created_at FROM users_' + shardIdx + ' WHERE email != $1';
            const params = [currentEmail];
            let paramIdx = 2;
            if (genderFilter) { q += ' AND gender = $' + paramIdx; params.push(genderFilter); paramIdx++; }
            if (cityFilter) { q += ' AND city ILIKE $' + paramIdx; params.push('%' + cityFilter + '%'); paramIdx++; }
            q += ' AND email NOT IN (SELECT to_email FROM likes_' + shardIdx + ' WHERE from_email = $' + paramIdx + ')';
            params.push(currentEmail); paramIdx++;
            q += ' ORDER BY last_active DESC LIMIT $' + paramIdx + ' OFFSET $' + (paramIdx + 1);
            params.push(limit * 2, offset);
            const r = await pool.query(q, params);
            return r.rows;
          } catch (e) {
            console.error('[discover shard ' + shardIdx + ']', e.message);
            return [];
          }
        });
        const results = await Promise.all(shardQueries);
        users = results.flat()
          .sort((a, b) => new Date(b.last_active) - new Date(a.last_active))
          .slice(0, limit);
      } else {
        const all = readJson('users', []);
        const likes = readJson('likes', []).filter(l => l.from_email === currentEmail).map(l => l.to_email);
        let filtered = all.filter(u => u.email !== currentEmail && !likes.includes(u.email));
        if (req.query.gender) filtered = filtered.filter(u => u.gender === req.query.gender);
        if (req.query.city) filtered = filtered.filter(u => u.city && u.city.toLowerCase().includes(req.query.city.toLowerCase()));
        filtered.sort((a, b) => new Date(b.last_active || b.created_at) - new Date(a.last_active || a.created_at));
        users = filtered.slice(offset, offset + limit).map(u => {
          const { password: _, ...safe } = u;
          return safe;
        });
      }

      const result = { users, page, limit, hasMore: users.length === limit };
      await setCache(cacheKey, result, 60);
      res.json(result);
    } catch (e) {
      console.error('[discover]', e);
      res.status(500).json({ error: 'Discover failed' });
    }
  });

  app.post('/api/like', authMiddleware, async (req, res) => {
    try {
      const from = req.user.email.toLowerCase().trim();
      const to = (req.body.email || req.body.to || '').toLowerCase().trim();
      if (!to) return res.status(400).json({ error: 'Target email required' });
      if (from === to) return res.status(400).json({ error: 'Cannot like yourself' });
      const target = await findUserByEmail(to);
      if (!target) return res.status(404).json({ error: 'User not found' });

      if (DB_MODE === 'postgres') {
        const fromShard = getShardIndex(from);
        const toShard = getShardIndex(to);
        await pools[fromShard].query('INSERT INTO likes_' + fromShard + ' (from_email, to_email) VALUES ($1,$2) ON CONFLICT (from_email, to_email) DO NOTHING', [from, to]);
        const check = await pools[fromShard].query('SELECT 1 FROM likes_' + getShardIndex(to) + ' as l WHERE l.from_email=$1 AND l.to_email=$2 LIMIT 1', [to, from]).catch(async () => {
          return await pools[toShard].query('SELECT 1 FROM likes_' + toShard + ' WHERE from_email=$1 AND to_email=$2 LIMIT 1', [to, from]);
        });
        // Check mutual like for match - check both shards
        let mutual = false;
        try {
          const r1 = await pools[toShard].query('SELECT 1 FROM likes_' + toShard + ' WHERE from_email=$1 AND to_email=$2 LIMIT 1', [to, from]);
          mutual = r1.rows.length > 0;
        } catch {
          const r2 = await pools[fromShard].query('SELECT 1 FROM likes_' + fromShard + ' WHERE from_email=$1 AND to_email=$2 LIMIT 1', [to, from]);
          mutual = r2.rows.length > 0;
        }

        let isMatch = false;
        if (mutual) {
          const sorted = [from, to].sort();
          const matchShard = getShardIndex(sorted[0]);
          await pools[matchShard].query('INSERT INTO matches_' + matchShard + ' (user1, user2) VALUES ($1,$2) ON CONFLICT (user1, user2) DO NOTHING', [sorted[0], sorted[1]]);
          isMatch = true;
        }
        await delCachePattern('discover:' + from);
        await delCache('likes:' + from);
        await delCache('matches:' + from);
        res.json({ liked: true, match: isMatch });
      } else {
        const likes = readJson('likes', []);
        if (!likes.find(l => l.from_email === from && l.to_email === to)) {
          likes.push({ from_email: from, to_email: to, created_at: new Date().toISOString() });
          writeJson('likes', likes);
        }
        const mutual = likes.find(l => l.from_email === to && l.to_email === from);
        if (mutual) {
          const matches = readJson('matches', []);
          const sorted = [from, to].sort();
          if (!matches.find(m => m.user1 === sorted[0] && m.user2 === sorted[1])) {
            matches.push({ user1: sorted[0], user2: sorted[1], created_at: new Date().toISOString() });
            writeJson('matches', matches);
          }
          return res.json({ liked: true, match: true });
        }
        res.json({ liked: true, match: false });
      }
    } catch (e) {
      console.error('[like]', e);
      res.status(500).json({ error: 'Like failed' });
    }
  });

  app.get('/api/likes', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      if (DB_MODE === 'postgres') {
        // Need to check all shards where to_email = me
        const results = await Promise.all(pools.map(async (pool, idx) => {
          try {
            const r = await pool.query('SELECT from_email, created_at FROM likes_' + idx + ' WHERE to_email=$1 ORDER BY created_at DESC LIMIT 100', [email]);
            return r.rows;
          } catch { return []; }
        }));
        const flat = results.flat().sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        // Enrich with user data
        const enriched = await Promise.all(flat.slice(0, 50).map(async (like) => {
          const u = await findUserByEmail(like.from_email);
          if (!u) return null;
          const { password: _, ...safe } = u;
          return { ...safe, liked_at: like.created_at };
        }));
        res.json({ likes: enriched.filter(Boolean) });
      } else {
        const likes = readJson('likes', []).filter(l => l.to_email === email).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        const users = readJson('users', []);
        const enriched = likes.map(l => {
          const u = users.find(x => x.email === l.from_email);
          if (!u) return null;
          const { password: _, ...safe } = u;
          return { ...safe, liked_at: l.created_at };
        }).filter(Boolean);
        res.json({ likes: enriched });
      }
    } catch (e) {
      res.status(500).json({ error: 'Failed to get likes' });
    }
  });

  app.get('/api/likes/count', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      const cacheKey = 'likes_count:' + email;
      const cached = await getCache(cacheKey);
      if (cached !== null) return res.json(cached);
      let count = 0;
      if (DB_MODE === 'postgres') {
        const results = await Promise.all(pools.map(async (pool, idx) => {
          try {
            const r = await pool.query('SELECT COUNT(*) as c FROM likes_' + idx + ' WHERE to_email=$1', [email]);
            return parseInt(r.rows[0].c, 10);
          } catch { return 0; }
        }));
        count = results.reduce((a, b) => a + b, 0);
      } else {
        count = readJson('likes', []).filter(l => l.to_email === email).length;
      }
      const result = { count };
      await setCache(cacheKey, result, 30);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: 'Count failed' });
    }
  });

  app.delete('/api/like/:email', authMiddleware, async (req, res) => {
    try {
      const from = req.user.email.toLowerCase().trim();
      const to = req.params.email.toLowerCase().trim();
      if (DB_MODE === 'postgres') {
        const shard = getShardIndex(from);
        await pools[shard].query('DELETE FROM likes_' + shard + ' WHERE from_email=$1 AND to_email=$2', [from, to]);
        const sorted = [from, to].sort();
        const mShard = getShardIndex(sorted[0]);
        await pools[mShard].query('DELETE FROM matches_' + mShard + ' WHERE (user1=$1 AND user2=$2) OR (user1=$2 AND user2=$1)', [from, to]).catch(() => {});
      } else {
        let likes = readJson('likes', []);
        likes = likes.filter(l => !(l.from_email === from && l.to_email === to));
        writeJson('likes', likes);
        let matches = readJson('matches', []);
        matches = matches.filter(m => !((m.user1 === from && m.user2 === to) || (m.user1 === to && m.user2 === from)));
        writeJson('matches', matches);
      }
      await delCache('likes:' + from);
      await delCache('likes_count:' + to);
      await delCache('matches:' + from);
      res.json({ removed: true });
    } catch (e) {
      res.status(500).json({ error: 'Remove like failed' });
    }
  });

  app.get('/api/matches', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      const cacheKey = 'matches:' + email;
      const cached = await getCache(cacheKey);
      if (cached) return res.json(cached);

      let matches = [];
      if (DB_MODE === 'postgres') {
        const results = await Promise.all(pools.map(async (pool, idx) => {
          try {
            const r = await pool.query('SELECT user1, user2, created_at FROM matches_' + idx + ' WHERE user1=$1 OR user2=$1 ORDER BY created_at DESC', [email]);
            return r.rows;
          } catch { return []; }
        }));
        const flat = results.flat();
        const otherEmails = flat.map(m => m.user1 === email ? m.user2 : m.user1);
        const unique = [...new Set(otherEmails)];
        const enriched = await Promise.all(unique.map(async (e) => {
          const u = await findUserByEmail(e);
          if (!u) return null;
          const { password: _, ...safe } = u;
          const m = flat.find(x => (x.user1 === e && x.user2 === email) || (x.user2 === e && x.user1 === email));
          return { ...safe, matched_at: m ? m.created_at : null };
        }));
        matches = enriched.filter(Boolean);
      } else {
        const allMatches = readJson('matches', []).filter(m => m.user1 === email || m.user2 === email);
        const users = readJson('users', []);
        matches = allMatches.map(m => {
          const other = m.user1 === email ? m.user2 : m.user1;
          const u = users.find(x => x.email === other);
          if (!u) return null;
          const { password: _, ...safe } = u;
          return { ...safe, matched_at: m.created_at };
        }).filter(Boolean);
      }
      const result = { matches };
      await setCache(cacheKey, result, 60);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: 'Matches failed' });
    }
  });

  app.get('/api/messages/count', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      const cacheKey = 'msg_count:' + email;
      const cached = await getCache(cacheKey);
      if (cached) return res.json(cached);
      let count = 0;
      if (DB_MODE === 'postgres') {
        const results = await Promise.all(pools.map(async (pool, idx) => {
          try {
            const r = await pool.query('SELECT COUNT(*) as c FROM messages_' + idx + ' WHERE to_email=$1', [email]);
            return parseInt(r.rows[0].c, 10);
          } catch { return 0; }
        }));
        count = results.reduce((a, b) => a + b, 0);
      } else {
        count = readJson('messages', []).filter(m => m.to_email === email).length;
      }
      const result = { count };
      await setCache(cacheKey, result, 20);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: 'Count failed' });
    }
  });

  app.get('/api/messages', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      if (DB_MODE === 'postgres') {
        const results = await Promise.all(pools.map(async (pool, idx) => {
          try {
            const r = await pool.query(
              'SELECT from_email, to_email, COUNT(*) as cnt, MAX(created_at) as last_at FROM messages_' + idx + ' WHERE from_email=$1 OR to_email=$1 GROUP BY from_email, to_email ORDER BY last_at DESC LIMIT 100',
              [email]
            );
            return r.rows;
          } catch { return []; }
        }));
        const convMap = new Map();
        results.flat().forEach(row => {
          const other = row.from_email === email ? row.to_email : row.from_email;
          if (!convMap.has(other) || new Date(row.last_at) > new Date(convMap.get(other).last_at)) {
            convMap.set(other, row);
          }
        });
        const conversations = Array.from(convMap.values()).sort((a, b) => new Date(b.last_at) - new Date(a.last_at));
        res.json({ conversations });
      } else {
        const msgs = readJson('messages', []).filter(m => m.from_email === email || m.to_email === email);
        const map = new Map();
        msgs.forEach(m => {
          const other = m.from_email === email ? m.to_email : m.from_email;
          if (!map.has(other)) map.set(other, m);
          else if (new Date(m.created_at) > new Date(map.get(other).created_at)) map.set(other, m);
        });
        res.json({ conversations: Array.from(map.values()) });
      }
    } catch (e) {
      res.status(500).json({ error: 'Messages failed' });
    }
  });

  app.get('/api/messages/:email', authMiddleware, async (req, res) => {
    try {
      const me = req.user.email.toLowerCase().trim();
      const other = req.params.email.toLowerCase().trim();
      const page = Math.max(1, parseInt(req.query.page || '1', 10));
      const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '50', 10)));
      const offset = (page - 1) * limit;

      let messages = [];
      if (DB_MODE === 'postgres') {
        const shardsToQuery = [getShardIndex(me), getShardIndex(other)];
        const uniqShards = [...new Set(shardsToQuery)];
        const results = await Promise.all(uniqShards.map(async (idx) => {
          try {
            const r = await pools[idx].query(
              'SELECT * FROM messages_' + idx + ' WHERE (from_email=$1 AND to_email=$2) OR (from_email=$2 AND to_email=$1) ORDER BY created_at DESC LIMIT $3 OFFSET $4',
              [me, other, limit, offset]
            );
            return r.rows;
          } catch { return []; }
        }));
        messages = results.flat().sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
      } else {
        const all = readJson('messages', []).filter(m => (m.from_email === me && m.to_email === other) || (m.from_email === other && m.to_email === me));
        all.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        messages = all.slice(offset, offset + limit);
      }
      res.json({ messages, page, limit });
    } catch (e) {
      res.status(500).json({ error: 'Failed to get conversation' });
    }
  });

  app.post('/api/messages', authMiddleware, async (req, res) => {
    try {
      const from = req.user.email.toLowerCase().trim();
      const to = (req.body.to || req.body.email || '').toLowerCase().trim();
      const content = req.body.content || req.body.message;
      if (!to || !content) return res.status(400).json({ error: 'To and content required' });
      if (from === to) return res.status(400).json({ error: 'Cannot message self' });

      const msg = { from_email: from, to_email: to, content, created_at: new Date().toISOString() };

      if (DB_MODE === 'postgres') {
        const shard = getShardIndex(from);
        const r = await pools[shard].query('INSERT INTO messages_' + shard + ' (from_email, to_email, content) VALUES ($1,$2,$3) RETURNING *', [from, to, content]);
        await delCache('msg_count:' + to);
        res.json(r.rows[0]);
      } else {
        const msgs = readJson('messages', []);
        msgs.push({ id: msgs.length + 1, ...msg });
        writeJson('messages', msgs);
        res.json(msg);
      }
    } catch (e) {
      console.error('[messages POST]', e);
      res.status(500).json({ error: 'Send failed' });
    }
  });

  // Barion (keep existing logic)
  app.post('/api/barion/start', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      const user = await findUserByEmail(email);
      if (!user) return res.status(404).json({ error: 'User not found' });
      const paymentId = 'barion-' + Date.now() + '-' + Math.random().toString(36).substring(7);
      if (DB_MODE === 'postgres') {
        const idx = getShardIndex(email);
        await pools[idx].query('UPDATE users_' + idx + ' SET barion_id=$1 WHERE email=$2', [paymentId, email]);
      } else {
        const users = readJson('users', []);
        const u = users.find(x => x.email === email);
        if (u) { u.barion_id = paymentId; writeJson('users', users); }
      }
      await delCache('user:' + email);
      res.json({ paymentId, message: 'Barion payment started', gatewayUrl: 'https://secure.test.barion.com/Pay?Id=' + paymentId });
    } catch (e) {
      res.status(500).json({ error: 'Barion start failed' });
    }
  });

  app.post('/api/barion/confirm', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      const { paymentId } = req.body;
      const user = await findUserByEmail(email);
      if (!user) return res.status(404).json({ error: 'User not found' });
      // In real implementation verify with Barion API
      if (DB_MODE === 'postgres') {
        const idx = getShardIndex(email);
        await pools[idx].query('UPDATE users_' + idx + ' SET premium=true WHERE email=$1', [email]);
      } else {
        const users = readJson('users', []);
        const u = users.find(x => x.email === email);
        if (u) { u.premium = true; writeJson('users', users); }
      }
      await delCache('user:' + email);
      res.json({ premium: true, message: 'Premium activated' });
    } catch (e) {
      res.status(500).json({ error: 'Barion confirm failed' });
    }
  });

  app.delete('/api/account', authMiddleware, async (req, res) => {
    try {
      const email = req.user.email.toLowerCase().trim();
      if (DB_MODE === 'postgres') {
        const shard = getShardIndex(email);
        await pools[shard].query('DELETE FROM messages_' + shard + ' WHERE from_email=$1 OR to_email=$1', [email]).catch(() => {});
        await pools[shard].query('DELETE FROM likes_' + shard + ' WHERE from_email=$1 OR to_email=$1', [email]).catch(() => {});
        const matchDeletions = await Promise.all(pools.map(async (pool, idx) => {
          try { await pool.query('DELETE FROM matches_' + idx + ' WHERE user1=$1 OR user2=$1', [email]); } catch {}
        }));
        await pools[shard].query('DELETE FROM users_' + shard + ' WHERE email=$1', [email]);
      } else {
        let users = readJson('users', []);
        users = users.filter(u => u.email !== email);
        writeJson('users', users);
        let likes = readJson('likes', []);
        likes = likes.filter(l => l.from_email !== email && l.to_email !== email);
        writeJson('likes', likes);
        let matches = readJson('matches', []);
        matches = matches.filter(m => m.user1 !== email && m.user2 !== email);
        writeJson('matches', matches);
        let messages = readJson('messages', []);
        messages = messages.filter(m => m.from_email !== email && m.to_email !== email);
        writeJson('messages', messages);
      }
      await delCache('user:' + email);
      await delCachePattern(email);
      res.json({ deleted: true });
    } catch (e) {
      console.error('[DELETE account]', e);
      res.status(500).json({ error: 'Delete failed' });
    }
  });

  // Wildcard
  app.get('/api/*', (req, res) => {
    res.status(404).json({ error: 'API endpoint not found: ' + req.path });
  });

  // === 1B OPTIMIZATION === Error handling middleware
  app.use((err, req, res, next) => {
    console.error('[Error][' + req.path + ']', err.message);
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large, max 10MB' });
    }
    res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
  });

  // Graceful shutdown
  let server;
  async function start() {
    await initDb();
    server = app.listen(PORT, () => {
      console.log('[Worker ' + process.pid + '] Lovenux 1B Ready server listening on ' + PORT + ' mode=' + DB_MODE);
    });
  }

  // === 1B OPTIMIZATION === Graceful shutdown on SIGTERM/SIGINT
  async function shutdown(signal) {
    console.log('[Worker ' + process.pid + '] Received ' + signal + ', shutting down gracefully...');
    try {
      if (server) {
        server.close(() => console.log('[Worker ' + process.pid + '] HTTP server closed'));
      }
      if (DB_MODE === 'postgres') {
        await Promise.all(pools.map(p => p.end().catch(() => {})));
        console.log('[Worker ' + process.pid + '] DB pools closed');
      }
      if (redisClient) {
        await redisClient.quit().catch(() => {});
        console.log('[Worker ' + process.pid + '] Redis closed');
      }
      setTimeout(() => process.exit(0), 3000);
    } catch (e) {
      console.error('[Shutdown] error', e);
      process.exit(1);
    }
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('uncaughtException', (err) => {
    console.error('[uncaughtException]', err);
  });
  process.on('unhandledRejection', (reason) => {
    console.error('[unhandledRejection]', reason);
  });

  start();
}
