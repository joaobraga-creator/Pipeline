require('dotenv').config();
const fs = require('fs');
const path = require('path');

if (process.env.NODE_ENV === 'production') {
  function cleanJson(raw) {
    return raw.replace(/\r?\n/g, '').replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
  }
  if (process.env.GOOGLE_TOKENS) {
    const tokens = JSON.stringify(JSON.parse(cleanJson(process.env.GOOGLE_TOKENS)));
    fs.writeFileSync(path.join(__dirname, 'tokens.json'), tokens, 'utf8');
    console.log('[Startup] tokens.json gravado a partir de GOOGLE_TOKENS');
  }
  if (process.env.GOOGLE_ADC_CREDENTIALS) {
    const adc = JSON.stringify(JSON.parse(cleanJson(process.env.GOOGLE_ADC_CREDENTIALS)));
    fs.writeFileSync(path.join(__dirname, 'adc_credentials.json'), adc, 'utf8');
    console.log('[Startup] adc_credentials.json gravado a partir de GOOGLE_ADC_CREDENTIALS');
  }
}

const express = require('express');
const session = require('express-session');
const cors = require('cors');

const apiRoutes = require('./routes/api');
const authRoutes = require('./routes/auth');

const app = express();
const PORT = process.env.PORT || 3000;
const DEV_MODE = process.env.NODE_ENV !== 'production';

app.set('trust proxy', 1);
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true
}));

app.use(session({
  secret: process.env.SESSION_SECRET || 'pipeline-secret-change-me',
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production',
    httpOnly: true,
    maxAge: 24 * 60 * 60 * 1000
  }
}));

app.use(express.static(path.join(__dirname, '../frontend')));

app.use('/auth', authRoutes);
app.use('/api', ensureAuthenticated, apiRoutes);

app.get('/', (req, res) => {
  if (DEV_MODE || req.session.user) {
    res.sendFile(path.join(__dirname, '../frontend/index.html'));
  } else {
    res.redirect('/auth/login');
  }
});

app.get('/login', (req, res) => res.redirect('/auth/login'));

function ensureAuthenticated(req, res, next) {
  if (DEV_MODE) return next();
  if (!req.session.user) {
    return res.status(401).json({ error: 'Nao autenticado. Faca login em /auth/login' });
  }
  req.user = req.session.user;
  next();
}

app.get('/api/me', ensureAuthenticated, (req, res) => {
  if (DEV_MODE) {
    return res.json({ email: process.env.DEV_USER_EMAIL || 'joao.braga@mercadolivre.com', name: 'Dev Mode', photo: '' });
  }
  res.json({ email: req.user.email, name: req.user.name, photo: '' });
});

app.listen(PORT, () => {
  console.log(`Pipeline Backend rodando em http://localhost:${PORT}`);
  if (DEV_MODE) console.log('  MODO DEV: autenticacao desativada');
});
