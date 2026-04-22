const bcrypt = require('bcrypt');

const SALT_ROUNDS = 10;
let userCache = null;

async function loadUsers() {
  if (userCache) return userCache;
  const raw = process.env.USERS || '';
  const users = {};
  for (const entry of raw.split(',')) {
    const idx = entry.indexOf(':');
    if (idx === -1) continue;
    const email = entry.slice(0, idx).trim().toLowerCase();
    const pass = entry.slice(idx + 1).trim();
    if (!email || !pass) continue;
    if (pass.startsWith('$2b$') || pass.startsWith('$2a$')) {
      users[email] = pass;
    } else {
      users[email] = await bcrypt.hash(pass, SALT_ROUNDS);
    }
  }
  userCache = users;
  return users;
}

async function authenticate(email, password) {
  if (!email || !password) return null;
  const users = await loadUsers();
  const hash = users[email.toLowerCase()];
  if (!hash) return null;
  const valid = await bcrypt.compare(password, hash);
  if (!valid) return null;
  return { email: email.toLowerCase(), name: email.split('@')[0] };
}

module.exports = { authenticate };
