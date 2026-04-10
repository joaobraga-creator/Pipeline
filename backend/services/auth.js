const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;

passport.use(new GoogleStrategy({
  clientID: process.env.GOOGLE_CLIENT_ID,
  clientSecret: process.env.GOOGLE_CLIENT_SECRET,
  callbackURL: process.env.GOOGLE_CALLBACK_URL || 'http://localhost:3000/auth/google/callback'
}, (accessToken, refreshToken, profile, done) => {
  const email = profile.emails && profile.emails[0] ? profile.emails[0].value : '';
  const allowedDomain = process.env.ALLOWED_DOMAIN;

  // Verifica domínio se configurado
  if (allowedDomain && !email.endsWith('@' + allowedDomain)) {
    return done(null, false, { message: `Acesso restrito ao domínio @${allowedDomain}` });
  }

  const user = {
    id: profile.id,
    email: email,
    name: profile.displayName,
    photo: profile.photos && profile.photos[0] ? profile.photos[0].value : null
  };
  return done(null, user);
}));

passport.serializeUser((user, done) => done(null, user));
passport.deserializeUser((user, done) => done(null, user));
