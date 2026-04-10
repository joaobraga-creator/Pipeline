const express = require('express');
const passport = require('passport');
const router = express.Router();

// Inicia o fluxo de login com Google
router.get('/google', passport.authenticate('google', {
  scope: ['profile', 'email']
}));

// Callback após o Google autenticar
router.get('/google/callback',
  passport.authenticate('google', { failureRedirect: '/login?error=acesso_negado' }),
  (req, res) => {
    res.redirect(process.env.FRONTEND_URL || '/');
  }
);

// Página de login simples
router.get('/login', (req, res) => {
  const error = req.query.error;
  res.send(`
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Pipeline — Login</title>
      <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100 flex items-center justify-center min-h-screen">
      <div class="bg-white p-8 rounded-xl shadow-lg max-w-sm w-full text-center">
        <img src="https://http2.mlstatic.com/frontend-assets/ui-navigation/5.18.9/mercadolibre/logo__small@2x.png"
             alt="Mercado Livre" class="h-10 mx-auto mb-6">
        <h1 class="text-2xl font-bold text-gray-800 mb-2">Pipeline</h1>
        <p class="text-gray-500 text-sm mb-6">Ferramenta de Prospecção de Places</p>
        ${error ? `<p class="text-red-600 text-sm mb-4 bg-red-50 p-3 rounded-lg">Acesso negado. Use seu e-mail corporativo.</p>` : ''}
        <a href="/auth/google"
           class="block w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors duration-200">
          Entrar com Google
        </a>
        <p class="text-xs text-gray-400 mt-4">Apenas contas do domínio autorizado</p>
      </div>
    </body>
    </html>
  `);
});

// Logout
router.get('/logout', (req, res, next) => {
  req.logout((err) => {
    if (err) return next(err);
    res.redirect('/login');
  });
});

module.exports = router;
