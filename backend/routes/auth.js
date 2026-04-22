const express = require('express');
const { authenticate } = require('../services/auth');
const router = express.Router();

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function loginPage(errorMsg) {
  return `<!DOCTYPE html>
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
    ${errorMsg ? `<p class="text-red-600 text-sm mb-4 bg-red-50 p-3 rounded-lg">${escapeHtml(errorMsg)}</p>` : ''}
    <form method="POST" action="/auth/login" class="text-left space-y-4" id="loginForm">
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">E-mail</label>
        <input type="email" name="email" required autocomplete="email" id="emailInput"
               class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Senha</label>
        <input type="password" name="password" required autocomplete="current-password"
               class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
      </div>
      <button type="submit" id="submitBtn"
              class="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2">
        <span id="btnText">Entrar</span>
        <svg id="spinner" class="hidden animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"></path>
        </svg>
      </button>
    </form>
  </div>
  <script>
    document.getElementById('loginForm').addEventListener('submit', function() {
      var btn = document.getElementById('submitBtn');
      var text = document.getElementById('btnText');
      var spinner = document.getElementById('spinner');
      btn.disabled = true;
      btn.classList.remove('bg-blue-600', 'hover:bg-blue-700');
      btn.classList.add('bg-blue-400', 'cursor-not-allowed');
      text.textContent = 'Carregando...';
      spinner.classList.remove('hidden');
    });
  </script>
</body>
</html>`;
}

router.get('/login', (req, res) => {
  res.send(loginPage(null));
});

router.post('/login', express.urlencoded({ extended: false }), async (req, res) => {
  const { email, password } = req.body;
  const user = await authenticate(email, password);
  if (!user) {
    return res.status(401).send(loginPage('E-mail ou senha incorretos.'));
  }
  req.session.regenerate((err) => {
    if (err) return res.status(500).send(loginPage('Erro interno. Tente novamente.'));
    req.session.user = user;
    res.redirect(process.env.FRONTEND_URL || '/');
  });
});

router.get('/logout', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/auth/login');
  });
});

module.exports = router;
