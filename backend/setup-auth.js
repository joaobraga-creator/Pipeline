/**
 * setup-auth.js
 * Roda UMA VEZ para gerar tokens com escopo do Google Sheets.
 * Salva os tokens em tokens.json para uso do servidor.
 */
const http = require('http');
const { google } = require('googleapis');
const fs = require('fs');
const os = require('os');
const path = require('path');

// Lê as credenciais do gcloud ADC
const adcPath = path.join(os.homedir(), 'AppData', 'Roaming', 'gcloud', 'application_default_credentials.json');
const adcCreds = JSON.parse(fs.readFileSync(adcPath, 'utf8'));

const CLIENT_ID = adcCreds.client_id;
const CLIENT_SECRET = adcCreds.client_secret;
const REDIRECT_URI = 'http://localhost:3001/callback';
const TOKENS_FILE = path.join(__dirname, 'tokens.json');

const SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/gmail.send'
];

const oauth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI);

const authUrl = oauth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: SCOPES,
  prompt: 'consent'
});

console.log('\n======================================================');
console.log('PASSO 1: Abra este link no navegador:');
console.log('\n' + authUrl + '\n');
console.log('PASSO 2: Faça login com sua conta @mercadolivre.com');
console.log('PASSO 3: Aguarde — o servidor vai capturar o token automaticamente');
console.log('======================================================\n');

// Servidor local para capturar o callback
const server = http.createServer(async (req, res) => {
  if (!req.url.startsWith('/callback')) return;

  const code = new URL(req.url, 'http://localhost:3001').searchParams.get('code');

  if (!code) {
    res.end('Erro: codigo nao encontrado.');
    return;
  }

  try {
    const { tokens } = await oauth2Client.getToken(code);
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(tokens, null, 2));

    res.end('<h2>Autenticacao concluida!</h2><p>Pode fechar esta aba e voltar ao terminal.</p>');
    console.log('\n✅ tokens.json salvo com sucesso!');
    console.log('Agora rode: node server.js\n');
    server.close();
  } catch (e) {
    res.end('Erro ao obter token: ' + e.message);
    console.error('Erro:', e.message);
    server.close();
  }
});

server.listen(3001, () => {
  console.log('Aguardando callback em http://localhost:3001 ...\n');
});
