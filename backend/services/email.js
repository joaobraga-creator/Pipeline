/**
 * email.js — Serviço de envio de e-mail via Gmail API
 * Porta de sendLeadProspectingEmail do Code.gs
 */
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const os = require('os');

// Arquivo para controle anti-duplicidade (equivale ao PropertiesService do Apps Script)
const SENT_EMAILS_FILE = path.join(__dirname, '..', 'sent_emails.json');

function loadSentEmails() {
  try {
    if (fs.existsSync(SENT_EMAILS_FILE)) {
      return JSON.parse(fs.readFileSync(SENT_EMAILS_FILE, 'utf8'));
    }
  } catch (e) { /* ignora erro de leitura */ }
  return {};
}

function saveSentEmails(data) {
  try {
    fs.writeFileSync(SENT_EMAILS_FILE, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {
    console.error('[Email] Erro ao salvar sent_emails.json:', e.message);
  }
}

function getEmailAuth() {
  const tokensPath = path.resolve(process.cwd(), 'tokens.json');
  const adcPath = process.platform === 'win32'
    ? path.join(os.homedir(), 'AppData', 'Roaming', 'gcloud', 'application_default_credentials.json')
    : path.join(__dirname, '..', 'adc_credentials.json');

  let tokensData = null;
  if (fs.existsSync(tokensPath)) {
    tokensData = JSON.parse(fs.readFileSync(tokensPath, 'utf8'));
  }

  let clientId, clientSecret;
  if (fs.existsSync(adcPath)) {
    const adc = JSON.parse(fs.readFileSync(adcPath, 'utf8'));
    clientId = adc.client_id;
    clientSecret = adc.client_secret;
  }

  if (tokensData && clientId && clientSecret && tokensData.refresh_token) {
    const client = new google.auth.OAuth2(clientId, clientSecret);
    client.setCredentials({
      refresh_token: tokensData.refresh_token,
      access_token: tokensData.access_token,
      expiry_date: tokensData.expiry_date
    });
    client.quotaProjectId = process.env.GOOGLE_CLOUD_QUOTA_PROJECT || 'calm-mariner-105612';
    return client;
  }
  return null;
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || '').trim());
}

function formatLeadDate(raw) {
  if (!raw) return 'data não informada';
  // Aceita "dd/mm/yyyy HH:mm:ss" → retorna só dd/mm/yyyy
  const fullDt = String(raw).match(/^(\d{2}\/\d{2}\/\d{4})/);
  if (fullDt) return fullDt[1];
  // Aceita "yyyy-MM-dd"
  const iso = String(raw).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[3]}/${iso[2]}/${iso[1]}`;
  return String(raw).trim() || 'data não informada';
}

function escapeHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function buildLeadEmailHtml(nome, dataCadastro) {
  const nomeEsc = escapeHtml(nome);
  const dataEsc = escapeHtml(dataCadastro);
  const ano = new Date().getFullYear();

  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Mercado Livre – Recebemos seu cadastro</title>
</head>
<body style="margin:0;padding:0;background-color:#f0f0f0;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#f0f0f0;padding:32px 16px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" border="0"
             style="max-width:600px;width:100%;background:#ffffff;
                    border-radius:12px;overflow:hidden;
                    box-shadow:0 6px 24px rgba(0,0,0,0.12);">
        <!-- HEADER AMARELO -->
        <tr>
          <td style="background-color:#FFE600;padding:0;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td style="padding:24px 36px 0 36px;">
                  <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td>
                        <img src="https://http2.mlstatic.com/frontend-assets/ui-navigation/5.18.9/mercadolibre/logo__small@2x.png"
                             alt="Mercado Livre" width="130" style="display:block;border:0;"/>
                      </td>
                      <td align="right">
                        <span style="font-size:12px;color:#333;font-weight:700;
                                     letter-spacing:0.6px;text-transform:uppercase;">
                          Expansão &amp; Parcerias
                        </span>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
              <tr>
                <td style="padding:20px 36px 32px 36px;">
                  <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td style="width:70%;">
                        <h1 style="margin:0 0 8px 0;font-size:28px;font-weight:900;
                                   color:#333333;line-height:1.2;">
                          Places Mercado Livre
                        </h1>
                        <p style="margin:0;font-size:14px;color:#555;font-style:italic;">
                          Sua parceria com o Mercado Livre começa aqui.
                        </p>
                      </td>
                      <td align="right" style="width:30%;vertical-align:bottom;">
                        <div style="font-size:58px;line-height:1;">🚀</div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <!-- FAIXA TÍTULO ESCURO -->
        <tr>
          <td style="background-color:#333333;padding:14px 36px;">
            <p style="margin:0;font-size:14px;font-weight:700;color:#FFE600;
                       text-transform:uppercase;letter-spacing:0.8px;">
              Olá, ${nomeEsc}! Tudo bem?
            </p>
          </td>
        </tr>
        <!-- CORPO PRINCIPAL -->
        <tr>
          <td style="padding:32px 36px 0 36px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0"
                   style="background-color:#FFFDE7;border-left:5px solid #FFE600;
                          border-radius:6px;margin-bottom:24px;">
              <tr>
                <td style="padding:16px 20px;">
                  <p style="margin:0;font-size:11px;color:#999;font-weight:700;
                             text-transform:uppercase;letter-spacing:0.7px;">
                    Cadastro identificado em
                  </p>
                  <p style="margin:6px 0 0 0;font-size:22px;font-weight:900;color:#333;">
                    📅 ${dataEsc}
                  </p>
                </td>
              </tr>
            </table>
            <p style="margin:0 0 16px 0;font-size:15px;color:#333;line-height:1.8;">
              Identificamos o cadastro de <strong>${nomeEsc}</strong> em nosso sistema
              e queremos agradecer pelo interesse em fazer parte da rede de parceiros do
              <strong>Mercado Livre</strong>.
            </p>
            <p style="margin:0 0 16px 0;font-size:15px;color:#333;line-height:1.8;">
              Em breve, um(a) de nossos(as) <strong>consultores(as) especializados(as)</strong>
              entrará em contato para conhecer melhor sua operação e apresentar os próximos
              passos da nossa parceria.
            </p>
            <p style="margin:0 0 32px 0;font-size:15px;color:#333;line-height:1.8;">
              Enquanto isso, se quiser adiantar alguma informação ou tirar dúvidas,
              basta responder a este e-mail. Estamos aqui para ajudar! 😊
            </p>
          </td>
        </tr>
        <!-- PRÓXIMOS PASSOS -->
        <tr>
          <td style="padding:0 36px 32px 36px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0"
                   style="background:#f8f8f8;border-radius:10px;overflow:hidden;">
              <tr>
                <td style="background:#333;padding:13px 20px;">
                  <p style="margin:0;font-size:12px;font-weight:700;color:#FFE600;
                             text-transform:uppercase;letter-spacing:0.8px;">
                    Próximos passos
                  </p>
                </td>
              </tr>
              <tr>
                <td style="padding:20px 20px 16px 20px;">
                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:14px;">
                    <tr>
                      <td width="32" style="vertical-align:top;">
                        <div style="background:#FFE600;border-radius:50%;width:26px;height:26px;
                                    text-align:center;line-height:26px;font-size:12px;
                                    font-weight:900;color:#333;">1</div>
                      </td>
                      <td style="padding-left:12px;vertical-align:top;">
                        <p style="margin:0;font-size:14px;color:#333;line-height:1.6;">
                          <strong>Análise do perfil</strong><br/>
                          <span style="color:#666;">Nosso time avalia seu perfil e operação logística.</span>
                        </p>
                      </td>
                    </tr>
                  </table>
                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:14px;">
                    <tr>
                      <td width="32" style="vertical-align:top;">
                        <div style="background:#FFE600;border-radius:50%;width:26px;height:26px;
                                    text-align:center;line-height:26px;font-size:12px;
                                    font-weight:900;color:#333;">2</div>
                      </td>
                      <td style="padding-left:12px;vertical-align:top;">
                        <p style="margin:0;font-size:14px;color:#333;line-height:1.6;">
                          <strong>Contato do(a) consultor(a)</strong><br/>
                          <span style="color:#666;">Um(a) especialista Mercado Livre entrará em contato brevemente.</span>
                        </p>
                      </td>
                    </tr>
                  </table>
                  <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                      <td width="32" style="vertical-align:top;">
                        <div style="background:#FFE600;border-radius:50%;width:26px;height:26px;
                                    text-align:center;line-height:26px;font-size:12px;
                                    font-weight:900;color:#333;">3</div>
                      </td>
                      <td style="padding-left:12px;vertical-align:top;">
                        <p style="margin:0;font-size:14px;color:#333;line-height:1.6;">
                          <strong>Apresentação da proposta</strong><br/>
                          <span style="color:#666;">Apresentamos os benefícios, condições e próximos passos.</span>
                        </p>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <!-- RODAPÉ ESCURO -->
        <tr>
          <td style="background:#333;padding:26px 36px;border-radius:0 0 12px 12px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td>
                  <img src="https://http2.mlstatic.com/frontend-assets/ui-navigation/5.18.9/mercadolibre/logo__small@2x.png"
                       alt="Mercado Livre" width="90"
                       style="display:block;border:0;margin-bottom:10px;opacity:0.80;"/>
                  <p style="margin:0 0 4px 0;font-size:12px;font-weight:700;color:#FFE600;">
                    Time Mercado Livre – Expansão / Parcerias
                  </p>
                  <p style="margin:0;font-size:11px;color:#aaa;line-height:1.6;">
                    Este e-mail foi enviado automaticamente pela nossa plataforma de prospecção.<br/>
                    Se não reconhece esta comunicação, por favor ignore esta mensagem.
                  </p>
                </td>
                <td align="right" style="vertical-align:bottom;">
                  <p style="margin:0;font-size:11px;color:#666;">
                    © ${ano} Mercado Livre
                  </p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;
}

/**
 * Envia e-mail de prospecção via Gmail API.
 * Equivale a sendLeadProspectingEmail do Code.gs.
 *
 * @param {object} lead
 *   lead.leadId        {string}  ID do lead (usado para anti-duplicidade)
 *   lead.nome          {string}  Nome do estabelecimento
 *   lead.email         {string}  E-mail de destino
 *   lead.dataCadastro  {string}  Data de cadastro
 *   lead.tipoLead      {string}  "Quente" | "Frio" | etc.
 *   lead.statusContato {string}  Status atual
 *   lead.consultorEmail {string} E-mail do consultor (CC)
 * @returns {{ emailSent: boolean, reason: string }}
 */
async function sendLeadProspectingEmail(lead) {
  const logPrefix = `[EMAIL LEAD] leadId=${lead.leadId}`;

  // 1. Validação: tipo de lead deve ser "Quente"
  const tipoNorm = String(lead.tipoLead || '').toLowerCase().trim();
  if (tipoNorm !== 'quente' && tipoNorm !== 'hot') {
    const reason = `Tipo de lead não é "Quente" (valor: "${lead.tipoLead}"). E-mail não enviado.`;
    console.log(`${logPrefix} | SKIP | ${reason}`);
    return { emailSent: false, reason };
  }

  // 2. Validação: status deve ser "Em prospecção"
  const statusNorm = String(lead.statusContato || '').toLowerCase().trim();
  if (statusNorm !== 'em prospecção' && statusNorm !== 'em prospeccao') {
    const reason = `Status não é "Em prospecção" (valor: "${lead.statusContato}"). E-mail não enviado.`;
    console.log(`${logPrefix} | SKIP | ${reason}`);
    return { emailSent: false, reason };
  }

  // 3. Anti-duplicidade
  const propKey = 'EMAIL_SENT_' + String(lead.leadId || '').replace(/[^a-zA-Z0-9_]/g, '_');
  const sentEmails = loadSentEmails();
  if (sentEmails[propKey]) {
    const reason = `E-mail já enviado anteriormente em ${sentEmails[propKey]}. Duplicidade bloqueada.`;
    console.log(`${logPrefix} | DUPLICATE | ${reason}`);
    return { emailSent: false, reason };
  }

  // 4. Validação: e-mail de destino
  const emailDest = String(lead.email || '').trim();
  if (!emailDest || !isValidEmail(emailDest)) {
    const reason = `E-mail inválido ou ausente: "${emailDest}". E-mail não enviado.`;
    console.log(`${logPrefix} | INVALID_EMAIL | ${reason}`);
    return { emailSent: false, reason };
  }

  // 5. Construção do e-mail
  const nomeFormatado = String(lead.nome || 'parceiro(a)').trim();
  const dataCadastroFmt = formatLeadDate(lead.dataCadastro);
  const subject = `Mercado Livre | Recebemos seu cadastro, ${nomeFormatado}`;
  const htmlBody = buildLeadEmailHtml(nomeFormatado, dataCadastroFmt);

  // 6. Envio via Gmail API
  const authClient = getEmailAuth();
  if (!authClient) {
    const reason = 'Credenciais OAuth não encontradas para Gmail. Execute setup-auth.js com escopo gmail.send.';
    console.error(`${logPrefix} | NO_AUTH | ${reason}`);
    return { emailSent: false, reason };
  }

  const ccEmail = String(lead.consultorEmail || '').trim();
  const fromName = 'Mercado Livre – Expansão e Parcerias';

  // Monta mensagem RFC 2822
  // IMPORTANTE: usar null (não '') para linhas opcionais e filtrar por !== null
  // para preservar a linha em branco obrigatória entre cabeçalhos e corpo.
  const bodyBase64 = Buffer.from(htmlBody).toString('base64')
    .match(/.{1,76}/g).join('\r\n'); // RFC 2822: max 76 chars por linha
  const headers = [
    `From: "${fromName}" <me>`,
    `To: ${emailDest}`,
    isValidEmail(ccEmail) ? `Cc: ${ccEmail}` : null,
    `Subject: =?UTF-8?B?${Buffer.from(subject).toString('base64')}?=`,
    'MIME-Version: 1.0',
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: base64',
    '',           // linha em branco OBRIGATÓRIA — separa cabeçalhos do corpo
    bodyBase64
  ].filter(line => line !== null).join('\r\n');

  const raw = Buffer.from(headers).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

  try {
    const gmail = google.gmail({ version: 'v1', auth: authClient });
    await gmail.users.messages.send({
      userId: 'me',
      requestBody: { raw }
    });

    const sentAt = new Date().toISOString();
    sentEmails[propKey] = sentAt;
    saveSentEmails(sentEmails);

    const msg = `E-mail enviado com sucesso para ${emailDest}${isValidEmail(ccEmail) ? ` (cc: ${ccEmail})` : ''}.`;
    console.log(`${logPrefix} | SUCCESS | ${msg}`);
    return { emailSent: true, reason: msg };

  } catch (sendErr) {
    const reason = `Falha no envio via Gmail API: ${sendErr.message}`;
    console.error(`${logPrefix} | ERROR | ${reason}`);
    return { emailSent: false, reason };
  }
}

module.exports = { sendLeadProspectingEmail };
