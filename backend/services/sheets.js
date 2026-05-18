/**
 * sheets.js — Serviço de acesso ao Google Sheets
 * Porta fiel do Code.gs para Node.js usando googleapis
 */
const { google } = require('googleapis');

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const EXTERNAL_PROSPECCAO_SHEET_ID = process.env.EXTERNAL_PROSPECCAO_SHEET_ID;
const EXTERNAL_PROSPECCAO_SHEET_NAME = process.env.EXTERNAL_PROSPECCAO_SHEET_NAME || 'Prospecção de Coordenadas';

const NOME_ABA_PIPELINE = 'Pipeline';
const NOME_ABA_MINHAS_PROPOSTAS = 'Minhas Propostas';
const NOME_ABA_ACEITES = 'Aceites';
const NOME_ABA_ADM = 'ADM';

const COLUNAS_PIPELINE = [
  'Geo_Id', 'Lat_geo', 'Long_geo', 'Tipo', 'Cidade', 'Estado',
  'Servico', 'Regional', 'Consultor', 'Status_prospeccao',
  'Place_id', 'Place_name', 'Telefone_Place', 'SVC', 'Data_Solicitacao',
  'Data_MP', 'Data_Treinamento', 'Data_Logistics', 'Observacoes', 'Motivo_recusa',
  'Dificuldade', 'Contatos_Feitos', 'Data_Ultimo_Contato',
  'SHP_PROSP_REG_RESP_NAME', 'Go_Live', 'Primeiro_Contato', 'Aceite',
  'Data_Status_Em_Prospeccao', 'Data_Status_Analisando', 'Data_Status_Aceitou',
  'Data_Status_Ativo', 'Data_Status_Recusado', 'Data_Status_Desistiu', 'LEAD_ID'
];

const STATUS = {
  PROSPECCAO: 'Em prospecção',
  ANALISANDO: 'Analisando',
  ACEITOU: 'Aceitou',
  ATIVO: 'Ativo',
  RECUSADO: 'Recusado',
  DESISTIU: 'Desistiu',
};

// ─── Cache em memória simples ──────────────────────────────────────────────────
const _cache = {};
function cacheGet(key) {
  const entry = _cache[key];
  if (entry && Date.now() < entry.expires) return entry.value;
  return null;
}
function cacheSet(key, value, ttlSeconds) {
  _cache[key] = { value, expires: Date.now() + ttlSeconds * 1000 };
}
function cacheClear(key) {
  delete _cache[key];
}

// ─── Auth Google ───────────────────────────────────────────────────────────────
function getAuth() {
  const fs = require('fs');
  const path = require('path');
  const os = require('os');

  const tokensPath = path.resolve(process.cwd(), 'tokens.json');
  console.log('[Auth] Procurando tokens.json em:', tokensPath, '| existe:', fs.existsSync(tokensPath));
  const adcPath = process.platform === 'win32'
    ? path.join(os.homedir(), 'AppData', 'Roaming', 'gcloud', 'application_default_credentials.json')
    : path.join(__dirname, '..', 'adc_credentials.json');

  let credsSource = null;
  if (fs.existsSync(tokensPath)) {
    credsSource = { type: 'tokens', data: JSON.parse(fs.readFileSync(tokensPath, 'utf8')) };
  } else if (fs.existsSync(adcPath)) {
    credsSource = { type: 'adc', data: JSON.parse(fs.readFileSync(adcPath, 'utf8')) };
  }

  // Lê client_id/secret sempre do ADC (tokens.json não os contém)
  let clientId, clientSecret;
  if (fs.existsSync(adcPath)) {
    const adc = JSON.parse(fs.readFileSync(adcPath, 'utf8'));
    clientId = adc.client_id;
    clientSecret = adc.client_secret;
  }

  if (credsSource && clientId && clientSecret) {
    const d = credsSource.data;
    const refreshToken = d.refresh_token;

    if (refreshToken) {
      const client = new google.auth.OAuth2(clientId, clientSecret);
      client.setCredentials({
        refresh_token: refreshToken,
        access_token: d.access_token,
        expiry_date: d.expiry_date,
        scope: d.scope
      });
      client.quotaProjectId = process.env.GOOGLE_CLOUD_QUOTA_PROJECT || 'calm-mariner-105612';
      console.log('[Auth] Usando tokens.json com scopes:', d.scope);
      return { getClient: async () => client };
    }
  }

  return new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
}

async function getSheetsClient() {
  const auth = await getAuth().getClient();
  return google.sheets({ version: 'v4', auth });
}

// ─── Helpers ───────────────────────────────────────────────────────────────────
function formatDateTimeBR(date) {
  const parts = new Intl.DateTimeFormat('pt-BR', {
    timeZone: 'America/Sao_Paulo',
    day: '2-digit', month: '2-digit', year: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).formatToParts(date);
  const p = {};
  parts.forEach(({ type, value }) => { p[type] = value; });
  return `${p.day}/${p.month}/${p.year} ${p.hour}:${p.minute}:${p.second}`;
}

function normalizeStatus(s) {
  return String(s || '').toLowerCase().trim();
}

function formatStatusForSaving(status) {
  if (!status || String(status).trim() === '') return '';
  const s = String(status).toLowerCase().trim();
  const map = {
    'em prospecção': 'Em prospecção', 'em prospeccao': 'Em prospecção',
    'analisando': 'Analisando', 'aceitou': 'Aceitou', 'ativo': 'Ativo',
    'recusado': 'Recusado', 'desistiu': 'Desistiu'
  };
  return map[s] || String(status).trim();
}

function cleanAndFormatString(value) {
  if (!value) return '';
  return String(value).replace(/-/g, '').replace(/\s+/g, '').trim();
}

function normalizeHeaderKey(v) {
  return String(v || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
}

function getColIndex(headers, colName) {
  const idx = headers.indexOf(colName);
  return idx;
}

function getColIndexLoose(headers, nameOrList) {
  const wanted = Array.isArray(nameOrList) ? nameOrList : [nameOrList];
  const wantedKeys = wanted.map(normalizeHeaderKey);
  const map = {};
  headers.forEach((h, i) => {
    const k = normalizeHeaderKey(h);
    if (k && map[k] === undefined) map[k] = i;
  });
  for (const wk of wantedKeys) {
    if (map[wk] !== undefined) return map[wk];
  }
  return -1;
}

function getColumnIndices(headers) {
  const idx = {};
  COLUNAS_PIPELINE.forEach(col => { idx[col] = headers.indexOf(col); });
  return idx;
}

function formatCoordinate(value) {
  if (value === null || value === undefined || value === '') return '';
  const n = parseFloat(value);
  if (isNaN(n)) return '';
  return n.toFixed(6);
}

function formatCoordinateAsString(value) {
  if (value === null || value === undefined || value === '') return '';
  const n = parseFloat(String(value).replace(',', '.'));
  if (isNaN(n)) return String(value).trim();
  return n.toFixed(6);
}

function formatSheetDate(dateValue) {
  if (!dateValue) return '';
  if (dateValue instanceof Date) {
    const y = dateValue.getFullYear();
    const m = String(dateValue.getMonth() + 1).padStart(2, '0');
    const d = String(dateValue.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  if (typeof dateValue === 'string') {
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateValue)) return dateValue;
    // "2026-04-22 19:18:40" or "2026-04-22T19:18:40"
    const iso = dateValue.match(/^(\d{4}-\d{2}-\d{2})[T ]/);
    if (iso) return iso[1];
    const slash = dateValue.match(/(\d{2})\/(\d{2})\/(\d{4})/);
    if (slash) return `${slash[3]}-${slash[2]}-${slash[1]}`;
  }
  return '';
}

function normalizeSlaEmoji(v) {
  if (!v) return '';
  return String(v).replace(/\uFE0F/g, '').replace(/\u200D/g, '').replace(/\u00A0/g, '').trim();
}

function toKeyString(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'number' && isFinite(v)) return String(Math.floor(v) === v ? v : v);
  let s = String(v).trim();
  if (/^\d+\.0+$/.test(s)) s = s.replace(/\.0+$/, '');
  return s;
}

function getStatusSLA(aceiteDate, goLiveDate) {
  if (!aceiteDate) return '';
  const today = new Date();
  const aceite = new Date(aceiteDate);
  const goLive = goLiveDate ? new Date(goLiveDate) : null;
  if (goLive && goLive <= today) return '✅';
  const days = Math.floor((today.getTime() - aceite.getTime()) / (1000 * 60 * 60 * 24));
  if (days <= 7) return '🟢';
  if (days <= 15) return '🟡';
  return '🔴';
}

// ─── Lê aba completa ───────────────────────────────────────────────────────────
async function readSheet(spreadsheetId, sheetName) {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: sheetName
  });
  return res.data.values || [];
}

// ─── Escreve em uma célula específica ─────────────────────────────────────────
async function writeCell(spreadsheetId, sheetName, rowIndex1, colIndex1, value) {
  const sheets = await getSheetsClient();
  const colLetter = colToLetter(colIndex1);
  const range = `${sheetName}!${colLetter}${rowIndex1}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[value === null || value === undefined ? '' : value]] }
  });
}

// ─── Escreve uma linha inteira ────────────────────────────────────────────────
async function writeRow(spreadsheetId, sheetName, rowIndex1, rowData) {
  const sheets = await getSheetsClient();
  const range = `${sheetName}!A${rowIndex1}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [rowData] }
  });
}

// ─── Append linha ─────────────────────────────────────────────────────────────
async function appendRow(spreadsheetId, sheetName, rowData) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [rowData] }
  });
}

// ─── Deleta linha ─────────────────────────────────────────────────────────────
async function deleteRowByIndex(spreadsheetId, sheetName, rowIndex1) {
  const sheets = await getSheetsClient();
  // Primeiro precisamos do sheetId
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const sheet = meta.data.sheets.find(s => s.properties.title === sheetName);
  if (!sheet) throw new Error(`Aba "${sheetName}" não encontrada.`);
  const sheetId = sheet.properties.sheetId;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{
        deleteDimension: {
          range: {
            sheetId,
            dimension: 'ROWS',
            startIndex: rowIndex1 - 1,  // 0-based
            endIndex: rowIndex1          // exclusive
          }
        }
      }]
    }
  });
}

function colToLetter(colNum) {
  let letter = '';
  while (colNum > 0) {
    const rem = (colNum - 1) % 26;
    letter = String.fromCharCode(65 + rem) + letter;
    colNum = Math.floor((colNum - 1) / 26);
  }
  return letter;
}

function colLetterToNumber(letters) {
  const s = String(letters || '').toUpperCase().trim();
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    n = n * 26 + (s.charCodeAt(i) - 64);
  }
  return n;
}

// ─── Admin check ──────────────────────────────────────────────────────────────
async function isUserAdmin(userEmail) {
  try {
    const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_ADM);
    const emails = rows.flat().filter(Boolean);
    return emails.some(e => normalizeStatus(e) === normalizeStatus(userEmail));
  } catch (e) {
    return false;
  }
}

// ─── Extras da planilha externa ───────────────────────────────────────────────
async function getProspeccaoExtrasMap() {
  const cached = cacheGet('ext_prospeccao_extras_v2');
  if (cached) return cached;

  const map = {};
  try {
    const rows = await readSheet(EXTERNAL_PROSPECCAO_SHEET_ID, EXTERNAL_PROSPECCAO_SHEET_NAME);
    if (rows.length < 2) { cacheSet('ext_prospeccao_extras_v2', map, 300); return map; }

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      // Índice 1 (B) = Geo ID Coordenada
      const k = toKeyString(row[1]);
      if (!k) continue;
      map[k] = {
        volumetria: String(row[25] || '').trim(),
        tipopagamento: String(row[26] || '').trim(),
        observacaoprospeccao: String(row[27] || '').trim()
      };
      const k2 = k.replace(/\s+/g, '');
      if (k2 && k2 !== k) map[k2] = map[k];
    }
  } catch (e) { /* falha silenciosa */ }

  cacheSet('ext_prospeccao_extras_v2', map, 300);
  return map;
}

function mergeProspeccaoExtras(extras, lists) {
  lists.forEach(arr => {
    if (!arr) return;
    arr.forEach(item => {
      const k = toKeyString(item.geoid || item.Geo_Id || item.GEO_ID || '');
      let ext = extras[k];
      if (!ext && k) ext = extras[k.replace(/\s+/g, '')];
      item.volumetria = ext ? ext.volumetria : '';
      item.tipopagamento = ext ? ext.tipopagamento : '';
      item.observacaoprospeccao = ext ? ext.observacaoprospeccao : '';
    });
  });
}

// ─── Mapa Status ID da pipeline ───────────────────────────────────────────────
async function getPipelineStatusIdMap() {
  const cached = cacheGet('PIPELINE_STATUSID_v1');
  if (cached) return cached;

  const map = {};
  try {
    const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE);
    if (rows.length < 2) { cacheSet('PIPELINE_STATUSID_v1', map, 300); return map; }
    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const k = toKeyString(row[0]); // col A = Geo_Id
      if (!k) continue;
      map[k] = String(row[40] || '').trim(); // col AO = index 40
      const k2 = k.replace(/\s+/g, '');
      if (k2 && k2 !== k) map[k2] = map[k];
    }
  } catch (e) { /* silencioso */ }

  cacheSet('PIPELINE_STATUSID_v1', map, 300);
  return map;
}

function mergePipelineStatusId(statusMap, lists) {
  lists.forEach(arr => {
    if (!arr) return;
    arr.forEach(item => {
      const k = toKeyString(item.geoid || item.Geo_Id || '');
      let v = statusMap[k];
      if (!v && k) v = statusMap[k.replace(/\s+/g, '')];
      item.status_id = v || '';
    });
  });
}

// ─── FUNÇÃO PRINCIPAL: getAppData ─────────────────────────────────────────────
async function getAppData(userEmail) {
  const isAdmin = await isUserAdmin(userEmail);

  const [pipelineRows, propostasRows, aceitesRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS),
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES)
  ]);

  // ── Pipeline ────────────────────────────────────────────────────────────────
  const pHeaders = pipelineRows[0] || [];
  const pRows = pipelineRows.slice(1).sort((a, b) => {
    const iDUC = getColIndex(pHeaders, 'Data_Ultimo_Contato');
    const da = iDUC !== -1 && a[iDUC] ? new Date(a[iDUC]) : new Date(0);
    const db = iDUC !== -1 && b[iDUC] ? new Date(b[iDUC]) : new Date(0);
    return db - da;
  });

  // Mapa Status SLA do Pipeline
  const pipelineStatusSlaMap = {};
  const cGeoP = getColIndex(pHeaders, 'Geo_Id');
  const cAceP = getColIndex(pHeaders, 'Aceite');
  const cGLP  = getColIndex(pHeaders, 'Go_Live');
  const cSlaP = getColIndexLoose(pHeaders, ['Status_SLA', 'STATUS_SLA']);

  if (cGeoP !== -1) {
    for (const row of pRows) {
      const k = toKeyString(row[cGeoP]);
      if (!k) continue;
      let v = cSlaP !== -1 ? normalizeSlaEmoji(String(row[cSlaP] || '').trim()) : '';
      if (!v) {
        const aceiteFmt = cAceP !== -1 ? formatSheetDate(row[cAceP]) : '';
        const goLiveFmt = cGLP !== -1 ? formatSheetDate(row[cGLP]) : '';
        v = getStatusSLA(aceiteFmt, goLiveFmt);
      }
      pipelineStatusSlaMap[k] = v;
      const k2 = k.replace(/\s+/g, '');
      if (k2 && k2 !== k) pipelineStatusSlaMap[k2] = v;
    }
  }

  function getSlaFromPipeline(geoIdValue) {
    const k = toKeyString(geoIdValue);
    if (!k) return '';
    return pipelineStatusSlaMap[k] || pipelineStatusSlaMap[k.replace(/\s+/g, '')] || '';
  }

  // Índices Pipeline
  const cGeo  = getColIndex(pHeaders, 'Geo_Id');
  const cCons = getColIndex(pHeaders, 'Consultor');
  const cStat = getColIndex(pHeaders, 'Status_prospeccao');
  const cLat  = getColIndex(pHeaders, 'Lat_geo');
  const cLon  = getColIndex(pHeaders, 'Long_geo');
  const cResp = getColIndex(pHeaders, 'SHP_PROSP_REG_RESP_NAME');
  const cDSol = getColIndex(pHeaders, 'Data_Solicitacao');
  const cGL   = getColIndex(pHeaders, 'Go_Live');
  const cTipo = getColIndex(pHeaders, 'Tipo');
  const cCid  = getColIndex(pHeaders, 'Cidade');
  const cEst  = getColIndex(pHeaders, 'Estado');
  const cServ = getColIndex(pHeaders, 'Servico');
  const cReg  = getColIndex(pHeaders, 'Regional');
  const cPlId = getColIndex(pHeaders, 'Place_id');
  const cPlNm = getColIndex(pHeaders, 'Place_name');
  const cTel  = getColIndex(pHeaders, 'Telefone_Place');
  const cMP   = getColIndex(pHeaders, 'Data_MP');
  const cTr   = getColIndex(pHeaders, 'Data_Treinamento');
  const cLg   = getColIndex(pHeaders, 'Data_Logistics');
  const cObs  = getColIndex(pHeaders, 'Observacoes');
  const cMot  = getColIndex(pHeaders, 'Motivo_recusa');
  const cDif  = getColIndex(pHeaders, 'Dificuldade');
  const cCtt  = getColIndex(pHeaders, 'Contatos_Feitos');
  const cDUC  = getColIndex(pHeaders, 'Data_Ultimo_Contato');
  const cSVC  = getColIndex(pHeaders, 'SVC');
  const cAce  = getColIndex(pHeaders, 'Aceite');
  const cSLA  = getColIndexLoose(pHeaders, ['Status_SLA', 'STATUS_SLA']);
  const cLeadP= getColIndex(pHeaders, 'LEAD_ID');

  const minhasProspeccoes = [];
  const allGeoIds = [];
  const prospectGeoIdsWithCoords = [];

  for (const row of pRows) {
    const geoId = String(row[cGeo] || '').trim();
    const status = normalizeStatus(row[cStat] || '');
    const rowEmail = String(row[cCons] || '').trim();

    if (isAdmin || normalizeStatus(rowEmail) === normalizeStatus(userEmail)) {
      const data = {
        geoid: geoId,
        latgeo: formatCoordinate(row[cLat]),
        longgeo: formatCoordinate(row[cLon]),
        tipo: cTipo !== -1 ? (row[cTipo] || '') : '',
        cidade: row[cCid] || '',
        estado: row[cEst] || '',
        servico: cServ !== -1 ? (row[cServ] || '') : '',
        regional: cReg !== -1 ? (row[cReg] || '') : '',
        consultor: rowEmail,
        statusprospeccao: status,
        placeid: String(row[cPlId] || ''),
        placename: row[cPlNm] || '',
        telefoneplace: row[cTel] || '',
        datamp: formatSheetDate(row[cMP]),
        datatreinamento: formatSheetDate(row[cTr]),
        datalogistics: formatSheetDate(row[cLg]),
        observacoes: row[cObs] || '',
        motivorecusa: row[cMot] || '',
        dificuldade: row[cDif] || '',
        contatosfeitos: (row[cCtt] === '' || row[cCtt] === null || row[cCtt] === undefined) ? 0 : row[cCtt],
        dataultimocontato: formatSheetDate(row[cDUC]),
        shpprospregrespname: cResp !== -1 ? (row[cResp] || '') : '',
        svc: cSVC !== -1 ? (row[cSVC] || '') : '',
        datasolicitacao: formatSheetDate(row[cDSol]),
        golive: cGL !== -1 ? formatSheetDate(row[cGL]) : '',
        statussla: normalizeSlaEmoji(cSLA !== -1 ? String(row[cSLA] || '').trim() : getStatusSLA(row[cAce], row[cGL])),
        leadid: cLeadP !== -1 ? (row[cLeadP] || '') : ''
      };

      if (status === normalizeStatus(STATUS.PROSPECCAO) || status === normalizeStatus(STATUS.ANALISANDO)) {
        minhasProspeccoes.push(data);
      }
      if (geoId) {
        allGeoIds.push(geoId);
        if (status === normalizeStatus(STATUS.PROSPECCAO) && cLat !== -1 && cLon !== -1 && row[cLat] && row[cLon]) {
          prospectGeoIdsWithCoords.push({
            geoId,
            latitude: formatCoordinate(row[cLat]),
            longitude: formatCoordinate(row[cLon]),
            servico: cServ !== -1 ? (row[cServ] || '') : '',
            svc: cSVC !== -1 ? (row[cSVC] || '') : ''
          });
        }
      }
    }
  }

  // ── Minhas Propostas ────────────────────────────────────────────────────────
  const mpHeaders = propostasRows[0] || [];
  const mpRows = propostasRows.slice(1)
    .filter(row => row.some(c => c !== '' && c !== null && c !== undefined))
    .sort((a, b) => {
      const iDUC = getColIndex(mpHeaders, 'Data_Ultimo_Contato');
      const da = iDUC !== -1 && a[iDUC] ? new Date(a[iDUC]) : new Date(0);
      const db = iDUC !== -1 && b[iDUC] ? new Date(b[iDUC]) : new Date(0);
      return db - da;
    });

  const minhasPropostas = [];
  if (mpRows.length) {
    const c = {};
    ['Geo_Id','Consultor','Status_prospeccao','Place_id','Place_name','Telefone_Place',
     'Data_MP','Data_Treinamento','Data_Logistics','Observacoes','Motivo_recusa',
     'Data_Ultimo_Contato','Lat_geo','Long_geo','SHP_PROSP_REG_RESP_NAME','SVC',
     'Data_Solicitacao','Go_Live','Tipo','Cidade','Estado','Servico','LEAD_ID'].forEach(col => {
      c[col] = getColIndex(mpHeaders, col);
    });
    const cSla = getColIndexLoose(mpHeaders, 'Status_SLA');

    for (const row of mpRows) {
      const geoId = String(row[c['Geo_Id']] || '');
      const rowEmail = String(row[c['Consultor']] || '');
      if (isAdmin || normalizeStatus(rowEmail) === normalizeStatus(userEmail)) {
        minhasPropostas.push({
          geoid: geoId,
          latgeo: formatCoordinate(row[c['Lat_geo']]),
          longgeo: formatCoordinate(row[c['Long_geo']]),
          tipo: c['Tipo'] !== -1 ? (row[c['Tipo']] || '') : '',
          cidade: c['Cidade'] !== -1 ? (row[c['Cidade']] || '') : '',
          estado: c['Estado'] !== -1 ? (row[c['Estado']] || '') : '',
          servico: c['Servico'] !== -1 ? (row[c['Servico']] || '') : '',
          regional: '',
          consultor: rowEmail,
          statusprospeccao: String(row[c['Status_prospeccao']] || ''),
          placeid: String(row[c['Place_id']] || ''),
          placename: row[c['Place_name']] || '',
          telefoneplace: row[c['Telefone_Place']] || '',
          datamp: formatSheetDate(row[c['Data_MP']]),
          datatreinamento: formatSheetDate(row[c['Data_Treinamento']]),
          datalogistics: formatSheetDate(row[c['Data_Logistics']]),
          motivorecusa: c['Motivo_recusa'] !== -1 ? (row[c['Motivo_recusa']] || '') : '',
          dataultimocontato: formatSheetDate(row[c['Data_Ultimo_Contato']]),
          observacoes: c['Observacoes'] !== -1 ? (row[c['Observacoes']] || '') : '',
          shpprospregrespname: c['SHP_PROSP_REG_RESP_NAME'] !== -1 ? (row[c['SHP_PROSP_REG_RESP_NAME']] || '') : '',
          svc: c['SVC'] !== -1 ? (row[c['SVC']] || '') : '',
          datasolicitacao: formatSheetDate(row[c['Data_Solicitacao']]),
          golive: c['Go_Live'] !== -1 ? formatSheetDate(row[c['Go_Live']]) : '',
          statussla: normalizeSlaEmoji((cSla !== -1 ? String(row[cSla] || '').trim() : getSlaFromPipeline(row[c['Geo_Id']])) || getSlaFromPipeline(row[c['Geo_Id']])),
          leadid: c['LEAD_ID'] !== -1 ? (row[c['LEAD_ID']] || '') : ''
        });
      }
    }
  }

  // ── Aceites ─────────────────────────────────────────────────────────────────
  const aHeaders = aceitesRows[0] || [];
  const aRows = aceitesRows.slice(1)
    .filter(row => row.some(c => c !== '' && c !== null && c !== undefined))
    .sort((a, b) => {
      const iAce = getColIndex(aHeaders, 'Aceite');
      const da = iAce !== -1 && a[iAce] ? new Date(a[iAce]) : new Date(0);
      const db = iAce !== -1 && b[iAce] ? new Date(b[iAce]) : new Date(0);
      return db - da;
    });

  const propostasEmAndamento = [];
  if (aRows.length) {
    const c = {};
    ['Geo_Id','Consultor','Status_prospeccao','Place_id','Place_name','Telefone_Place',
     'Data_MP','Data_Treinamento','Data_Logistics','Observacoes','Data_Ultimo_Contato',
     'SHP_PROSP_REG_RESP_NAME','Lat_geo','Long_geo','SVC','Tipo','Data_Solicitacao',
     'Go_Live','Servico','Aceite','LEAD_ID'].forEach(col => { c[col] = getColIndex(aHeaders, col); });

    for (const row of aRows) {
      const geoId = String(row[c['Geo_Id']] || '');
      const status = normalizeStatus(row[c['Status_prospeccao']] || '');
      const rowEmail = String(row[c['Consultor']] || '').trim();

      if (isAdmin || normalizeStatus(rowEmail) === normalizeStatus(userEmail)) {
        if (status === normalizeStatus(STATUS.ACEITOU) || status === normalizeStatus(STATUS.ATIVO)) {
          propostasEmAndamento.push({
            statussla: normalizeSlaEmoji(getSlaFromPipeline(row[c['Geo_Id']]) || getStatusSLA(row[c['Aceite']], row[c['Go_Live']])),
            geoid: geoId,
            latgeo: formatCoordinate(row[c['Lat_geo']]),
            longgeo: formatCoordinate(row[c['Long_geo']]),
            statusprospeccao: status,
            placeid: String(row[c['Place_id']] || ''),
            placename: row[c['Place_name']] || '',
            telefoneplace: row[c['Telefone_Place']] || '',
            datamp: formatSheetDate(row[c['Data_MP']]),
            datatreinamento: formatSheetDate(row[c['Data_Treinamento']]),
            datalogistics: formatSheetDate(row[c['Data_Logistics']]),
            consultor: rowEmail,
            dataultimocontato: formatSheetDate(row[c['Data_Ultimo_Contato']]),
            observacoes: c['Observacoes'] !== -1 ? (row[c['Observacoes']] || '') : '',
            shpprospregrespname: c['SHP_PROSP_REG_RESP_NAME'] !== -1 ? (row[c['SHP_PROSP_REG_RESP_NAME']] || '') : '',
            svc: c['SVC'] !== -1 ? (row[c['SVC']] || '') : '',
            tipo: c['Tipo'] !== -1 ? (row[c['Tipo']] || '') : '',
            servico: c['Servico'] !== -1 ? (row[c['Servico']] || '') : '',
            datasolicitacao: formatSheetDate(row[c['Data_Solicitacao']]),
            golive: c['Go_Live'] !== -1 ? formatSheetDate(row[c['Go_Live']]) : '',
            aceite: formatSheetDate(row[c['Aceite']]),
            leadid: c['LEAD_ID'] !== -1 ? (row[c['LEAD_ID']] || '') : ''
          });
        }
      }
    }
  }

  // ── Mescla extras ───────────────────────────────────────────────────────────
  const [extras, statusIdMap] = await Promise.all([
    getProspeccaoExtrasMap(),
    getPipelineStatusIdMap()
  ]);
  mergeProspeccaoExtras(extras, [minhasProspeccoes, minhasPropostas, propostasEmAndamento]);
  mergePipelineStatusId(statusIdMap, [minhasProspeccoes, minhasPropostas, propostasEmAndamento]);

  // Deduplicar
  const uniqueGeoIds = [...new Set(allGeoIds)].sort();
  const seenGeos = new Set();
  const uniqueProspectGeoIds = prospectGeoIdsWithCoords.filter(v => {
    if (seenGeos.has(v.geoId)) return false;
    seenGeos.add(v.geoId);
    return true;
  }).sort((a, b) => a.geoId.localeCompare(b.geoId));

  return {
    minhasProspeccoes,
    minhasPropostas,
    propostasEmAndamento,
    allGeoIds: uniqueGeoIds,
    prospectGeoIdsWithCoords: uniqueProspectGeoIds,
    userEmail,
    isAdmin,
    notificationCount: 0,
    proposalsByGeoId: {},
    historicoProdutividade: []
  };
}

// ─── getServicoOriginal ────────────────────────────────────────────────────────
async function getServicoOriginal(geoId) {
  const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE);
  if (rows.length < 2) return { servico: '', svc: '' };
  const headers = rows[0];
  const pGeo  = getColIndex(headers, 'Geo_Id');
  const pServ = getColIndex(headers, 'Servico');
  const pSVC  = getColIndex(headers, 'SVC');
  for (let i = 1; i < rows.length; i++) {
    if (String(rows[i][pGeo] || '').trim() === String(geoId).trim()) {
      return {
        servico: String(rows[i][pServ] || '').trim(),
        svc: String(rows[i][pSVC] || '').trim()
      };
    }
  }
  return { servico: '', svc: '' };
}

// ─── generateUniquePlaceId ────────────────────────────────────────────────────
async function generateUniquePlaceId(prefix = 'LF') {
  const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS);
  const now = new Date();
  const yy = String(now.getFullYear()).slice(-2);
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const currentPrefix = `${prefix}-${yy}${mm}`;
  const regex = new RegExp(`^${prefix}-${yy}${mm}-(\\d+)$`);

  if (rows.length < 2) return `${currentPrefix}-001`;

  const headers = rows[0];
  const placeIdCol = getColIndex(headers, 'Place_id');
  if (placeIdCol === -1) return `${currentPrefix}-001`;

  let nextSeq = 1;
  for (let i = 1; i < rows.length; i++) {
    const val = String(rows[i][placeIdCol] || '').trim();
    const match = val.match(regex);
    if (match) {
      const seq = parseInt(match[1], 10);
      if (!isNaN(seq) && seq >= nextSeq) nextSeq = seq + 1;
    }
  }
  return `${currentPrefix}-${String(nextSeq).padStart(3, '0')}`;
}

// ─── checkForOverdueAccepts ───────────────────────────────────────────────────
async function checkForOverdueAccepts(userEmail) {
  const isAdmin = await isUserAdmin(userEmail);
  const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES);
  if (rows.length < 2) return [{ message: 'Não há propostas aceitas para verificar.' }];

  const headers = rows[0];
  const geoIdCol    = getColIndex(headers, 'Geo_Id');
  const leadIdCol   = getColIndex(headers, 'LEAD_ID');
  const aceiteCol   = getColIndex(headers, 'Aceite');
  const placeNameCol= getColIndex(headers, 'Place_name');
  const consultorCol= getColIndex(headers, 'Consultor');

  if ([geoIdCol, leadIdCol, aceiteCol].some(x => x === -1)) {
    return [{ message: 'Colunas essenciais ausentes.' }];
  }

  const out = [];
  const now = Date.now();
  const threeHours = 3 * 60 * 60 * 1000;
  const cutoff = new Date('2025-09-02T00:00:00Z').getTime();

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const geoId  = String(row[geoIdCol] || '').trim();
    const leadId = String(row[leadIdCol] || '').trim();
    const aceite = row[aceiteCol] ? new Date(row[aceiteCol]) : null;

    if (!isAdmin) {
      const consultor = normalizeStatus(row[consultorCol]);
      if (consultor !== normalizeStatus(userEmail)) continue;
    }

    if (leadId === '' && aceite && aceite.getTime() >= cutoff) {
      const diff = now - aceite.getTime();
      if (diff > threeHours) {
        out.push({
          geoId,
          placeName: String(row[placeNameCol] || '').trim(),
          consultor: String(row[consultorCol] || '').trim(),
          hoursOverdue: Math.floor(diff / (1000 * 60 * 60))
        });
      }
    }
  }

  return out.length ? out : [{ message: 'Nenhum Aceite em atraso.' }];
}

// ─── updateLeadIdAndDismissNotification ───────────────────────────────────────
async function updateLeadIdAndDismissNotification(geoId, leadId) {
  const [aceitesRows, pipelineRows, propostasRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES),
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS)
  ]);

  const aHeaders = aceitesRows[0] || [];
  const aiLead  = getColIndex(aHeaders, 'LEAD_ID');
  const aiGeo   = getColIndex(aHeaders, 'Geo_Id');
  const aiPlace = getColIndex(aHeaders, 'Place_id');

  let placeIdFromAceites = '';
  for (let i = 1; i < aceitesRows.length; i++) {
    if (String(aceitesRows[i][aiGeo] || '').trim() === String(geoId).trim()) {
      await writeCell(SPREADSHEET_ID, NOME_ABA_ACEITES, i + 1, aiLead + 1, leadId);
      if (aiPlace !== -1) placeIdFromAceites = String(aceitesRows[i][aiPlace] || '').trim();
      break;
    }
  }

  // Sincroniza no Pipeline
  const pHeaders = pipelineRows[0] || [];
  const piLead = getColIndex(pHeaders, 'LEAD_ID');
  const piGeo  = getColIndex(pHeaders, 'Geo_Id');
  if (piLead !== -1 && piGeo !== -1) {
    for (let i = 1; i < pipelineRows.length; i++) {
      if (String(pipelineRows[i][piGeo] || '').trim() === String(geoId).trim()) {
        await writeCell(SPREADSHEET_ID, NOME_ABA_PIPELINE, i + 1, piLead + 1, leadId);
        break;
      }
    }
  }

  return { success: true, message: `LEAD_ID para o Geo_Id ${geoId} salvo com sucesso!` };
}

// ─── atualizarCamposSimples ───────────────────────────────────────────────────
async function atualizarCamposSimples(userEmail, data) {
  const isAdmin = await isUserAdmin(userEmail);

  const ABA_MAP = {
    minhasProspeccoes: NOME_ABA_PIPELINE,
    minhasPropostas: NOME_ABA_MINHAS_PROPOSTAS,
    propostasEmAndamento: NOME_ABA_ACEITES
  };
  const nomAba = ABA_MAP[data.aba] || NOME_ABA_PIPELINE;
  const rows = await readSheet(SPREADSHEET_ID, nomAba);
  if (rows.length < 2) return { success: false, message: `Aba "${nomAba}" não encontrada ou vazia.` };

  const headers = rows[0];
  const geoCol   = getColIndex(headers, 'Geo_Id');
  const consCol  = getColIndex(headers, 'Consultor');
  const placeCol = getColIndex(headers, 'Place_id');

  const geoIdBusca   = String(data.geoId || '').trim();
  const placeIdLimpo = data.placeId ? cleanAndFormatString(String(data.placeId)) : null;

  const PROTEGIDAS = new Set([
    'Geo_Id','Lat_geo','Long_geo','Consultor',
    'Data_Status_Em_Prospeccao','Data_Status_Analisando','Data_Status_Aceitou',
    'Data_Status_Ativo','Data_Status_Recusado','Data_Status_Desistiu','Primeiro_Contato'
  ]);
  const DATE_COLS = new Set([
    'Data_MP','Data_Treinamento','Data_Logistics','Go_Live',
    'Data_Solicitacao','Data_Ultimo_Contato','Aceite'
  ]);

  let rowIndex1Based = -1;
  for (let i = 1; i < rows.length; i++) {
    const rowGeo = String(rows[i][geoCol] || '').trim();
    if (rowGeo !== geoIdBusca) continue;
    if (placeIdLimpo && placeCol !== -1 && nomAba !== NOME_ABA_PIPELINE) {
      const rowPlace = cleanAndFormatString(String(rows[i][placeCol] || ''));
      if (rowPlace !== placeIdLimpo) continue;
    }
    if (!isAdmin && consCol !== -1) {
      const rowEmail = String(rows[i][consCol] || '').trim();
      if (normalizeStatus(rowEmail) !== normalizeStatus(userEmail)) {
        return { success: false, message: 'Sem permissão para editar este registro.' };
      }
    }
    rowIndex1Based = i + 1;
    break;
  }

  if (rowIndex1Based === -1) {
    return { success: false, message: `Registro Geo_Id="${geoIdBusca}" não encontrado na aba "${nomAba}".` };
  }

  const campos = data.campos || {};
  let atualizados = 0;

  // ── Admin: altera Consultor e propaga a todas as abas ────────────────────────
  if (isAdmin && campos.Consultor) {
    const novoConsultor = String(campos.Consultor).trim();
    if (novoConsultor) {
      for (const sheetName of [NOME_ABA_PIPELINE, NOME_ABA_MINHAS_PROPOSTAS, NOME_ABA_ACEITES]) {
        const shRows = await readSheet(SPREADSHEET_ID, sheetName);
        const shHeaders = shRows[0] || [];
        const shGeoC  = getColIndex(shHeaders, 'Geo_Id');
        const shConsC = getColIndex(shHeaders, 'Consultor');
        if (shGeoC === -1 || shConsC === -1) continue;
        for (let i = 1; i < shRows.length; i++) {
          if (String(shRows[i][shGeoC] || '').trim() === geoIdBusca) {
            await writeCell(SPREADSHEET_ID, sheetName, i + 1, shConsC + 1, novoConsultor);
            break;
          }
        }
      }
      atualizados++;
    }
    delete campos.Consultor; // evita dupla escrita abaixo
  }

  for (const colName of Object.keys(campos)) {
    if (PROTEGIDAS.has(colName)) continue;
    const colIdx0 = getColIndex(headers, colName);
    if (colIdx0 === -1) continue;

    let valor = campos[colName];
    if (DATE_COLS.has(colName) && typeof valor === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(valor)) {
      // Mantém como string ISO — Sheets aceita
    }
    if (colName === 'SVC') {
      const atual = String(rows[rowIndex1Based - 1][colIdx0] || '').trim();
      if (atual !== '') continue;
    }

    await writeCell(SPREADSHEET_ID, nomAba, rowIndex1Based, colIdx0 + 1, valor === null || valor === undefined ? '' : valor);
    atualizados++;
  }

  if (atualizados === 0) return { success: false, message: 'Nenhuma coluna válida foi atualizada.' };

  // Data_Ultimo_Contato automática
  const ducCol = getColIndex(headers, 'Data_Ultimo_Contato');
  if (ducCol !== -1 && !campos['Data_Ultimo_Contato']) {
    await writeCell(SPREADSHEET_ID, nomAba, rowIndex1Based, ducCol + 1, formatDateTimeBR(new Date()));
  }

  // Propaga ao Pipeline
  if (nomAba !== NOME_ABA_PIPELINE) {
    const SYNC_COLS = ['LEAD_ID', 'Observacoes', 'Servico', 'Motivo_recusa'];
    const pRows = await readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE);
    const pHdrs = pRows[0] || [];
    const pGeoC = getColIndex(pHdrs, 'Geo_Id');
    if (pGeoC !== -1) {
      for (let i = 1; i < pRows.length; i++) {
        if (String(pRows[i][pGeoC] || '').trim() !== geoIdBusca) continue;
        for (const col of SYNC_COLS) {
          if (campos.hasOwnProperty(col)) {
            const pC = getColIndex(pHdrs, col);
            if (pC !== -1) {
              await writeCell(SPREADSHEET_ID, NOME_ABA_PIPELINE, i + 1, pC + 1, campos[col] ?? '');
            }
          }
        }
        break;
      }
    }
  }

  return { success: true, message: `${atualizados} campo(s) atualizado(s) com sucesso.` };
}

// ─── deleteRowFromSheet ───────────────────────────────────────────────────────
async function deleteRowFromSheet(userEmail, geoId, placeId, aba) {
  const isAdmin = await isUserAdmin(userEmail);

  const ABA_MAP = {
    pipeline: NOME_ABA_PIPELINE,
    minhasProspeccoes: NOME_ABA_PIPELINE,
    minhasPropostas: NOME_ABA_MINHAS_PROPOSTAS,
    aceites: NOME_ABA_ACEITES,
    propostasEmAndamento: NOME_ABA_ACEITES
  };
  const nomAba = ABA_MAP[aba] || aba;
  const rows = await readSheet(SPREADSHEET_ID, nomAba);
  if (rows.length < 2) return { success: false, message: 'Aba vazia.' };

  const headers = rows[0];
  const geoCol   = getColIndex(headers, 'Geo_Id');
  const consCol  = getColIndex(headers, 'Consultor');
  const placeCol = getColIndex(headers, 'Place_id');
  const placeIdLimpo = placeId ? cleanAndFormatString(String(placeId)) : null;

  let rowIndex1Based = -1;
  for (let i = 1; i < rows.length; i++) {
    if (String(rows[i][geoCol] || '').trim() !== String(geoId).trim()) continue;
    if (placeIdLimpo && placeCol !== -1) {
      if (cleanAndFormatString(String(rows[i][placeCol] || '')) !== placeIdLimpo) continue;
    }
    if (!isAdmin && consCol !== -1) {
      if (normalizeStatus(rows[i][consCol] || '') !== normalizeStatus(userEmail)) {
        return { success: false, message: 'Sem permissão.' };
      }
    }
    rowIndex1Based = i + 1;
    break;
  }

  if (rowIndex1Based === -1) return { success: false, message: 'Registro não encontrado.' };
  await deleteRowByIndex(SPREADSHEET_ID, nomAba, rowIndex1Based);
  return { success: true, message: 'Registro excluído com sucesso.' };
}

// ─── handleStatusChange / updateProspect ─────────────────────────────────────
// Porta do handleStatusChange do Code.gs (lógica de negócio principal)
async function updateProspect(userEmail, data) {
  try {
    const isAdmin = await isUserAdmin(userEmail);
    const geoIdToUpdate = String(data.geoId).trim();
    const placeIdToUpdateClean = cleanAndFormatString(data.placeId);
    const placeIdToUpdateOriginal = String(data.placeId || '').trim();
    const newStatus = formatStatusForSaving(data.status);
    const now = new Date();

    const [pipelineRows, propostasRows, aceitesRows] = await Promise.all([
      readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
      readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS),
      readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES)
    ]);

    const pHeaders = pipelineRows[0] || [];
    const mpHeaders = propostasRows[0] || [];
    const aHeaders = aceitesRows[0] || [];

    const pIdx = getColumnIndices(pHeaders);
    const mpIdx = getColumnIndices(mpHeaders);
    const aIdx = getColumnIndices(aHeaders);

    // Localiza linha no Pipeline
    let pRowIndex = -1;
    for (let i = 1; i < pipelineRows.length; i++) {
      if (String(pipelineRows[i][pIdx['Geo_Id']] || '').trim() === geoIdToUpdate) {
        pRowIndex = i;
        break;
      }
    }

    if (pRowIndex === -1 && !data.isColdLead) {
      return { success: false, message: `Geo_Id '${geoIdToUpdate}' não encontrado na Pipeline.` };
    }

    const pipelineRowValues = pRowIndex !== -1 ? pipelineRows[pRowIndex] : null;

    // Permissão
    const rowUserMail = pipelineRowValues ? String(pipelineRowValues[pIdx['Consultor']] || '').trim() : userEmail;
    if (!isAdmin && normalizeStatus(rowUserMail) !== normalizeStatus(userEmail)) {
      return { success: false, message: 'Sem permissão para editar este item.' };
    }

    const servicoFromUpdate = data.servico || (pipelineRowValues ? pipelineRowValues[pIdx['Servico']] : '') || '';
    const svcFromUpdate = data.svc || (pipelineRowValues ? pipelineRowValues[pIdx['SVC']] : '') || '';
    const statusKey = normalizeStatus(newStatus);

    // ── Helper: stampar datas ─────────────────────────────────────────────────
    const mapStatusToDateCol = {
      'em prospecção': 'Data_Status_Em_Prospeccao',
      'analisando': 'Data_Status_Analisando',
      'aceitou': 'Data_Status_Aceitou',
      'ativo': 'Data_Status_Ativo',
      'recusado': 'Data_Status_Recusado',
      'desistiu': 'Data_Status_Desistiu'
    };

    async function stampDates(sheetName, idxMap, rowIndex1Based, status, when) {
      const key = normalizeStatus(status);
      const allRows = sheetName === NOME_ABA_PIPELINE ? pipelineRows
                   : sheetName === NOME_ABA_MINHAS_PROPOSTAS ? propostasRows
                   : aceitesRows;
      const currentRow = allRows[rowIndex1Based - 1] || [];

      if (idxMap['Primeiro_Contato'] !== -1 && key === 'em prospecção') {
        const current = currentRow[idxMap['Primeiro_Contato']];
        if (!current) await writeCell(SPREADSHEET_ID, sheetName, rowIndex1Based, idxMap['Primeiro_Contato'] + 1, formatDateTimeBR(when));
      }
      if (idxMap['Data_Ultimo_Contato'] !== -1) {
        await writeCell(SPREADSHEET_ID, sheetName, rowIndex1Based, idxMap['Data_Ultimo_Contato'] + 1, formatDateTimeBR(when));
      }
      const colName = mapStatusToDateCol[key];
      if (colName && idxMap[colName] !== -1) {
        await writeCell(SPREADSHEET_ID, sheetName, rowIndex1Based, idxMap[colName] + 1, formatDateTimeBR(when));
      }
      if (key === 'aceitou' && idxMap['Aceite'] !== -1) {
        await writeCell(SPREADSHEET_ID, sheetName, rowIndex1Based, idxMap['Aceite'] + 1, formatDateTimeBR(when));
      }
    }

    async function setIfExists(sheetName, idxMap, rowIndex1Based, colName, value, allRows) {
      const colIndex = idxMap[colName];
      if (colIndex === -1) return;
      if (colName === 'SVC' && String(value || '').trim() !== '') {
        const current = (allRows[rowIndex1Based - 1] || [])[colIndex];
        if (String(current || '').trim() !== '') return;
      }
      await writeCell(SPREADSHEET_ID, sheetName, rowIndex1Based, colIndex + 1, value);
    }

    function createNewRow(baseRow, srcHeaders, dstHeaders, srcIdx, dstIdx) {
      const newRow = Array(dstHeaders.length).fill('');
      const COLS_NAO_COPIAR = new Set([
        'Data_MP','Data_Treinamento','Data_Logistics','Go_Live','Primeiro_Contato',
        'Aceite','Data_Status_Em_Prospeccao','Data_Status_Analisando',
        'Data_Status_Aceitou','Data_Status_Ativo','Data_Status_Recusado',
        'Data_Status_Desistiu','Data_Ultimo_Contato'
      ]);
      COLUNAS_PIPELINE.forEach(col => {
        if (COLS_NAO_COPIAR.has(col)) return;
        const pi = srcIdx[col]; const ni = dstIdx[col];
        if (pi !== -1 && ni !== -1) {
          if (col === 'Lat_geo' || col === 'Long_geo') {
            // Copia a string exata da Pipeline sem converter para float,
            // para não perder precisão nem criar formato errado.
            const rawCoord = String(baseRow[pi] || '').trim().replace(',', '.');
            newRow[ni] = rawCoord;
          } else {
            newRow[ni] = baseRow[pi];
          }
        }
      });
      return newRow;
    }

    // ── ACEITOU / ATIVO ───────────────────────────────────────────────────────
    if (statusKey === normalizeStatus(STATUS.ACEITOU) || statusKey === normalizeStatus(STATUS.ATIVO)) {
      if (!placeIdToUpdateClean) return { success: false, message: 'Place_Id é obrigatório para Aceite/Ativo.' };

      // 1) Minhas Propostas
      let mpRow = -1;
      for (let i = 1; i < propostasRows.length; i++) {
        const g = String(propostasRows[i][mpIdx['Geo_Id']] || '').trim();
        const pid = cleanAndFormatString(propostasRows[i][mpIdx['Place_id']] || '');
        if (g === geoIdToUpdate) {
          if (pid === placeIdToUpdateClean) { mpRow = i; }
          else {
            const r = i + 1;
            const curSt = normalizeStatus(propostasRows[i][mpIdx['Status_prospeccao']] || '');
            if (curSt !== normalizeStatus(STATUS.RECUSADO) && curSt !== normalizeStatus(STATUS.DESISTIU)) {
              await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Status_prospeccao', STATUS.RECUSADO, propostasRows);
              await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Motivo_recusa', 'Outro Place aceitou a oferta', propostasRows);
              await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, STATUS.RECUSADO, now);
            }
          }
        }
      }

      if (mpRow !== -1) {
        const r = mpRow + 1;
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Status_prospeccao', newStatus, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Place_id', placeIdToUpdateOriginal, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Place_name', data.placeName || '', propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Telefone_Place', data.telefonePlace || '', propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Servico', servicoFromUpdate, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'LEAD_ID', data.leadId || '', propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'SVC', svcFromUpdate, propostasRows);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, newStatus, now);
      } else {
        const baseRow = pipelineRowValues || createColdLeadBaseRow(data, pHeaders, userEmail);
        const newRow = createNewRow(baseRow, pHeaders, mpHeaders, pIdx, mpIdx);
        if (mpIdx['Status_prospeccao'] !== -1) newRow[mpIdx['Status_prospeccao']] = newStatus;
        if (mpIdx['Place_id'] !== -1) newRow[mpIdx['Place_id']] = placeIdToUpdateOriginal;
        if (mpIdx['Place_name'] !== -1) newRow[mpIdx['Place_name']] = data.placeName || '';
        if (mpIdx['Telefone_Place'] !== -1) newRow[mpIdx['Telefone_Place']] = data.telefonePlace || '';
        if (mpIdx['Servico'] !== -1) newRow[mpIdx['Servico']] = servicoFromUpdate;
        if (mpIdx['LEAD_ID'] !== -1) newRow[mpIdx['LEAD_ID']] = data.leadId || '';
        await appendRow(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS, newRow);
        // stampDates via append na última linha
        const freshRows = await readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, freshRows.length, newStatus, now);
        mpRow = freshRows.length - 2; // 0-based index
      }

      // 2) Aceites
      let aRow = -1;
      for (let i = 1; i < aceitesRows.length; i++) {
        const g = String(aceitesRows[i][aIdx['Geo_Id']] || '').trim();
        const pid = cleanAndFormatString(aceitesRows[i][aIdx['Place_id']] || '');
        if (g === geoIdToUpdate && pid === placeIdToUpdateClean) { aRow = i; break; }
      }

      if (aRow !== -1) {
        const r = aRow + 1;
        await setIfExists(NOME_ABA_ACEITES, aIdx, r, 'Status_prospeccao', newStatus, aceitesRows);
        await setIfExists(NOME_ABA_ACEITES, aIdx, r, 'Place_id', placeIdToUpdateOriginal, aceitesRows);
        await setIfExists(NOME_ABA_ACEITES, aIdx, r, 'Telefone_Place', data.telefonePlace || '', aceitesRows);
        await setIfExists(NOME_ABA_ACEITES, aIdx, r, 'Servico', servicoFromUpdate, aceitesRows);
        await setIfExists(NOME_ABA_ACEITES, aIdx, r, 'LEAD_ID', data.leadId || '', aceitesRows);
        await stampDates(NOME_ABA_ACEITES, aIdx, r, newStatus, now);
      } else {
        const baseRow = pipelineRowValues || createColdLeadBaseRow(data, pHeaders, userEmail);
        const newRow = createNewRow(baseRow, pHeaders, aHeaders, pIdx, aIdx);
        if (aIdx['Status_prospeccao'] !== -1) newRow[aIdx['Status_prospeccao']] = newStatus;
        if (aIdx['Place_id'] !== -1) newRow[aIdx['Place_id']] = placeIdToUpdateOriginal;
        if (aIdx['Place_name'] !== -1) newRow[aIdx['Place_name']] = data.placeName || '';
        if (aIdx['Telefone_Place'] !== -1) newRow[aIdx['Telefone_Place']] = data.telefonePlace || '';
        if (aIdx['Servico'] !== -1) newRow[aIdx['Servico']] = servicoFromUpdate;
        if (aIdx['LEAD_ID'] !== -1) newRow[aIdx['LEAD_ID']] = data.leadId || '';
        await appendRow(SPREADSHEET_ID, NOME_ABA_ACEITES, newRow);
        const freshRows = await readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES);
        await stampDates(NOME_ABA_ACEITES, aIdx, freshRows.length, newStatus, now);
      }

      // 3) Pipeline
      if (pRowIndex !== -1) {
        const pRow1Based = pRowIndex + 1;
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'Status_prospeccao', newStatus, pipelineRows);
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'Place_id', placeIdToUpdateOriginal, pipelineRows);
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'Place_name', data.placeName || '', pipelineRows);
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'Telefone_Place', data.telefonePlace || '', pipelineRows);
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'Servico', servicoFromUpdate, pipelineRows);
        await setIfExists(NOME_ABA_PIPELINE, pIdx, pRow1Based, 'LEAD_ID', data.leadId || '', pipelineRows);
        await stampDates(NOME_ABA_PIPELINE, pIdx, pRow1Based, newStatus, now);
      }

      return { success: true, message: `Proposta marcada como "${newStatus}". Pipeline atualizado.` };
    }

    // ── DESISTIU / RECUSADO ───────────────────────────────────────────────────
    if (statusKey === normalizeStatus(STATUS.DESISTIU) || statusKey === normalizeStatus(STATUS.RECUSADO)) {
      if (!placeIdToUpdateClean) return { success: false, message: 'Place_Id é obrigatório para Recusa/Desistência.' };

      // Remove de Aceites
      for (let i = 1; i < aceitesRows.length; i++) {
        const g = String(aceitesRows[i][aIdx['Geo_Id']] || '').trim();
        const pid = cleanAndFormatString(aceitesRows[i][aIdx['Place_id']] || '');
        if (g === geoIdToUpdate && pid === placeIdToUpdateClean) {
          await deleteRowByIndex(SPREADSHEET_ID, NOME_ABA_ACEITES, i + 1);
          break;
        }
      }

      // Minhas Propostas: marca status
      let mpRow = -1;
      for (let i = 1; i < propostasRows.length; i++) {
        const g = String(propostasRows[i][mpIdx['Geo_Id']] || '').trim();
        const pid = cleanAndFormatString(propostasRows[i][mpIdx['Place_id']] || '');
        if (g === geoIdToUpdate && pid === placeIdToUpdateClean) { mpRow = i; break; }
      }
      if (mpRow !== -1) {
        const r = mpRow + 1;
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Status_prospeccao', newStatus, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Motivo_recusa', data.motivoRecusa || '', propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Servico', servicoFromUpdate, propostasRows);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, newStatus, now);
      } else {
        const baseRow = pipelineRowValues || createColdLeadBaseRow(data, pHeaders, userEmail);
        const newRow = createNewRow(baseRow, pHeaders, mpHeaders, pIdx, mpIdx);
        if (mpIdx['Status_prospeccao'] !== -1) newRow[mpIdx['Status_prospeccao']] = newStatus;
        if (mpIdx['Place_id'] !== -1) newRow[mpIdx['Place_id']] = placeIdToUpdateOriginal;
        if (mpIdx['Motivo_recusa'] !== -1) newRow[mpIdx['Motivo_recusa']] = data.motivoRecusa || '';
        if (mpIdx['Servico'] !== -1) newRow[mpIdx['Servico']] = servicoFromUpdate;
        await appendRow(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS, newRow);
        const freshRows = await readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, freshRows.length, newStatus, now);
      }

      // Pipeline: reseta para Em prospecção
      if (pRowIndex !== -1) {
        const rP = pRowIndex + 1;
        const resetFields = {
          'Status_prospeccao': STATUS.PROSPECCAO,
          'Place_id': '', 'Place_name': '', 'Telefone_Place': '', 'LEAD_ID': '',
          'Aceite': '', 'Data_MP': '', 'Data_Treinamento': '', 'Data_Logistics': '',
          'Primeiro_Contato': '', 'Data_Ultimo_Contato': '', 'Go_Live': '',
          'Data_Status_Em_Prospeccao': '', 'Data_Status_Analisando': '',
          'Data_Status_Aceitou': '', 'Data_Status_Ativo': '',
          'Data_Status_Recusado': '', 'Data_Status_Desistiu': ''
        };
        for (const [col, val] of Object.entries(resetFields)) {
          if (pIdx[col] !== -1) {
            await writeCell(SPREADSHEET_ID, NOME_ABA_PIPELINE, rP, pIdx[col] + 1, val);
          }
        }
      }

      return { success: true, message: `Marcado como "${newStatus}". Pipeline voltou para "Em prospecção".` };
    }

    // ── STATUS INTERMEDIÁRIOS ─────────────────────────────────────────────────
    if (placeIdToUpdateClean) {
      let mpRow = -1;
      for (let i = 1; i < propostasRows.length; i++) {
        const g = String(propostasRows[i][mpIdx['Geo_Id']] || '').trim();
        const pid = cleanAndFormatString(propostasRows[i][mpIdx['Place_id']] || '');
        if (g === geoIdToUpdate && pid === placeIdToUpdateClean) { mpRow = i; break; }
      }

      if (mpRow !== -1) {
        const r = mpRow + 1;
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Status_prospeccao', newStatus, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Place_id', placeIdToUpdateOriginal, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Servico', servicoFromUpdate, propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'Observacoes', data.observacoes || '', propostasRows);
        await setIfExists(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, 'LEAD_ID', data.leadId || '', propostasRows);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, r, newStatus, now);
      } else {
        const baseRow = pipelineRowValues || createColdLeadBaseRow(data, pHeaders, userEmail);
        const newRow = createNewRow(baseRow, pHeaders, mpHeaders, pIdx, mpIdx);
        if (mpIdx['Status_prospeccao'] !== -1) newRow[mpIdx['Status_prospeccao']] = newStatus;
        if (mpIdx['Place_id'] !== -1) newRow[mpIdx['Place_id']] = placeIdToUpdateOriginal;
        if (mpIdx['Place_name'] !== -1) newRow[mpIdx['Place_name']] = data.placeName || '';
        if (mpIdx['Telefone_Place'] !== -1) newRow[mpIdx['Telefone_Place']] = data.telefonePlace || '';
        if (mpIdx['Servico'] !== -1) newRow[mpIdx['Servico']] = servicoFromUpdate;
        if (mpIdx['Observacoes'] !== -1) newRow[mpIdx['Observacoes']] = data.observacoes || '';
        if (mpIdx['LEAD_ID'] !== -1) newRow[mpIdx['LEAD_ID']] = data.leadId || '';
        await appendRow(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS, newRow);
        const freshRows = await readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS);
        await stampDates(NOME_ABA_MINHAS_PROPOSTAS, mpIdx, freshRows.length, newStatus, now);
      }
    }

    // Pipeline: atualiza campos permitidos
    if (pRowIndex !== -1) {
      const rowP = pRowIndex + 1;
      await setIfExists(NOME_ABA_PIPELINE, pIdx, rowP, 'Observacoes', data.observacoes || '', pipelineRows);
      await setIfExists(NOME_ABA_PIPELINE, pIdx, rowP, 'Motivo_recusa', data.motivoRecusa || '', pipelineRows);
      await setIfExists(NOME_ABA_PIPELINE, pIdx, rowP, 'Servico', servicoFromUpdate, pipelineRows);
      await setIfExists(NOME_ABA_PIPELINE, pIdx, rowP, 'LEAD_ID', data.leadId || '', pipelineRows);
      await setIfExists(NOME_ABA_PIPELINE, pIdx, rowP, 'SVC', svcFromUpdate, pipelineRows);

      if (pIdx['Primeiro_Contato'] !== -1 && statusKey === 'em prospecção') {
        const current = pipelineRows[pRowIndex][pIdx['Primeiro_Contato']];
        if (!current) await writeCell(SPREADSHEET_ID, NOME_ABA_PIPELINE, rowP, pIdx['Primeiro_Contato'] + 1, formatDateTimeBR(now));
      }
      if (pIdx['Data_Ultimo_Contato'] !== -1) {
        await writeCell(SPREADSHEET_ID, NOME_ABA_PIPELINE, rowP, pIdx['Data_Ultimo_Contato'] + 1, formatDateTimeBR(now));
      }
    }

    return { success: true, message: `Status "${newStatus}" atualizado com sucesso.` };

  } catch (e) {
    return { success: false, message: `Erro ao atualizar: ${e.message}` };
  }
}

function createColdLeadBaseRow(data, pHeaders, userEmail) {
  const pIdx = getColumnIndices(pHeaders);
  const baseRow = Array(pHeaders.length).fill('');
  baseRow[pIdx['Geo_Id']] = data.geoId || '';
  baseRow[pIdx['Lat_geo']] = data.lat || '';
  baseRow[pIdx['Long_geo']] = data.lon || '';
  baseRow[pIdx['Servico']] = data.servico || '';
  baseRow[pIdx['Consultor']] = userEmail;
  baseRow[pIdx['Status_prospeccao']] = STATUS.PROSPECCAO;
  baseRow[pIdx['Place_id']] = data.placeId || '';
  baseRow[pIdx['Place_name']] = data.placeName || '';
  baseRow[pIdx['Telefone_Place']] = data.telefonePlace || '';
  baseRow[pIdx['SVC']] = data.svc || '';
  baseRow[pIdx['Tipo']] = 'Frio';
  return baseRow;
}

async function addColdLead(userEmail, leadData) {
  return updateProspect(userEmail, { ...leadData, isColdLead: true });
}

async function createOrUpdateProposalFromLead(userEmail, prospectData) {
  const result = await updateProspect(userEmail, { ...prospectData, isColdLead: false });

  // Dispara e-mail somente se status = "Em prospecção" e operação foi bem-sucedida
  const newStatus = String(prospectData.status || '').toLowerCase().trim();
  if (result && result.success && (newStatus === 'em prospecção' || newStatus === 'em prospeccao')) {
    try {
      const { sendLeadProspectingEmail } = require('./email');
      const emailPayload = {
        leadId:        String(prospectData.placeId || prospectData.geoId || '').trim(),
        nome:          String(prospectData.placeName || '').trim(),
        email:         String(prospectData.emailLead || prospectData.shpRespEmailDesc || '').trim(),
        dataCadastro:  String(prospectData.dataCadastro || '').trim(),
        tipoLead:      String(prospectData.tipo || prospectData.tipoLead || '').trim(),
        statusContato: prospectData.status,
        consultorEmail: userEmail
      };
      const emailResult = await sendLeadProspectingEmail(emailPayload);
      result.emailSent   = emailResult.emailSent;
      result.emailReason = emailResult.reason;
    } catch (emailErr) {
      console.error('[EMAIL LEAD] Erro ao disparar e-mail:', emailErr.message);
      result.emailSent   = false;
      result.emailReason = 'Erro interno no disparo: ' + emailErr.message;
    }
  } else if (result) {
    result.emailSent   = false;
    result.emailReason = 'Condições de envio não atendidas (status ou tipo de lead).';
  }

  return result;
}

// ─── editarRegistro ───────────────────────────────────────────────────────────
// Edita campos livres de um registro sem alterar lógica de status.
// dados: { aba, geoId, placeId, campos: { colName: valor, ... } }
async function editarRegistro(userEmail, dados) {
  const { aba, geoId, placeId, campos } = dados;
  if (!aba || !geoId || !campos || Object.keys(campos).length === 0) {
    return { success: false, message: 'aba, geoId e campos são obrigatórios.' };
  }

  const ABA_MAP = {
    pipeline: NOME_ABA_PIPELINE,
    minhasPropostas: NOME_ABA_MINHAS_PROPOSTAS,
    aceites: NOME_ABA_ACEITES
  };
  const nomAba = ABA_MAP[aba] || aba;

  const rows = await readSheet(SPREADSHEET_ID, nomAba);
  if (rows.length < 2) return { success: false, message: 'Aba vazia.' };

  const headers = rows[0];
  const geoCol = getColIndex(headers, 'Geo_Id');
  const placeCol = getColIndex(headers, 'Place_id');
  const placeIdLimpo = placeId ? cleanAndFormatString(String(placeId)) : null;

  let rowIndex1Based = -1;
  for (let i = 1; i < rows.length; i++) {
    if (String(rows[i][geoCol] || '').trim() !== String(geoId).trim()) continue;
    if (placeIdLimpo && placeCol !== -1) {
      if (cleanAndFormatString(String(rows[i][placeCol] || '')) !== placeIdLimpo) continue;
    }
    rowIndex1Based = i + 1;
    break;
  }

  if (rowIndex1Based === -1) return { success: false, message: 'Registro não encontrado.' };

  const sheetsClient = await getSheetsClient();
  const updates = [];

  for (const [colName, valor] of Object.entries(campos)) {
    const colIndex = getColIndex(headers, colName);
    if (colIndex === -1) continue;
    const colLetter = colToLetter(colIndex + 1);
    updates.push(
      sheetsClient.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${nomAba}!${colLetter}${rowIndex1Based}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[valor]] }
      })
    );
  }

  await Promise.all(updates);
  cacheClear(SPREADSHEET_ID + '_' + nomAba);
  return { success: true, message: 'Campos atualizados com sucesso.' };
}

const GOLIVE_SPREADSHEET_ID = '1GH-NsJ4EkqD91p7SLUlDYLzJDs0M_ufameiq_VcNKpo';
const GOLIVE_TAB_NAME = 'Extração Go Live';

/**
 * Escreve linhas do BigQuery na aba "Extração Go Live" da planilha Go Live.
 * Porta de dadosGoLive() do Apps Script.
 */
async function escreverDadosGoLive(rows) {
  if (!rows || rows.length === 0) {
    return { success: true, totalRows: 0, message: 'Nenhum dado retornado pelo BigQuery.' };
  }

  const sheetsClient = await getSheetsClient();

  const headers = Object.keys(rows[0]);
  const dataRows = rows.map(row => headers.map(h => {
    const v = row[h];
    if (v === null || v === undefined) return '';
    if (typeof v === 'object' && v.value !== undefined) return String(v.value);
    return String(v);
  }));

  const values = [headers, ...dataRows];
  const numCols = headers.length;
  const lastCol = String.fromCharCode(64 + numCols);

  await sheetsClient.spreadsheets.values.clear({
    spreadsheetId: GOLIVE_SPREADSHEET_ID,
    range: `'${GOLIVE_TAB_NAME}'!A1:${lastCol}10000`
  });

  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: GOLIVE_SPREADSHEET_ID,
    range: `'${GOLIVE_TAB_NAME}'!A1`,
    valueInputOption: 'RAW',
    requestBody: { values }
  });

  return { success: true, totalRows: dataRows.length };
}

// ─── Escrita genérica de dados BQ em aba ─────────────────────────────────────
/**
 * Limpa e reescreve uma aba da planilha com rows vindas do BigQuery.
 * rows: array de objetos (retorno do bq.query)
 */
async function escreverDadosBQParaAba(rows, tabName, spreadsheetId) {
  const ssId = spreadsheetId || SPREADSHEET_ID;
  if (!rows || rows.length === 0) {
    return { success: true, totalRows: 0, message: 'Nenhum dado retornado pelo BigQuery.' };
  }
  const sheetsClient = await getSheetsClient();
  const headers = Object.keys(rows[0]);
  const dataRows = rows.map(row => headers.map(h => {
    const v = row[h];
    if (v === null || v === undefined) return '';
    if (typeof v === 'object' && v.value !== undefined) return String(v.value);
    if (v instanceof Date) return v.toISOString().substring(0, 10);
    return String(v);
  }));
  const values = [headers, ...dataRows];
  const numCols = headers.length;
  const lastColLetter = colToLetter(numCols);
  await sheetsClient.spreadsheets.values.clear({
    spreadsheetId: ssId,
    range: `'${tabName}'!A1:${lastColLetter}100000`
  });
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: ssId,
    range: `'${tabName}'!A1`,
    valueInputOption: 'RAW',
    requestBody: { values }
  });
  return { success: true, totalRows: dataRows.length };
}

async function escreverDadosLogistics(rows)    { return escreverDadosBQParaAba(rows, 'Logistics'); }
async function escreverDadosMercadopago(rows)  { return escreverDadosBQParaAba(rows, 'Mercado Pago'); }
async function escreverDadosNewPlace(rows)     { return escreverDadosBQParaAba(rows, 'NEW_PLACE'); }
async function escreverDadosTreinamento(rows)  { return escreverDadosBQParaAba(rows, 'Treinamento'); }
async function escreverDadosVolumetria(rows)   { return escreverDadosBQParaAba(rows, 'Base'); }
async function escreverDadosSBO(rows)          { return escreverDadosBQParaAba(rows, 'Extração SBO1'); }

// ─── Batch write helper ───────────────────────────────────────────────────────
async function batchWriteRanges(spreadsheetId, dataRanges) {
  if (!dataRanges || dataRanges.length === 0) return;
  const sheetsClient = await getSheetsClient();
  await sheetsClient.spreadsheets.values.batchUpdate({
    spreadsheetId,
    requestBody: {
      valueInputOption: 'RAW',
      data: dataRanges
    }
  });
}

// ─── contarContatosPorGeoID ───────────────────────────────────────────────────
// Porta de contarContatosPorGeoID() do Apps Script.
async function contarContatosPorGeoID() {
  const [pipelineRows, propostasRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS)
  ]);

  if (propostasRows.length < 2 || pipelineRows.length < 2) {
    return { success: false, message: 'Abas sem dados suficientes.' };
  }

  // Conta ocorrências de Geo_Id em Minhas Propostas (col A)
  const contagem = {};
  for (let i = 1; i < propostasRows.length; i++) {
    const geoId = String(propostasRows[i][0] || '').trim();
    if (geoId) contagem[geoId] = (contagem[geoId] || 0) + 1;
  }

  // Atualiza col V (índice 21) do Pipeline
  const pHeaders  = pipelineRows[0] || [];
  const cGeo      = getColIndex(pHeaders, 'Geo_Id');
  const cContatos = getColIndex(pHeaders, 'Contatos_Feitos');
  if (cGeo === -1 || cContatos === -1) {
    return { success: false, message: 'Colunas Geo_Id ou Contatos_Feitos não encontradas no Pipeline.' };
  }

  const novoValores = [];
  for (let i = 1; i < pipelineRows.length; i++) {
    const geoId = String(pipelineRows[i][cGeo] || '').trim();
    novoValores.push([geoId ? (contagem[geoId] || 0) : 0]);
  }

  const colLetter = colToLetter(cContatos + 1);
  await batchWriteRanges(SPREADSHEET_ID, [{
    range: `${NOME_ABA_PIPELINE}!${colLetter}2:${colLetter}${pipelineRows.length}`,
    values: novoValores
  }]);

  return { success: true, message: `${novoValores.length} registros atualizados.` };
}

// ─── preencherSVCMaisProximo ──────────────────────────────────────────────────
// Porta de preencherSVCMaisProximo() do Apps Script.
function _haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

async function preencherSVCMaisProximo() {
  const svcRows = await readSheet(SPREADSHEET_ID, 'SVC');
  if (svcRows.length < 2) return { success: false, message: 'Aba SVC vazia.' };

  const svcList = [];
  for (let i = 1; i < svcRows.length; i++) {
    const lat = parseFloat(svcRows[i][0]);
    const lon = parseFloat(svcRows[i][1]);
    if (!isNaN(lat) && !isNaN(lon)) {
      svcList.push({ lat, lon, id: svcRows[i][2], regional: svcRows[i][3] });
    }
  }
  if (!svcList.length) return { success: false, message: 'Nenhum SVC válido.' };

  const ABAS = [NOME_ABA_PIPELINE, NOME_ABA_ACEITES, NOME_ABA_MINHAS_PROPOSTAS];
  const updates = {};
  let totalLinhas = 0;

  for (const nomeAba of ABAS) {
    const rows = await readSheet(SPREADSHEET_ID, nomeAba);
    if (rows.length < 2) continue;
    const headers  = rows[0];
    const cLat     = getColIndex(headers, 'Lat_geo');
    const cLon     = getColIndex(headers, 'Long_geo');
    const cRegion  = getColIndex(headers, 'Regional');
    const cSVC     = getColIndex(headers, 'SVC');
    if (cLat === -1 || cLon === -1) continue;

    const colH = [], colN = [];
    for (let i = 1; i < rows.length; i++) {
      const lat = parseFloat(rows[i][cLat]);
      const lon = parseFloat(rows[i][cLon]);
      if (isNaN(lat) || isNaN(lon)) { colH.push(['']); colN.push(['']); continue; }

      let minDist = Infinity, nearest = null;
      for (const s of svcList) {
        const d = _haversine(lat, lon, s.lat, s.lon);
        if (d < minDist) { minDist = d; nearest = s; }
      }
      colH.push([nearest ? nearest.regional : '']);
      colN.push([nearest ? nearest.id : '']);
    }

    const dataToWrite = [];
    if (cRegion !== -1) {
      dataToWrite.push({ range: `${nomeAba}!${colToLetter(cRegion+1)}2:${colToLetter(cRegion+1)}${rows.length}`, values: colH });
    }
    if (cSVC !== -1) {
      dataToWrite.push({ range: `${nomeAba}!${colToLetter(cSVC+1)}2:${colToLetter(cSVC+1)}${rows.length}`, values: colN });
    }
    if (dataToWrite.length) await batchWriteRanges(SPREADSHEET_ID, dataToWrite);
    totalLinhas += rows.length - 1;
  }

  return { success: true, message: `${totalLinhas} linhas processadas.` };
}

// ─── preencherMultiplasAbas ───────────────────────────────────────────────────
// Porta de preencherMultiplasAbas() do Apps Script.
// Preenche Place_ID (col K) nas abas com base em LEAD_ID (col AH) da aba NEW_PLACE.
async function preencherMultiplasAbas() {
  const newPlaceRows = await readSheet(SPREADSHEET_ID, 'NEW_PLACE');
  if (newPlaceRows.length < 2) return { success: false, message: 'Aba NEW_PLACE vazia.' };

  const placeIdMap = new Map();
  for (let i = 1; i < newPlaceRows.length; i++) {
    const leadId  = newPlaceRows[i][0]; // col A = LEAD_ID
    const placeId = newPlaceRows[i][1]; // col B = PLACE_ID
    if (leadId !== '' && leadId != null && placeId !== '' && placeId != null) {
      placeIdMap.set(String(leadId).trim(), placeId);
    }
  }

  let totalPreenchidos = 0;
  for (const nomeAba of [NOME_ABA_ACEITES, NOME_ABA_PIPELINE, NOME_ABA_MINHAS_PROPOSTAS]) {
    const rows = await readSheet(SPREADSHEET_ID, nomeAba);
    if (rows.length < 2) continue;
    const headers   = rows[0];
    const cPlaceId  = getColIndex(headers, 'Place_id');
    const cLeadId   = getColIndex(headers, 'LEAD_ID');
    if (cPlaceId === -1 || cLeadId === -1) continue;

    const newPlaceIds = [];
    for (let i = 1; i < rows.length; i++) {
      const leadIdRaw = rows[i][cLeadId];
      const existPlaceId = rows[i][cPlaceId];
      if (leadIdRaw !== '' && leadIdRaw != null) {
        const found = placeIdMap.get(String(leadIdRaw).trim());
        if (found !== undefined) { newPlaceIds.push([found]); totalPreenchidos++; continue; }
      }
      newPlaceIds.push([existPlaceId]);
    }
    await batchWriteRanges(SPREADSHEET_ID, [{
      range: `${nomeAba}!${colToLetter(cPlaceId+1)}2:${colToLetter(cPlaceId+1)}${rows.length}`,
      values: newPlaceIds
    }]);
  }

  return { success: true, message: `${totalPreenchidos} Place_IDs preenchidos.` };
}

// ─── preencherDataLogistics ───────────────────────────────────────────────────
// Porta de preencherDataLogistics() do Apps Script.
// Preenche Aceites col R (Data_Logistics) a partir da planilha externa Logistics DC.
const EXT_LOGISTICS_DC_SS_ID = '1h4DbreIAFfpuedV4x4bgBtSunC9leHhUXTyVnTh_thA';

async function preencherDataLogistics() {
  const [aceitesRows, logisticsRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES),
    readSheet(EXT_LOGISTICS_DC_SS_ID, 'Logistics DC')
  ]);
  if (aceitesRows.length < 2) return { success: false, message: 'Aba Aceites vazia.' };
  if (logisticsRows.length < 2) return { success: false, message: 'Aba Logistics DC não encontrada.' };

  // Mapa: place_id → data (col B=idx1, data=col M=idx12 → mas o script usa idx12 para data)
  const mapaLogistics = {};
  for (let i = 1; i < logisticsRows.length; i++) {
    const placeId = logisticsRows[i][1]; // col B (idx 1)
    const data    = logisticsRows[i][12]; // col M (idx 12)
    if (placeId) mapaLogistics[String(placeId).trim()] = data;
  }

  const aHeaders = aceitesRows[0] || [];
  const cPlaceId = getColIndex(aHeaders, 'Place_id');
  const cDataLog = getColIndex(aHeaders, 'Data_Logistics');
  if (cPlaceId === -1 || cDataLog === -1) {
    return { success: false, message: 'Colunas Place_id ou Data_Logistics não encontradas em Aceites.' };
  }

  const novosDados = [];
  for (let i = 1; i < aceitesRows.length; i++) {
    const placeId = String(aceitesRows[i][cPlaceId] || '').trim();
    const atual   = aceitesRows[i][cDataLog];
    if (!atual && placeId && mapaLogistics[placeId]) {
      const d = mapaLogistics[placeId];
      novosDados.push([d instanceof Date ? d.toISOString().substring(0, 10) : String(d || '')]);
    } else {
      novosDados.push([atual !== undefined && atual !== null ? atual : '']);
    }
  }

  await batchWriteRanges(SPREADSHEET_ID, [{
    range: `${NOME_ABA_ACEITES}!${colToLetter(cDataLog+1)}2:${colToLetter(cDataLog+1)}${aceitesRows.length}`,
    values: novosDados
  }]);

  return { success: true, message: `${aceitesRows.length - 1} linhas processadas.` };
}

// ─── updateGoLiveDates ────────────────────────────────────────────────────────
// Porta de updateGoLiveDatess() do Apps Script.
const EXT_GOLIVE_SS_ID = '1g9NjIriwwqU5Dew8I5cH03NRjQy2Q5xJJIYiON26O5g';

async function updateGoLiveDates() {
  const srcRows = await readSheet(EXT_GOLIVE_SS_ID, 'Go_Live');
  if (srcRows.length < 2) return { success: false, message: 'Planilha externa Go_Live vazia.' };

  // Mapa: id (col C=idx2) → { date: col K=idx9, approval: col M=idx11 }
  const srcMap = {};
  for (let i = 1; i < srcRows.length; i++) {
    const id = srcRows[i][2]; // col C
    if (id) {
      srcMap[String(id).trim()] = {
        date:     srcRows[i][9],  // col J → 10th col = idx 9
        approval: srcRows[i][11]  // col L → 12th col = idx 11
      };
    }
  }

  const today = new Date(); today.setHours(0,0,0,0);
  let totalUpdated = 0;

  for (const nomeAba of [NOME_ABA_ACEITES, NOME_ABA_PIPELINE, NOME_ABA_MINHAS_PROPOSTAS]) {
    const rows = await readSheet(SPREADSHEET_ID, nomeAba);
    if (rows.length < 2) continue;
    const headers    = rows[0];
    const cPlaceId   = getColIndex(headers, 'Place_id');
    const cGoLive    = getColIndex(headers, 'Go_Live');
    const cStatus    = getColIndex(headers, 'Status_prospeccao');
    if (cPlaceId === -1 || cGoLive === -1 || cStatus === -1) continue;

    const updates = [];
    for (let i = 1; i < rows.length; i++) {
      const placeId = String(rows[i][cPlaceId] || '').trim();
      if (!placeId || !srcMap[placeId]) continue;
      const { date: extDate, approval } = srcMap[placeId];
      if (!extDate) continue;
      const extDateObj = extDate instanceof Date ? extDate : new Date(extDate);
      if (isNaN(extDateObj.getTime())) continue;
      const formattedDate = formatSheetDate(extDateObj);

      let newStatus = String(rows[i][cStatus] || '').trim();
      const approvalStr = String(approval || '').trim();

      if (approvalStr === 'Aprovado' && extDateObj <= today && newStatus !== 'Ativo') {
        newStatus = 'Ativo';
      } else if (approvalStr === 'Pendente' && extDateObj < today && newStatus !== 'Activación pendiente') {
        newStatus = 'Activación pendiente';
      }

      updates.push({ row: i + 1, goLive: formattedDate, status: newStatus });
      totalUpdated++;
    }

    const sheetsClient = await getSheetsClient();
    for (const upd of updates) {
      await sheetsClient.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          valueInputOption: 'RAW',
          data: [
            { range: `${nomeAba}!${colToLetter(cGoLive+1)}${upd.row}`, values: [[upd.goLive]] },
            { range: `${nomeAba}!${colToLetter(cStatus+1)}${upd.row}`, values: [[upd.status]] }
          ]
        }
      });
    }
  }

  return { success: true, message: `${totalUpdated} linhas atualizadas.` };
}

// ─── syncDataPipeline ─────────────────────────────────────────────────────────
// Porta de syncData() do Apps Script.
// Sincroniza data de solicitação da planilha externa → Pipeline col O (Data_Solicitacao)
const EXT_PROSPECCAO_COORD_SS_ID = process.env.EXTERNAL_PROSPECCAO_SHEET_ID;
const EXT_PROSPECCAO_COORD_SHEET = process.env.EXTERNAL_PROSPECCAO_SHEET_NAME || 'Prospecção de Coordenadas';

async function syncDataPipeline() {
  if (!EXT_PROSPECCAO_COORD_SS_ID) {
    return { success: false, message: 'EXTERNAL_PROSPECCAO_SHEET_ID não configurado.' };
  }
  const [srcRows, pipelineRows] = await Promise.all([
    readSheet(EXT_PROSPECCAO_COORD_SS_ID, EXT_PROSPECCAO_COORD_SHEET),
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE)
  ]);
  if (pipelineRows.length < 2) return { success: false, message: 'Pipeline vazio.' };

  // Mapa: geoId (col B=idx1) → timestamp (col C=idx2)
  const mapaOrigem = new Map();
  for (let i = 1; i < srcRows.length; i++) {
    const geoId = srcRows[i][1];
    const ts    = srcRows[i][2];
    if (geoId) mapaOrigem.set(String(geoId).trim(), ts);
  }

  const pHeaders   = pipelineRows[0] || [];
  const cGeo       = getColIndex(pHeaders, 'Geo_Id');
  const cDataSol   = getColIndex(pHeaders, 'Data_Solicitacao');
  if (cGeo === -1 || cDataSol === -1) {
    return { success: false, message: 'Colunas Geo_Id ou Data_Solicitacao não encontradas.' };
  }

  const newValues = [];
  for (let i = 1; i < pipelineRows.length; i++) {
    const geoId   = String(pipelineRows[i][cGeo] || '').trim();
    const existing = pipelineRows[i][cDataSol];
    if (!existing && mapaOrigem.has(geoId)) {
      const ts = mapaOrigem.get(geoId);
      newValues.push([ts instanceof Date ? ts.toISOString() : String(ts || '')]);
    } else {
      newValues.push([existing !== undefined && existing !== null ? existing : '']);
    }
  }

  await batchWriteRanges(SPREADSHEET_ID, [{
    range: `${NOME_ABA_PIPELINE}!${colToLetter(cDataSol+1)}2:${colToLetter(cDataSol+1)}${pipelineRows.length}`,
    values: newValues
  }]);

  return { success: true, message: `${pipelineRows.length - 1} linhas processadas.` };
}

// ─── sincronizarAceitesPorGeoId ───────────────────────────────────────────────
// Porta de sincronizarAceitesPorGeoId() do Apps Script.
const COLUNAS_PROTEGIDAS_SYNC = new Set([
  'SLA', 'Status_SLA', 'Semana Solicitação', 'Prioridade',
  'Ativação W-1', 'Ativação W0', 'Status ID', 'ACEITES',
  'ACEITE_PIPE', 'Check', 'STATUS_SBO', 'Status Go Live',
  'DATA ATIVAÇÃO', 'SEMANA ATIVAÇÃO'
]);

async function sincronizarAceitesPorGeoId() {
  const [aceitesRows, pipelineRows, propostasRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES),
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS)
  ]);

  const aHeaders    = aceitesRows[0] || [];
  const aGeoIdx     = aHeaders.indexOf('Geo_Id');
  const aStatusIdx  = aHeaders.indexOf('Status_prospeccao');
  if (aGeoIdx === -1 || aStatusIdx === -1) {
    return { success: false, message: 'Colunas essenciais não encontradas em Aceites.' };
  }

  let totalAtualizacoes = 0;
  const sheetsClient = await getSheetsClient();

  async function syncParaAba(destRows, nomeAba) {
    if (!destRows || destRows.length < 2) return 0;
    const dHeaders = destRows[0];
    const dGeoIdx  = dHeaders.indexOf('Geo_Id');
    if (dGeoIdx === -1) return 0;

    const cabB = dHeaders[1] ? String(dHeaders[1]).trim() : null;
    const cabC = dHeaders[2] ? String(dHeaders[2]).trim() : null;

    const updates = [];
    for (let i = 1; i < aceitesRows.length; i++) {
      const linha   = aceitesRows[i];
      const geoId   = String(linha[aGeoIdx] || '').trim();
      const status  = String(linha[aStatusIdx] || '').trim().toLowerCase();
      if (!geoId || !['aceitou', 'ativo'].includes(status)) continue;

      for (let j = 1; j < destRows.length; j++) {
        if (String(destRows[j][dGeoIdx] || '').trim() !== geoId) continue;
        // Encontrou linha correspondente
        for (let c = 0; c < aHeaders.length; c++) {
          const nomeCol = String(aHeaders[c]).trim();
          const dIdx    = dHeaders.indexOf(nomeCol);
          if (dIdx === -1 || nomeCol === 'Geo_Id') continue;
          if (COLUNAS_PROTEGIDAS_SYNC.has(nomeCol)) continue;
          if (nomeAba === NOME_ABA_PIPELINE) {
            if (nomeCol === cabB || nomeCol === cabC) continue;
            if (dIdx >= 34) continue; // AI+ são fórmulas
          }
          const valorOrigem  = linha[c];
          const valorDestino = destRows[j][dIdx];
          const vo = String(valorOrigem  === null || valorOrigem  === undefined ? '' : valorOrigem).trim();
          const vd = String(valorDestino === null || valorDestino === undefined ? '' : valorDestino).trim();
          if (vo !== vd) {
            updates.push({
              range: `${nomeAba}!${colToLetter(dIdx+1)}${j+1}`,
              values: [[valorOrigem === null || valorOrigem === undefined ? '' : valorOrigem]]
            });
            totalAtualizacoes++;
          }
        }
        break;
      }
    }

    // Batch write
    if (updates.length > 0) {
      const CHUNK = 100;
      for (let s = 0; s < updates.length; s += CHUNK) {
        await sheetsClient.spreadsheets.values.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
          requestBody: { valueInputOption: 'RAW', data: updates.slice(s, s+CHUNK) }
        });
      }
    }
    return updates.length;
  }

  await syncParaAba(pipelineRows, NOME_ABA_PIPELINE);
  await syncParaAba(propostasRows, NOME_ABA_MINHAS_PROPOSTAS);

  return { success: true, message: `${totalAtualizacoes} atualizações realizadas.` };
}

// ─── atualizarTodasDatasPendentes ─────────────────────────────────────────────
// Porta de atualizarTodasDatasPendentes() do Apps Script.
// Preenche Cidade/Estado (geocoding), Data_Logistics, Data_MP, Data_Treinamento.
async function atualizarTodasDatasPendentes() {
  const fetch = require('node-fetch');
  const MAPS_KEY = process.env.GOOGLE_MAPS_API_KEY || 'AIzaSyAsNE_w2gWedPwRvkQ2DkaRZ_8cNQaaaWU';

  const [pipelineRows, aceitesRows, propostasRows, logisticsRows, sbo1Rows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS),
    readSheet(SPREADSHEET_ID, 'Logistics').catch(() => []),
    readSheet(SPREADSHEET_ID, 'Extração SBO1').catch(() => [])
  ]);

  // Mapa Logistics: place_id → approval_date (status = APROVADO)
  const logMap = new Map();
  if (logisticsRows.length > 1) {
    const lHeaders  = logisticsRows[0];
    const lPlaceIdx = lHeaders.indexOf('PLC_PLACE_ID');
    const lStatusIdx= lHeaders.indexOf('APPROVAL_STATUS');
    const lDateIdx  = lHeaders.indexOf('OPS_APPROVAL_DATE');
    for (let i = 1; i < logisticsRows.length; i++) {
      const pid = String(logisticsRows[i][lPlaceIdx === -1 ? 0 : lPlaceIdx] || '').trim();
      const st  = String(logisticsRows[i][lStatusIdx === -1 ? 1 : lStatusIdx] || '').trim().toUpperCase();
      const dt  = logisticsRows[i][lDateIdx === -1 ? 2 : lDateIdx];
      if (pid && st === 'APROVADO' && dt) logMap.set(pid, dt);
    }
  }

  // Mapa SBO1: SHP_AGENCY_ID (col A) → SHP_AGEN_ACTIVE_DT
  const sbo1Map = new Map();
  if (sbo1Rows.length > 1) {
    const sHeaders    = sbo1Rows[0];
    const sAgencyIdx  = sHeaders.indexOf('SHP_AGENCY_ID');
    const sActiveDtIdx= sHeaders.indexOf('SHP_AGEN_ACTIVE_DT');
    for (let i = 1; i < sbo1Rows.length; i++) {
      const agId = String(sbo1Rows[i][sAgencyIdx === -1 ? 0 : sAgencyIdx] || '').trim();
      const dt   = sbo1Rows[i][sActiveDtIdx === -1 ? 3 : sActiveDtIdx];
      if (agId && dt) sbo1Map.set(agId, dt);
    }
  }

  const geoCache = {};
  async function reverseGeocode(lat, lng) {
    const key = `${Number(lat).toFixed(6)},${Number(lng).toFixed(6)}`;
    if (geoCache[key]) return geoCache[key];
    try {
      const url = `https://maps.googleapis.com/maps/api/geocode/json?latlng=${encodeURIComponent(key)}&language=pt-BR&key=${MAPS_KEY}`;
      const resp = await fetch(url, { timeout: 5000 });
      const data = await resp.json();
      if (!data || !data.results || !data.results.length) {
        geoCache[key] = { city: '', state: '' };
        return geoCache[key];
      }
      let city = '', state = '';
      const getComp = (comps, type) => {
        const c = comps.find(x => x.types && x.types.includes(type));
        return c ? c.long_name : null;
      };
      for (const r of data.results) {
        const comps = r.address_components || [];
        if (!state) state = getComp(comps, 'administrative_area_level_1') || '';
        if (!city) {
          for (const t of ['locality','administrative_area_level_2','postal_town','sublocality_level_1']) {
            const v = getComp(comps, t);
            if (v) { city = v; break; }
          }
        }
        if (city && state) break;
      }
      geoCache[key] = { city, state };
      return geoCache[key];
    } catch (e) {
      geoCache[key] = { city: '', state: '' };
      return geoCache[key];
    }
  }

  // ── Pipeline: Cidade, Estado, Data_Logistics ──────────────────────────────
  if (pipelineRows.length > 1) {
    const ph      = pipelineRows[0];
    const cGeo    = getColIndex(ph, 'Geo_Id');
    const cLat    = getColIndex(ph, 'Lat_geo');
    const cLon    = getColIndex(ph, 'Long_geo');
    const cCid    = getColIndex(ph, 'Cidade');
    const cEst    = getColIndex(ph, 'Estado');
    const cPlId   = getColIndex(ph, 'Place_id');
    const cTipo   = getColIndex(ph, 'Tipo');
    const cDataLg = getColIndex(ph, 'Data_Logistics');

    const updCidade = [], updEstado = [], updDataLog = [];

    for (let i = 1; i < pipelineRows.length; i++) {
      const row  = pipelineRows[i];
      const lat  = parseFloat(String(row[cLat] || '').replace(',', '.'));
      const lon  = parseFloat(String(row[cLon] || '').replace(',', '.'));
      const cid  = String(row[cCid] || '').trim();
      const est  = String(row[cEst] || '').trim();

      let newCid = cid, newEst = est;
      if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
        if (!cid || !est) {
          const geo = await reverseGeocode(lat, lon);
          if (geo.city) newCid = geo.city;
          if (geo.state) newEst = geo.state;
          await new Promise(r => setTimeout(r, 80)); // rate limit
        }
      }
      updCidade.push([newCid]);
      updEstado.push([newEst]);

      // Data_Logistics
      const placeId = String(row[cPlId] || '').trim();
      const tipo    = String(row[cTipo] || '').trim().toUpperCase();
      if (cDataLg !== -1 && placeId && (tipo === 'DC' || tipo === 'DC|PU') && logMap.has(placeId)) {
        const d = logMap.get(placeId);
        updDataLog.push([d instanceof Date ? d.toISOString().substring(0, 10) : String(d)]);
      } else {
        updDataLog.push([cDataLg !== -1 ? (row[cDataLg] || '') : '']);
      }
    }

    const n = pipelineRows.length;
    const dataToWrite = [];
    if (cCid !== -1)  dataToWrite.push({ range: `${NOME_ABA_PIPELINE}!${colToLetter(cCid+1)}2:${colToLetter(cCid+1)}${n}`, values: updCidade });
    if (cEst !== -1)  dataToWrite.push({ range: `${NOME_ABA_PIPELINE}!${colToLetter(cEst+1)}2:${colToLetter(cEst+1)}${n}`, values: updEstado });
    if (cDataLg !== -1) dataToWrite.push({ range: `${NOME_ABA_PIPELINE}!${colToLetter(cDataLg+1)}2:${colToLetter(cDataLg+1)}${n}`, values: updDataLog });
    if (dataToWrite.length) await batchWriteRanges(SPREADSHEET_ID, dataToWrite);
  }

  // ── Aceites e Minhas Propostas: Data_MP, Data_Treinamento, Data_Logistics ──
  for (const [nomAba, abaRows] of [[NOME_ABA_ACEITES, aceitesRows], [NOME_ABA_MINHAS_PROPOSTAS, propostasRows]]) {
    if (abaRows.length < 2) continue;
    const ah     = abaRows[0];
    const cPlId  = getColIndex(ah, 'Place_id');
    const cDataMP= getColIndex(ah, 'Data_MP');
    const cDataTr= getColIndex(ah, 'Data_Treinamento');
    const cDataLg= getColIndex(ah, 'Data_Logistics');

    const updMP = [], updTr = [], updLg = [];
    for (let i = 1; i < abaRows.length; i++) {
      const row    = abaRows[i];
      const plId   = String(row[cPlId === -1 ? 10 : cPlId] || '').trim();
      const exMP   = cDataMP !== -1 ? row[cDataMP] : '';
      const exTr   = cDataTr !== -1 ? row[cDataTr] : '';
      const exLg   = cDataLg !== -1 ? row[cDataLg] : '';

      const activeDt = plId ? sbo1Map.get(plId) : null;
      const fmt = v => (v instanceof Date ? v.toISOString().substring(0, 10) : String(v || ''));

      updMP.push([(!exMP && activeDt) ? fmt(activeDt) : (exMP || '')]);
      updTr.push([(!exTr && activeDt) ? fmt(activeDt) : (exTr || '')]);

      if (!exLg && plId && logMap.has(plId)) {
        updLg.push([fmt(logMap.get(plId))]);
      } else {
        updLg.push([exLg || '']);
      }
    }

    const n = abaRows.length;
    const dataToWrite = [];
    if (cDataMP !== -1) dataToWrite.push({ range: `${nomAba}!${colToLetter(cDataMP+1)}2:${colToLetter(cDataMP+1)}${n}`, values: updMP });
    if (cDataTr !== -1) dataToWrite.push({ range: `${nomAba}!${colToLetter(cDataTr+1)}2:${colToLetter(cDataTr+1)}${n}`, values: updTr });
    if (cDataLg !== -1) dataToWrite.push({ range: `${nomAba}!${colToLetter(cDataLg+1)}2:${colToLetter(cDataLg+1)}${n}`, values: updLg });
    if (dataToWrite.length) await batchWriteRanges(SPREADSHEET_ID, dataToWrite);
  }

  return { success: true, message: 'Datas pendentes atualizadas.' };
}

// ─── pipelineJobHourly ────────────────────────────────────────────────────────
// Porta de pipelineJobHourly() do Apps Script.
// Preenche CEP (col BI) e Bairro (col BJ) via reverse geocoding, em lotes.
async function pipelineJobHourly() {
  const fetch  = require('node-fetch');
  const MAPS_KEY  = process.env.GOOGLE_MAPS_API_KEY || 'AIzaSyAsNE_w2gWedPwRvkQ2DkaRZ_8cNQaaaWU';
  const BATCH_SIZE = 800;
  const SLEEP_MS   = 80;
  const COL_CEP    = 61; // BI (1-based)
  const COL_BAIRRO = 62; // BJ (1-based)

  const rows = await readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE);
  if (rows.length < 2) return { success: false, message: 'Pipeline vazio.' };

  const headers   = rows[0];
  const cLat      = getColIndex(headers, 'Lat_geo');
  const cLon      = getColIndex(headers, 'Long_geo');
  if (cLat === -1 || cLon === -1) return { success: false, message: 'Colunas Lat_geo/Long_geo não encontradas.' };

  let updated = 0;
  const updCep = [], updBairro = [];

  for (let i = 1; i < rows.length && updated < BATCH_SIZE; i++) {
    const lat = parseFloat(String(rows[i][cLat] || '').replace(',', '.'));
    const lon = parseFloat(String(rows[i][cLon] || '').replace(',', '.'));
    const cepAtual    = String(rows[i][COL_CEP - 1] || '').trim();
    const bairroAtual = String(rows[i][COL_BAIRRO - 1] || '').trim();

    if (!cepAtual || !bairroAtual) {
      if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90) {
        try {
          const key = `${lat.toFixed(6)},${lon.toFixed(6)}`;
          const url = `https://maps.googleapis.com/maps/api/geocode/json?latlng=${encodeURIComponent(key)}&language=pt-BR&key=${MAPS_KEY}`;
          const resp = await fetch(url, { timeout: 5000 });
          const data = await resp.json();
          let cep = '', bairro = '';
          if (data && data.results) {
            for (const r of data.results) {
              for (const c of (r.address_components || [])) {
                if (!cep && c.types.includes('postal_code')) cep = c.long_name || '';
                if (!bairro) {
                  if (c.types.includes('neighborhood')) bairro = c.long_name || '';
                  else if (c.types.includes('sublocality_level_1')) bairro = c.long_name || '';
                  else if (c.types.includes('sublocality')) bairro = c.long_name || '';
                }
              }
              if (cep && bairro) break;
            }
          }
          updCep.push({ row: i + 1, value: cep || cepAtual });
          updBairro.push({ row: i + 1, value: bairro || bairroAtual });
          updated++;
          await new Promise(r => setTimeout(r, SLEEP_MS));
        } catch (_) {
          updCep.push({ row: i + 1, value: cepAtual });
          updBairro.push({ row: i + 1, value: bairroAtual });
        }
      }
    }
  }

  if (updCep.length > 0) {
    const sheetsClient = await getSheetsClient();
    const dataArr = [
      ...updCep.map(u => ({ range: `${NOME_ABA_PIPELINE}!${colToLetter(COL_CEP)}${u.row}`, values: [[u.value]] })),
      ...updBairro.map(u => ({ range: `${NOME_ABA_PIPELINE}!${colToLetter(COL_BAIRRO)}${u.row}`, values: [[u.value]] }))
    ];
    for (let s = 0; s < dataArr.length; s += 100) {
      await sheetsClient.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: 'RAW', data: dataArr.slice(s, s+100) }
      });
    }
  }

  return { success: true, message: `${updated} linhas com CEP/Bairro atualizadas.` };
}

// ─── atualizarFinalSemana ─────────────────────────────────────────────────────
// Porta de atualizarFinalSemana() do Apps Script.
// Preenche Pipeline col BK (índice 62, 0-based) com info de funcionamento nos sábados.
async function atualizarFinalSemana() {
  const [pipelineRows, sbo1Rows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, 'Extração SBO1').catch(() => [])
  ]);
  if (pipelineRows.length < 2) return { success: false, message: 'Pipeline vazio.' };

  // Mapa SBO1: agency_id → hours_display
  const sboMap = new Map();
  if (sbo1Rows.length > 1) {
    const sh = sbo1Rows[0];
    const sAgIdx = sh.indexOf('SHP_AGENCY_ID');
    const sHrIdx = sh.findIndex(h => String(h || '').trim().toUpperCase().normalize('NFD').replace(/[̀-ͯ]/g, '') === 'SHP_AGEN_HOURS_DISPLAY');
    if (sAgIdx !== -1 && sHrIdx !== -1) {
      for (let i = 1; i < sbo1Rows.length; i++) {
        const agId = String(sbo1Rows[i][sAgIdx] || '').trim();
        const hrs  = String(sbo1Rows[i][sHrIdx] || '').trim();
        if (agId && (!sboMap.has(agId) || (sboMap.get(agId) === '' && hrs !== ''))) {
          sboMap.set(agId, hrs);
        }
      }
    }
  }

  const ph       = pipelineRows[0];
  const cStatus  = getColIndex(ph, 'Status_prospeccao');
  const cPlaceId = getColIndex(ph, 'Place_id');
  const COL_BK   = 63; // column BK (1-based)

  function extractSaturdayInfo(hoursDisplay) {
    if (!hoursDisplay) return null;
    const txt = String(hoursDisplay).toLowerCase()
      .normalize('NFD').replace(/[̀-ͯ]/g, '')
      .replace(/ /g, ' ').replace(/\s+/g, ' ').trim();
    const sabRe = /\bsabado?s?\b/g;
    const periods = [];
    let m;
    while ((m = sabRe.exec(txt)) !== null) {
      const window = txt.substring(m.index + m[0].length, m.index + m[0].length + 120);
      const p = window.replace(/\bas\b|\bàs\b|\bà\b/g, ' a ').replace(/[–—]/g, '-');
      const re = /(\d{1,2})(?:[:h](\d{2}))?\s*(?:h|hs)?\s*(?:a|-)\s*(\d{1,2})(?:[:h](\d{2}))?\s*(?:h|hs)?/g;
      let rm;
      while ((rm = re.exec(p)) !== null) {
        const h1 = rm[1].padStart(2,'0'), m1 = rm[2] || '00';
        const h2 = rm[3].padStart(2,'0'), m2 = rm[4] || '00';
        const t1 = m1 === '00' ? `${h1}h` : `${h1}h${m1}`;
        const t2 = m2 === '00' ? `${h2}h` : `${h2}h${m2}`;
        periods.push(`${t1} às ${t2}`);
      }
    }
    if (periods.length) return 'Aberto sábado. ' + [...new Set(periods)].join(' | ');
    if (/\bsabado?s?\b/.test(txt)) return 'Aberto sábado. (horário não identificado)';
    return null;
  }

  const output = [];
  for (let i = 1; i < pipelineRows.length; i++) {
    const row    = pipelineRows[i];
    const status = String(row[cStatus === -1 ? 9 : cStatus] || '').trim().toLowerCase();
    const plId   = String(row[cPlaceId === -1 ? 10 : cPlaceId] || '').trim();
    const oldVal = row[COL_BK - 1] !== undefined ? row[COL_BK - 1] : '';

    if (!['ativo','aceitou'].includes(status)) { output.push([oldVal]); continue; }
    if (!plId) { output.push(['Sem Place_id']); continue; }
    const hrs = sboMap.get(plId);
    if (!hrs) { output.push(['Sem dados no SBO']); continue; }
    const info = extractSaturdayInfo(hrs);
    output.push([info !== null ? info : 'Não abre aos sábados']);
  }

  const colLetter = colToLetter(COL_BK);
  await batchWriteRanges(SPREADSHEET_ID, [{
    range: `${NOME_ABA_PIPELINE}!${colLetter}2:${colLetter}${pipelineRows.length}`,
    values: output
  }]);

  return { success: true, message: `${output.length} linhas de sábado atualizadas.` };
}

// ─── executarSincronizacaoBigQuery ────────────────────────────────────────────
// Wrapper que lê as 3 abas e chama syncSheetsToBigQuery.
async function executarSincronizacaoBigQuery() {
  const bq = require('./bigquery');
  const [pipelineRows, propostasRows, aceitesRows] = await Promise.all([
    readSheet(SPREADSHEET_ID, NOME_ABA_PIPELINE),
    readSheet(SPREADSHEET_ID, NOME_ABA_MINHAS_PROPOSTAS),
    readSheet(SPREADSHEET_ID, NOME_ABA_ACEITES)
  ]);

  const sources = [
    { sheetName: 'Pipeline',         headers: pipelineRows[0] || [],  rows: pipelineRows.slice(1),   primaryKeyField: 'GEO_ID',   tableId: 'BT_HUNTING_PIPELINE_MLB' },
    { sheetName: 'Minhas Propostas', headers: propostasRows[0] || [], rows: propostasRows.slice(1),  primaryKeyField: 'PLACE_ID', tableId: 'BT_PIPELINE_HUNTING_PROPOSTAS' },
    { sheetName: 'Aceites',          headers: aceitesRows[0] || [],   rows: aceitesRows.slice(1),    primaryKeyField: 'PLACE_ID', tableId: 'BT_PIPELINE_HUNTING_ACEITES' }
  ].filter(s => s.headers.length > 0);

  return bq.syncSheetsToBigQuery(sources);
}

module.exports = {
  getAppData,
  getServicoOriginal,
  generateUniquePlaceId,
  checkForOverdueAccepts,
  updateLeadIdAndDismissNotification,
  atualizarCamposSimples,
  updateProspect,
  addColdLead,
  createOrUpdateProposalFromLead,
  deleteRowFromSheet,
  editarRegistro,
  isUserAdmin,
  cacheClear,
  escreverDadosGoLive,
  escreverDadosBQParaAba,
  escreverDadosLogistics,
  escreverDadosMercadopago,
  escreverDadosNewPlace,
  escreverDadosTreinamento,
  escreverDadosVolumetria,
  escreverDadosSBO,
  contarContatosPorGeoID,
  preencherSVCMaisProximo,
  preencherMultiplasAbas,
  preencherDataLogistics,
  updateGoLiveDates,
  syncDataPipeline,
  sincronizarAceitesPorGeoId,
  atualizarTodasDatasPendentes,
  pipelineJobHourly,
  atualizarFinalSemana,
  executarSincronizacaoBigQuery
};
