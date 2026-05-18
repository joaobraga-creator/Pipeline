/**
 * bigquery.js — Serviço BigQuery
 * Porta das funções obterLeadsQuentesProximos e searchPlaceIdsInBigQuery
 *
 * IMPORTANTE: As queries SQL abaixo são baseadas na estrutura esperada das
 * tabelas. Você precisará ajustá-las para corresponder às suas tabelas reais.
 */
const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');

const bqOpts = { projectId: process.env.BIGQUERY_PROJECT_ID };

// Tenta: service_account.json → credencial explícita → adc_credentials.json → ADC do sistema
const candidates = [
  path.join(__dirname, '..', 'service_account.json'),
  process.env.GOOGLE_APPLICATION_CREDENTIALS,
  path.join(__dirname, '..', 'adc_credentials.json')
];
for (const f of candidates) {
  if (f && fs.existsSync(f)) { bqOpts.keyFilename = f; break; }
}

const bq = new BigQuery(bqOpts);

const DATASET = process.env.BIGQUERY_DATASET;
const TABLE_LEADS = process.env.BIGQUERY_TABLE_LEADS;

// Treinamento fica em projeto/dataset separado
const TREINAMENTO_PROJECT = process.env.BIGQUERY_TREINAMENTO_PROJECT || 'meli-sbox';
const TREINAMENTO_DATASET = process.env.BIGQUERY_TREINAMENTO_DATASET || 'EXEOPRTRAINING';
const TREINAMENTO_TABLE   = process.env.BIGQUERY_TREINAMENTO_TABLE   || 'PLACE_FACT_FIX_BASE_PLACES';

// Colunas retornadas pelo query (aliases do BigQuery)
const COLUNAS_LEADS_FRONTEND = [
  'ID_PROSPEC', 'NOMBRE_ESTABLECIMIENTO', 'TELEFONE', 'PAIS',
  'LATITUDE', 'LONGITUDE', 'DATA_CADASTRO', 'SHP_PROSP_REG_EMAIL_DESC',
  'Tipo_Lead', 'DISTANCE_KM', 'SHP_PROSP_REG_RESP_NAME', 'Status_Contato',
  'CNPJ', 'ENDERECO_COMPLETO'
];

/**
 * Busca leads próximos às coordenadas informadas (aba Leads / BigQuery).
 */
async function obterLeadsQuentesProximos(coordsData) {
  const { lat, lon } = coordsData;

  if (!lat || !lon) {
    return { success: false, message: 'Latitude e Longitude são obrigatórias.' };
  }

  const latNum = parseFloat(lat);
  const lonNum = parseFloat(lon);

  if (isNaN(latNum) || isNaN(lonNum)) {
    return { success: false, message: 'Coordenadas inválidas.' };
  }

  try {
    const query = `
      WITH
        Ref AS (
          SELECT ST_GEOGPOINT(@refLon, @refLat) AS ref_point
        ),
        LeadBase AS (
          SELECT
            leads.ID_PROSPEC,
            leads.NOMBRE_ESTABLECIMIENTO,
            leads.TELEFONO1,
            leads.PAIS,
            leads.LEAD AS Tipo_Lead,
            leads.AUD_INS_DTTM,
            SAFE_CAST(leads.LATITUD AS FLOAT64) AS LATITUDE,
            SAFE_CAST(leads.LONGITUD AS FLOAT64) AS LONGITUDE,
            CASE
              WHEN SAFE_CAST(leads.LATITUD AS FLOAT64) BETWEEN -90 AND 90
               AND SAFE_CAST(leads.LONGITUD AS FLOAT64) BETWEEN -180 AND 180
              THEN ST_GEOGPOINT(SAFE_CAST(leads.LONGITUD AS FLOAT64), SAFE_CAST(leads.LATITUD AS FLOAT64))
            END AS LEAD_POINT
          FROM \`${DATASET}.${TABLE_LEADS}\` leads
          WHERE
            leads.PAIS = 'BRASIL'
            AND leads.TELEFONO1 IS NOT NULL
            AND SAFE_CAST(leads.LATITUD AS FLOAT64) IS NOT NULL
            AND SAFE_CAST(leads.LONGITUD AS FLOAT64) IS NOT NULL
            AND SAFE_CAST(leads.LATITUD AS FLOAT64) BETWEEN -90 AND 90
            AND SAFE_CAST(leads.LONGITUD AS FLOAT64) BETWEEN -180 AND 180
        ),
        NearbyLeads AS (
          SELECT
            CAST(lb.ID_PROSPEC AS STRING) AS ID_PROSPEC,
            COALESCE(hot.SHP_PROSP_FANTASY_NAME, lb.NOMBRE_ESTABLECIMIENTO) AS NOMBRE_ESTABLECIMIENTO,
            hot.SHP_PROSP_REG_RESP_NAME,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lb.TELEFONO1, '-', ''), ' ', ''), '(', ''), ')', ''), '+', '') AS TELEFONE,
            lb.PAIS,
            lb.LATITUDE,
            lb.LONGITUDE,
            FORMAT_DATETIME('%d/%m/%Y', hot.SHP_UPDATED_DATETIME) AS DATA_CADASTRO_HOT,
            FORMAT_DATE('%d/%m/%Y', DATE(lb.AUD_INS_DTTM)) AS DATA_CADASTRO_COLD,
            hot.SHP_PROSP_REG_EMAIL_DESC,
            hot.SHP_PROSP_FISCAL_NUMBER_COD AS CNPJ,
            hot.SHP_PROSP_ADDRESS_ZIP_CODE_NO AS ENDERECO_COMPLETO,
            lb.Tipo_Lead,
            (ST_DISTANCE(lb.LEAD_POINT, ref.ref_point) / 1000.0) AS DISTANCE_KM
          FROM LeadBase lb
          CROSS JOIN Ref ref
          LEFT JOIN \`${DATASET}.LK_SHP_PROSPECT_KANGU\` hot
            ON REGEXP_REPLACE(CAST(lb.ID_PROSPEC AS STRING), r'[^0-9]', '')
             = REGEXP_REPLACE(CAST(hot.SHP_PROSP_ID AS STRING), r'[^0-9]', '')
            AND hot.SIT_SITE_ID = 'MLB'
          WHERE ST_DWITHIN(lb.LEAD_POINT, ref.ref_point, @maxDistanceMeters)
        )
      SELECT
        nl.ID_PROSPEC,
        nl.NOMBRE_ESTABLECIMIENTO,
        nl.SHP_PROSP_REG_RESP_NAME,
        nl.TELEFONE,
        nl.PAIS,
        nl.LATITUDE,
        nl.LONGITUDE,
        COALESCE(nl.DATA_CADASTRO_HOT, nl.DATA_CADASTRO_COLD) AS DATA_CADASTRO,
        nl.SHP_PROSP_REG_EMAIL_DESC,
        nl.CNPJ,
        nl.ENDERECO_COMPLETO,
        nl.Tipo_Lead,
        CAST(NULL AS STRING) AS Status_Contato,
        nl.DISTANCE_KM
      FROM NearbyLeads nl
      ORDER BY Tipo_Lead DESC, DISTANCE_KM ASC
      LIMIT 100
    `;

    const options = {
      query,
      params: {
        refLat: latNum,
        refLon: lonNum,
        maxDistanceMeters: 3000
      },
      types: {
        refLat: 'FLOAT64',
        refLon: 'FLOAT64',
        maxDistanceMeters: 'INT64'
      },
      location: 'US'
    };

    const [rows] = await bq.query(options);

    if (!rows || rows.length === 0) {
      return { success: true, data: [], headers: COLUNAS_LEADS_FRONTEND, message: 'Nenhum lead encontrado.' };
    }

    const data = rows.map(row => COLUNAS_LEADS_FRONTEND.map(col => {
      const val = row[col];
      if (val === null || val === undefined) return '';
      if (typeof val === 'object' && val.value !== undefined) return val.value;
      return val;
    }));

    return {
      success: true,
      data,
      headers: COLUNAS_LEADS_FRONTEND,
      totalCount: rows.length
    };

  } catch (e) {
    console.error('[BigQuery] Erro em obterLeadsQuentesProximos:', e.message);
    return { success: false, message: 'Erro ao consultar BigQuery: ' + e.message };
  }
}

/**
 * Busca Place IDs no BigQuery por nome/ID parcial.
 * Porta de searchPlaceIdsInBigQuery do Code.gs.
 */
async function searchPlaceIdsInBigQuery(searchTerm) {
  const term = String(searchTerm || '').trim();
  if (!term) return { success: false, message: 'Termo de busca vazio.', data: [] };

  try {
    const query = `
      SELECT
        CAST(ID_PROSPEC AS STRING) AS ID_PROSPEC,
        NOMBRE_ESTABLECIMIENTO,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TELEFONO1, '-', ''), ' ', ''), '(', ''), ')', ''), '+', '') AS TELEFONE,
        SAFE_CAST(LATITUD AS FLOAT64) AS LATITUDE,
        SAFE_CAST(LONGITUD AS FLOAT64) AS LONGITUDE
      FROM \`${DATASET}.${TABLE_LEADS}\`
      WHERE PAIS = 'BRASIL'
        AND LOWER(NOMBRE_ESTABLECIMIENTO) LIKE LOWER(CONCAT('%', @term, '%'))
      LIMIT 25
    `;

    const [rows] = await bq.query({
      query,
      params: { term },
      location: 'US'
    });

    return { success: true, data: rows || [] };

  } catch (e) {
    console.error('[BigQuery] Erro em searchPlaceIdsInBigQuery:', e.message);
    return { success: false, message: 'Erro ao buscar: ' + e.message, data: [] };
  }
}

/**
 * Busca informações de um Place pelo ID.
 * Porta de getPlaceInfoFromLeads do Code.gs.
 */
async function getPlaceInfoFromLeads(placeId) {
  if (!placeId) return null;

  // Remove hifens para busca (IDs no BQ não têm hifens)
  const cleanedId = String(placeId).replace(/-/g, '').trim();
  if (!cleanedId) return null;

  try {
    const query = `
      SELECT
        CAST(ID_PROSPEC AS STRING) AS ID_PROSPEC,
        NOMBRE_ESTABLECIMIENTO,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TELEFONO1, '-', ''), ' ', ''), '(', ''), ')', ''), '+', '') AS TELEFONE,
        SAFE_CAST(LATITUD AS FLOAT64) AS LATITUDE,
        SAFE_CAST(LONGITUD AS FLOAT64) AS LONGITUDE,
        LEAD AS Tipo_Lead
      FROM \`${DATASET}.${TABLE_LEADS}\`
      WHERE PAIS = 'BRASIL'
        AND ID_PROSPEC = @placeIdParam
      LIMIT 1
    `;

    const [rows] = await bq.query({
      query,
      params: { placeIdParam: cleanedId },
      location: 'US'
    });

    if (!rows || rows.length === 0) return null;
    return {
      placeName: rows[0].NOMBRE_ESTABLECIMIENTO || '',
      telefonePlace: rows[0].TELEFONE || '',
      latitude: String(rows[0].LATITUDE || ''),
      longitude: String(rows[0].LONGITUDE || ''),
      tipo: rows[0].Tipo_Lead || ''
    };

  } catch (e) {
    console.error('[BigQuery] Erro em getPlaceInfoFromLeads:', e.message);
    return null;
  }
}

/**
 * Consulta treinamento por Place_id.
 * Porta de getTreinamentoByPlaceId do Code.gs.
 */
async function getTreinamentoByPlaceId(placeId) {
  if (!placeId) return null;

  try {
    const query = `
      SELECT DISTINCT
        AGENCIE_ID AS Place_id,
        REGEXP_REPLACE(SHP_AGEN_DESC, r'.*\\Agncia Mercado Livre - ', '') AS Place_name,
        DATE(COMPLETION_DATE) AS Data_Avaliacao,
        EXAM_GRADE AS QD_ACERTO
      FROM \`${TREINAMENTO_PROJECT}.${TREINAMENTO_DATASET}.${TREINAMENTO_TABLE}\`
      WHERE SITE = 'MLB'
        AND EXAM_GRADE IS NOT NULL
        AND AGENCIE_ID IS NOT NULL
        AND SERVICO NOT IN ('INSTALACAO')
        AND UPPER(TRIM(AGENCIE_ID)) = UPPER(TRIM(@placeId))
      ORDER BY Data_Avaliacao DESC
      LIMIT 1
    `;

    const [rows] = await bq.query({
      query,
      params: { placeId: String(placeId).trim() }
    });

    if (!rows || rows.length === 0) return null;

    const row = rows[0];
    return {
      place_id: row.Place_id || '',
      place_name: row.Place_name || '',
      data_avaliacao: row.Data_Avaliacao ? String(row.Data_Avaliacao.value || row.Data_Avaliacao) : '',
      qd_acerto: row.QD_ACERTO || ''
    };

  } catch (e) {
    console.error('[BigQuery] Erro em getTreinamentoByPlaceId:', e.message);
    return null;
  }
}

/**
 * Consulta BT_GO_LIVE no projeto meli-bi-data e retorna todas as linhas.
 * Porta de dadosGoLive() do Apps Script.
 */
async function dadosGoLive() {
  const bqGoLive = new BigQuery({ ...bqOpts, projectId: 'meli-bi-data' });

  const query = `
    SELECT
      INCLUSION_DATE,
      DUPLICATE,
      PLC_PLACE_ID,
      PLC_TYPE_ACTIVATION,
      PLC_SERVICE,
      PLC_ACTIVATION_REASON,
      PLC_ENTRIES,
      PLC_CITY,
      PLC_STATE,
      GO_LIVE_DATE,
      TICKET_DATE,
      APROVATION,
      AUD_FROM_INTERFACE,
      AUD_INS_DTTM,
      AUD_UPD_DTTM,
      AUD_TRANSACTION_ID
    FROM \`meli-bi-data.WHOWNER.BT_GO_LIVE\`
    WHERE GO_LIVE_DATE >= "2026-01-01"
  `;

  const [rows] = await bqGoLive.query({ query });
  return rows || [];
}

// ─── Helper genérico para queries meli-bi-data ────────────────────────────────
async function runBQQuery(projectId, query) {
  const client = new BigQuery({ ...bqOpts, projectId });
  const [rows] = await client.query({ query, location: 'US' });
  return rows || [];
}

/** Porta de dadosLogistics() → aba "Logistics" */
async function dadosLogistics() {
  return runBQQuery('meli-bi-data', `
    SELECT
      PLC_PLACE_ID,
      APPROVAL_STATUS,
      CONCLUSION_LOGISTICS_DATE AS OPS_APPROVAL_DATE
    FROM \`meli-bi-data.WHOWNER.BT_FUP_DC\`
  `);
}

/** Porta de dadosMercadopago() → aba "Mercado Pago" */
async function dadosMercadopago() {
  return runBQQuery('meli-bi-data', `
    SELECT ID, NOME, ID_BANCO, CONTA_BANCO_VAL, DT_ALT
    FROM \`meli-bi-data.WHOWNER.BT_VALIDACAO_BANCARIA_DATA\`
    WHERE CONTA_BANCO_VAL IN ('V','I','P')
  `);
}

/** Porta de dadosNewPlace() → aba "NEW_PLACE" */
async function dadosNewPlace() {
  return runBQQuery('meli-bi-data', `
    SELECT DISTINCT
      LEAD_ID,
      PLACE_ID,
      LEAD_STATUS,
      ACTIVATION_DATE AS Go_Live
    FROM \`meli-bi-data.WHOWNER.LK_PLACES_ORDERS_REG\`
    WHERE SIT_SITE_ID = 'MLB'
      AND PLACE_ID IS NOT NULL
      AND LEAD_STATUS NOT IN ('REJECTED')
  `);
}

/** Porta de treinamentoB() → aba "Treinamento" */
async function treinamentoB() {
  const client = new BigQuery({ ...bqOpts, projectId: 'meli-sbox' });
  const [rows] = await client.query({
    query: `
      SELECT DISTINCT
        AGENCIE_ID AS Place_id,
        REGEXP_REPLACE(SHP_AGEN_DESC, r'.*\\\\Agncia Mercado Livre - ', '') AS Place_name,
        DATE(COMPLETION_DATE) AS Data_Avaliacao,
        EXAM_GRADE AS QD_ACERTO
      FROM \`meli-sbox.EXEOPRTRAINING.PLACE_FACT_FIX_BASE_PLACES\`
      WHERE SITE = 'MLB'
        AND EXAM_GRADE IS NOT NULL
        AND AGENCIE_ID IS NOT NULL
        AND SERVICO NOT IN ('INSTALACAO')
    `,
    location: 'US'
  });
  return rows || [];
}

/** Porta de dadosVolumetria_Revisado() → aba "Base" */
async function dadosVolumetria() {
  return runBQQuery('meli-bi-data', `
    SELECT
      PLC_PLACE_ID,
      PLC_PLACE_STATUS_SBO AS STATUS_SBO,
      SAFE_CAST(PLC_PLACE_LAT AS FLOAT64) AS LATITUDE,
      SAFE_CAST(PLC_PLACE_LONG AS FLOAT64) AS LONGITUDE,
      CASE
        WHEN PLC_PLACE_NEX = 'Sim' THEN 'NODO'
        WHEN PLC_PLACE_DC = 'Sim' THEN 'NODO'
        ELSE 'PUDO'
      END AS TIPO_DE_PLACE,
      PLC_PLACE_DROP_OFF AS DROP_OFF,
      PLC_PLACE_PICK_UP AS PICK_UP,
      PLC_PLACE_NEX AS NEX,
      PLC_PLACE_DC AS DELIVERY_CELL,
      PLC_PLACE_SVC,
      PLC_PLACE_FACILITY
    FROM \`meli-bi-data.WHOWNER.BT_CARTEIRA_MLB\`
  `);
}

/** Porta de dadosSBO() → aba "Extração SBO1" */
async function dadosSBO() {
  return runBQQuery('meli-bi-data', `
    SELECT
      SHP_AGENCY_ID, SHP_AGENCY_IS_DC_FLAG, SHP_AGEN_ACRONYM, SHP_AGEN_ACTIVE_DT,
      SHP_AGEN_ADDRESS_LINE, SHP_AGEN_BUSINESS_NAME, SHP_AGEN_CAPACITY,
      SHP_AGEN_CITY_NAME, SHP_AGEN_CS_AGENCY_PICKUP_FLAG,
      SHP_AGEN_CS_DELIVERY_AVAILABILITY_FLAG, SHP_AGEN_CS_INSTALLATION_AVAILABILITY_FLAG,
      SHP_AGEN_CS_LATE_DROP_OFF_FLAG, SHP_AGEN_CS_PACKAGE_RECEPTION_FLAG,
      SHP_AGEN_CS_SECURE_PACKAGE_RECEPTION_FLAG, SHP_AGEN_CS_STAMP_SELLING_FLAG,
      SHP_AGEN_CS_UNLABELED_PACKAGE_RECEPTION_FLAG, SHP_AGEN_DATE_CREATED,
      SHP_AGEN_DESC, SHP_AGEN_EMAIL, SHP_AGEN_EXISTS, SHP_AGEN_HOURS_DISPLAY,
      SHP_AGEN_HOURS_EARLY_HS_FLAG, SHP_AGEN_HOURS_EXTENDED_HS_FLAG,
      SHP_AGEN_HOURS_OPEN_WKNDS_FLAG, SHP_AGEN_INACTIVE_DT, SHP_AGEN_IS_FRANCHISE,
      SHP_AGEN_LATITUD, SHP_AGEN_LONGITUD, SHP_AGEN_NEIGHBORHOOD_NAME,
      SHP_AGEN_OPEN_HOURS, SHP_AGEN_REACTIVE_DT, SHP_AGEN_STATE_ID,
      SHP_AGEN_STATE_NAME, SHP_AGEN_STATUS, SHP_AGEN_STREET_NAME,
      SHP_AGEN_STREET_NUMBER, SHP_AGEN_TYPE, SHP_AGEN_ZIP_CODE,
      SHP_CARRIER_ID, SHP_SITE_ID
    FROM \`meli-bi-data.WHOWNER.LK_SHP_AGENCIES_API\`
    WHERE SHP_SITE_ID = 'MLB'
  `);
}

// ─── syncSheetsToBigQuery ─────────────────────────────────────────────────────
// Porta de syncSheets_UpsertSemApagarAusentes() do Apps Script.
// sources: [{ sheetName, headers, rows, primaryKeyField, tableId }]

function _normBqType(t) {
  t = String(t || '').toUpperCase();
  if (t === 'BOOLEAN') return 'BOOL';
  if (t === 'INTEGER') return 'INT64';
  if (t === 'FLOAT')   return 'FLOAT64';
  return t || 'STRING';
}

function _normKey(v) {
  if (v === null || v === undefined) return '';
  let s = String(v).trim();
  if (/^\d+\.0$/.test(s)) s = s.replace(/\.0$/, '');
  return s;
}

function _toFieldName(header, idx) {
  let h = (!header || String(header).trim() === '') ? ('COL_' + (idx + 1)) : String(header).trim();
  h = h.toUpperCase().replace(/[^\p{L}\p{N}_]/gu, '_').replace(/_+/g, '_').replace(/^_+|_+$/g, '');
  if (!/^[A-Z_]/.test(h)) h = '_' + h;
  return h.substring(0, 300);
}

function _makeUnique(names) {
  const seen = {};
  return names.map(n => {
    let k = 1, curr = n;
    while (seen[curr]) { k++; curr = n + '_' + k; }
    seen[curr] = true;
    return curr;
  });
}

function _isSerial(v) { return typeof v === 'number' && v > 0 && v < 2958466; }
function _serialToDate(s) { return new Date(new Date(1899, 11, 30).getTime() + s * 86400000); }
function _parseBR(s) {
  s = String(s).trim();
  let m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1], +m[4], +m[5], +m[6]);
  m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1], 0, 0, 0);
  return null;
}
function _fmtSP(d) {
  if (!d || isNaN(d.getTime())) return null;
  const f = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
  }).format(d);
  // en-CA gives "YYYY-MM-DD, HH:MM:SS"
  return f.replace(', ', ' ').replace(',', ' ');
}

function _coerce(value, bqType) {
  if (value === '' || value === null || value === undefined) return null;
  bqType = _normBqType(bqType);
  switch (bqType) {
    case 'STRING': return String(value);
    case 'BOOL': {
      if (typeof value === 'boolean') return value;
      const s = String(value).trim().toLowerCase();
      if (['true','1','sim','s'].includes(s)) return true;
      if (['false','0','não','nao','n'].includes(s)) return false;
      return null;
    }
    case 'INT64': {
      if (typeof value === 'boolean') return value ? '1' : '0';
      const sv = String(value).trim().toLowerCase();
      if (sv === 'true' || sv === 'sim') return '1';
      if (sv === 'false' || sv === 'nao' || sv === 'não') return '0';
      const n = typeof value === 'number' ? value : Number(String(value).replace(',', '.'));
      if (isNaN(n) || !isFinite(n)) return null;
      return String(Math.trunc(n));
    }
    case 'FLOAT64': {
      const f = typeof value === 'number' ? value : Number(String(value).replace(',', '.'));
      return (isNaN(f) || !isFinite(f)) ? null : f;
    }
    case 'DATE': {
      if (_isSerial(value)) { const d = _serialToDate(value); if (!isNaN(d.getTime())) return _fmtSP(d).substring(0, 10); }
      if (value instanceof Date) return isNaN(value.getTime()) ? null : _fmtSP(value).substring(0, 10);
      const s = String(value).trim();
      const br = _parseBR(s); if (br && !isNaN(br.getTime())) return _fmtSP(br).substring(0, 10);
      const iso = s.match(/^(\d{4}-\d{2}-\d{2})/); if (iso) return iso[1];
      const gen = new Date(s); return (!isNaN(gen.getTime())) ? _fmtSP(gen).substring(0, 10) : null;
    }
    case 'DATETIME': {
      if (_isSerial(value)) { const d = _serialToDate(value); if (!isNaN(d.getTime())) return _fmtSP(d); }
      if (value instanceof Date) return isNaN(value.getTime()) ? null : _fmtSP(value);
      const s = String(value).trim();
      const br = _parseBR(s); if (br && !isNaN(br.getTime())) return _fmtSP(br);
      if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(s)) return s;
      const gen = new Date(s); return (!isNaN(gen.getTime())) ? _fmtSP(gen) : null;
    }
    case 'TIMESTAMP': {
      if (_isSerial(value)) { const d = _serialToDate(value); if (!isNaN(d.getTime())) return d.toISOString(); }
      if (value instanceof Date) return value.toISOString();
      const s = String(value).trim();
      const br = _parseBR(s); if (br && !isNaN(br.getTime())) return br.toISOString();
      const gen = new Date(s); return (!isNaN(gen.getTime())) ? gen.toISOString() : null;
    }
    default: return String(value);
  }
}

function _inferType(samples) {
  if (!samples.length) return 'STRING';
  let allBool=true, allInt=true, allFloat=true, allDate=true, allDt=true;
  for (const v of samples) {
    const s = String(v).trim().toLowerCase();
    if (!(typeof v === 'boolean' || ['true','false','sim','não','nao','1','0'].includes(s))) allBool = false;
    const n = typeof v === 'number' ? v : Number(String(v).replace(',', '.'));
    if (isNaN(n) || !isFinite(n)) { allInt = false; allFloat = false; }
    else if (Math.floor(n) !== n) allInt = false;
    let d = null;
    if (v instanceof Date) d = v;
    else if (_isSerial(v)) d = _serialToDate(v);
    else { const br = _parseBR(String(v)); if (br && !isNaN(br.getTime())) d = br; else { const p = new Date(v); if (!isNaN(p.getTime())) d = p; } }
    if (!d || isNaN(d.getTime())) { allDate = false; allDt = false; }
    else if (d.getHours() !== 0 || d.getMinutes() !== 0 || d.getSeconds() !== 0) allDate = false;
  }
  if (allBool)  return 'BOOL';
  if (allInt)   return 'INT64';
  if (allFloat) return 'FLOAT64';
  if (allDate)  return 'DATE';
  if (allDt)    return 'DATETIME';
  return 'STRING';
}

function _buildPlan(sheetName, headers, rows) {
  const rawNames = headers.map((h, i) => _toFieldName(h, i));
  const bqNames  = _makeUnique(rawNames);
  const samples  = {};
  for (const n of bqNames) samples[n] = [];
  const lim = Math.min(rows.length, 200);
  for (let r = 0; r < lim; r++) {
    for (let c = 0; c < headers.length; c++) {
      const v = rows[r][c];
      if (v !== '' && v !== null && v !== undefined) samples[bqNames[c]].push(v);
    }
  }
  const types = {};
  for (const f of Object.keys(samples)) types[f] = _inferType(samples[f]);
  // Force ranges for Pipeline
  if (sheetName === 'Pipeline') {
    for (let i = 54; i <= 59 && i < bqNames.length; i++) {
      if (bqNames[i]) types[bqNames[i]] = 'INT64';
    }
  }
  types['AUD_INS_DTTM'] = 'DATETIME';
  types['AUD_UPD_DTTM'] = 'DATETIME';
  return { bqNames, fieldTypes: types };
}

function _buildRecords(headers, rows, plan, primaryKeyField, schemaTypeMap) {
  const now = new Date();
  const pkIdx = plan.bqNames.indexOf(primaryKeyField);
  return rows.map(row => {
    const keyVal = _normKey(pkIdx !== -1 ? row[pkIdx] : null);
    if (!keyVal) return null;
    const rec = {};
    for (let c = 0; c < headers.length; c++) {
      const fname = plan.bqNames[c];
      const type  = schemaTypeMap[fname] || plan.fieldTypes[fname] || 'STRING';
      rec[fname]  = _coerce(row[c], type);
    }
    rec['AUD_UPD_DTTM'] = _coerce(now, schemaTypeMap['AUD_UPD_DTTM'] || 'DATETIME');
    rec['AUD_INS_DTTM'] = _coerce(now, schemaTypeMap['AUD_INS_DTTM'] || 'DATETIME');
    return rec;
  }).filter(Boolean);
}

async function syncSheetsToBigQuery(sources) {
  const BQ_PROJECT = 'meli-bi-data';
  const BQ_DATASET = process.env.BQ_SYNC_DATASET || 'SBOX_MLBPLACES';
  const BQ_LOCATION = 'US';

  const client  = new BigQuery({ ...bqOpts, projectId: BQ_PROJECT });
  const dataset = client.dataset(BQ_DATASET);
  const results = [];

  for (const src of sources) {
    const { sheetName, headers, rows, primaryKeyField, tableId } = src;
    console.log(`[syncBQ] Aba: ${sheetName} → ${tableId}`);
    try {
      if (!headers || !headers.length) { results.push({ sheetName, success: false, msg: 'Sem headers' }); continue; }

      const plan = _buildPlan(sheetName, headers, rows);
      let schemaTypeMap = {};
      let allCols = [];

      // Garante tabela
      try {
        const [, meta] = await dataset.table(tableId).get();
        (meta.schema?.fields || []).forEach(f => {
          schemaTypeMap[f.name] = _normBqType(f.type);
          allCols.push(f.name);
        });
      } catch (_) {
        const fields = plan.bqNames.map(n => ({ name: n, type: plan.fieldTypes[n] || 'STRING', mode: 'NULLABLE' }));
        fields.push({ name: 'AUD_INS_DTTM', type: 'DATETIME', mode: 'NULLABLE' });
        fields.push({ name: 'AUD_UPD_DTTM', type: 'DATETIME', mode: 'NULLABLE' });
        await dataset.createTable(tableId, { schema: { fields }, location: BQ_LOCATION });
        const [, meta2] = await dataset.table(tableId).get();
        (meta2.schema?.fields || []).forEach(f => {
          schemaTypeMap[f.name] = _normBqType(f.type);
          allCols.push(f.name);
        });
      }

      // Adiciona colunas faltando
      const newFields = [];
      for (const n of plan.bqNames) {
        if (!schemaTypeMap[n]) newFields.push({ name: n, type: plan.fieldTypes[n] || 'STRING', mode: 'NULLABLE' });
      }
      if (!schemaTypeMap['AUD_INS_DTTM']) newFields.push({ name: 'AUD_INS_DTTM', type: 'DATETIME', mode: 'NULLABLE' });
      if (!schemaTypeMap['AUD_UPD_DTTM']) newFields.push({ name: 'AUD_UPD_DTTM', type: 'DATETIME', mode: 'NULLABLE' });
      if (newFields.length > 0) {
        const tbl = dataset.table(tableId);
        const [, curMeta] = await tbl.get();
        const updFields = [...(curMeta.schema?.fields || []), ...newFields];
        await tbl.setMetadata({ schema: { fields: updFields } });
        const [, meta3] = await tbl.get();
        (meta3.schema?.fields || []).forEach(f => {
          schemaTypeMap[f.name] = _normBqType(f.type);
          if (!allCols.includes(f.name)) allCols.push(f.name);
        });
      }

      const records = _buildRecords(headers, rows, plan, primaryKeyField, schemaTypeMap);
      if (!records.length) { results.push({ sheetName, success: true, msg: 'Sem linhas com chave válida' }); continue; }

      // Staging
      const stagingId = tableId + '_STG_' + Date.now();
      const [, tgtMeta] = await dataset.table(tableId).get();
      await dataset.createTable(stagingId, { schema: tgtMeta.schema, location: BQ_LOCATION });

      try {
        const stgTable = dataset.table(stagingId);
        const BATCH = 500;
        let inserted = 0;
        for (let i = 0; i < records.length; i += BATCH) {
          await stgTable.insert(records.slice(i, i + BATCH), { skipInvalidRows: false, ignoreUnknownValues: false });
          inserted += Math.min(BATCH, records.length - i);
        }

        // Upsert via FULL OUTER JOIN
        const pk   = primaryKeyField;
        const qPk  = `\`${pk}\``;
        const cols = allCols.map(col => {
          const qc = `\`${col}\``;
          if (col === pk)            return `COALESCE(S.${qc}, T.${qc}) AS ${qc}`;
          if (col === 'AUD_INS_DTTM') return 'COALESCE(T.`AUD_INS_DTTM`, S.`AUD_INS_DTTM`) AS `AUD_INS_DTTM`';
          if (col === 'AUD_UPD_DTTM') return `CASE WHEN S.${qPk} IS NOT NULL THEN S.\`AUD_UPD_DTTM\` ELSE T.\`AUD_UPD_DTTM\` END AS \`AUD_UPD_DTTM\``;
          return `CASE WHEN S.${qPk} IS NOT NULL THEN S.${qc} ELSE T.${qc} END AS ${qc}`;
        });
        const fullTbl  = `\`${BQ_PROJECT}.${BQ_DATASET}.${tableId}\``;
        const stgFull  = `\`${BQ_PROJECT}.${BQ_DATASET}.${stagingId}\``;
        const sql = `CREATE OR REPLACE TABLE ${fullTbl} AS\nSELECT\n  ${cols.join(',\n  ')}\nFROM ${fullTbl} T\nFULL OUTER JOIN ${stgFull} S\n  ON CAST(T.${qPk} AS STRING) = CAST(S.${qPk} AS STRING)`;
        await client.query({ query: sql, location: BQ_LOCATION });
        results.push({ sheetName, success: true, rowsProcessed: inserted });
      } finally {
        try { await dataset.table(stagingId).delete(); } catch (_) {}
      }

    } catch (e) {
      console.error(`[syncBQ] Erro em ${sheetName}:`, e.message);
      results.push({ sheetName, success: false, msg: e.message });
    }
  }
  return results;
}

module.exports = {
  obterLeadsQuentesProximos,
  searchPlaceIdsInBigQuery,
  getPlaceInfoFromLeads,
  getTreinamentoByPlaceId,
  dadosGoLive,
  dadosLogistics,
  dadosMercadopago,
  dadosNewPlace,
  treinamentoB,
  dadosVolumetria,
  dadosSBO,
  syncSheetsToBigQuery
};
