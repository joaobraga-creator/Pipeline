/**
 * bigquery.js — Serviço BigQuery
 * Porta das funções obterLeadsQuentesProximos e searchPlaceIdsInBigQuery
 *
 * IMPORTANTE: As queries SQL abaixo são baseadas na estrutura esperada das
 * tabelas. Você precisará ajustá-las para corresponder às suas tabelas reais.
 */
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');

const bqOpts = { projectId: process.env.BIGQUERY_PROJECT_ID };

// Tenta usar GOOGLE_APPLICATION_CREDENTIALS, depois adc_credentials.json local
const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS
  || path.join(__dirname, '..', 'adc_credentials.json');
if (fs.existsSync(keyFile)) bqOpts.keyFilename = keyFile;

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

module.exports = {
  obterLeadsQuentesProximos,
  searchPlaceIdsInBigQuery,
  getPlaceInfoFromLeads,
  getTreinamentoByPlaceId
};
