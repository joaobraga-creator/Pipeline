/**
 * api.js — Todas as rotas da API
 * Cada rota corresponde a uma função do Apps Script (google.script.run)
 */
const express = require('express');
const router = express.Router();

const sheets = require('../services/sheets');
const bq = require('../services/bigquery');

// Em modo dev, garante que req.user sempre existe
if (process.env.NODE_ENV !== 'production') {
  router.use((req, res, next) => {
    if (!req.user) req.user = {
      email: process.env.DEV_USER_EMAIL || 'joao.braga@mercadolivre.com',
      name: 'Dev Mode',
      photo: ''
    };
    next();
  });
}

// Helper para tratar erros
function handleError(res, error, context) {
  console.error(`[API:${context}]`, error);
  res.status(500).json({ error: error.message || 'Erro interno no servidor.' });
}

// ─── getAppData ───────────────────────────────────────────────────────────────
// Substitui: google.script.run.getAppData()
router.post('/getAppData', async (req, res) => {
  try {
    const data = await sheets.getAppData(req.user.email);
    res.json(data);
  } catch (e) {
    handleError(res, e, 'getAppData');
  }
});

// ─── getNotificationCount ─────────────────────────────────────────────────────
// Substitui: google.script.run.getNotificationCountFromBackend(email)
router.post('/getNotificationCount', async (req, res) => {
  try {
    const overdue = await sheets.checkForOverdueAccepts(req.user.email);
    const count = (overdue.length === 1 && overdue[0].message) ? 0 : overdue.length;
    res.json({ count });
  } catch (e) {
    handleError(res, e, 'getNotificationCount');
  }
});

// ─── checkForOverdueAccepts ───────────────────────────────────────────────────
// Substitui: google.script.run.checkForOverdueAccepts(userEmail)
router.post('/checkForOverdueAccepts', async (req, res) => {
  try {
    const result = await sheets.checkForOverdueAccepts(req.user.email);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'checkForOverdueAccepts');
  }
});

// ─── updateLeadIdAndDismissNotification ──────────────────────────────────────
// Substitui: google.script.run.updateLeadIdAndDismissNotification({geoId, leadId})
router.post('/updateLeadIdAndDismissNotification', async (req, res) => {
  try {
    const { geoId, leadId } = req.body;
    if (!geoId || !leadId) return res.status(400).json({ success: false, message: 'geoId e leadId são obrigatórios.' });
    const result = await sheets.updateLeadIdAndDismissNotification(geoId, leadId);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'updateLeadIdAndDismissNotification');
  }
});

// ─── getServicoOriginal ───────────────────────────────────────────────────────
// Substitui: google.script.run.getServicoOriginal(geoId)
router.post('/getServicoOriginal', async (req, res) => {
  try {
    const { geoId } = req.body;
    if (!geoId) return res.status(400).json({ error: 'geoId é obrigatório.' });
    const result = await sheets.getServicoOriginal(geoId);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'getServicoOriginal');
  }
});

// ─── generateUniquePlaceId ────────────────────────────────────────────────────
// Substitui: google.script.run.generateUniquePlaceId('LF')
router.post('/generateUniquePlaceId', async (req, res) => {
  try {
    const prefix = req.body.prefix || 'LF';
    const id = await sheets.generateUniquePlaceId(prefix);
    res.json({ id });
  } catch (e) {
    handleError(res, e, 'generateUniquePlaceId');
  }
});

// ─── atualizarCamposSimples ───────────────────────────────────────────────────
// Substitui: google.script.run.atualizarCamposSimples(payload)
router.post('/atualizarCamposSimples', async (req, res) => {
  try {
    const result = await sheets.atualizarCamposSimples(req.user.email, req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'atualizarCamposSimples');
  }
});

// ─── updateProspect ───────────────────────────────────────────────────────────
// Substitui: google.script.run.updateProspect(updateData)
router.post('/updateProspect', async (req, res) => {
  try {
    const result = await sheets.updateProspect(req.user.email, req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'updateProspect');
  }
});

// ─── addColdLead ──────────────────────────────────────────────────────────────
// Substitui: google.script.run.addColdLead(leadData)
router.post('/addColdLead', async (req, res) => {
  try {
    const result = await sheets.addColdLead(req.user.email, req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'addColdLead');
  }
});

// ─── createOrUpdateProposalFromLead ──────────────────────────────────────────
// Substitui: google.script.run.createOrUpdateProposalFromLead(prospectData)
router.post('/createOrUpdateProposalFromLead', async (req, res) => {
  try {
    const result = await sheets.createOrUpdateProposalFromLead(req.user.email, req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'createOrUpdateProposalFromLead');
  }
});

// ─── deleteRow ────────────────────────────────────────────────────────────────
// Substitui: google.script.run.deleteRow(data)
router.post('/deleteRow', async (req, res) => {
  try {
    const { geoId, placeId, aba } = req.body;
    if (!geoId || !aba) return res.status(400).json({ success: false, message: 'geoId e aba são obrigatórios.' });
    const result = await sheets.deleteRowFromSheet(req.user.email, geoId, placeId, aba);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'deleteRow');
  }
});

// ─── obterLeadsQuentesProximos ────────────────────────────────────────────────
// Substitui: google.script.run.obterLeadsQuentesProximos(coordsData)
router.post('/obterLeadsQuentesProximos', async (req, res) => {
  try {
    const result = await bq.obterLeadsQuentesProximos(req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'obterLeadsQuentesProximos');
  }
});

// ─── searchPlaceIds ───────────────────────────────────────────────────────────
// Substitui: google.script.run.searchPlaceIdsInBigQuery(searchTerm)
router.post('/searchPlaceIds', async (req, res) => {
  try {
    const { searchTerm } = req.body;
    const result = await bq.searchPlaceIdsInBigQuery(searchTerm);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'searchPlaceIds');
  }
});

// ─── getPlaceInfoFromLeads ────────────────────────────────────────────────────
// Substitui: google.script.run.getPlaceInfoFromLeads(selectedId)
router.post('/getPlaceInfoFromLeads', async (req, res) => {
  try {
    const { placeId } = req.body;
    const result = await bq.getPlaceInfoFromLeads(placeId);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'getPlaceInfoFromLeads');
  }
});

// ─── getTreinamentoByPlaceId ──────────────────────────────────────────────────
// Substitui: google.script.run.getTreinamentoByPlaceId(placeId)
router.post('/getTreinamentoByPlaceId', async (req, res) => {
  try {
    const { placeId } = req.body;
    const result = await bq.getTreinamentoByPlaceId(placeId);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'getTreinamentoByPlaceId');
  }
});

// ─── dadosGoLive ──────────────────────────────────────────────────────────────
// Substitui: dadosGoLive() do Apps Script
router.post('/dadosGoLive', async (req, res) => {
  try {
    const rows = await bq.dadosGoLive();
    const result = await sheets.escreverDadosGoLive(rows);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'dadosGoLive');
  }
});

// ─── editarRegistro ───────────────────────────────────────────────────────────
// Substitui: google.script.run.editarRegistro(dados)
router.post('/editarRegistro', async (req, res) => {
  try {
    const result = await sheets.editarRegistro(req.user.email, req.body);
    res.json(result);
  } catch (e) {
    handleError(res, e, 'editarRegistro');
  }
});

module.exports = router;
