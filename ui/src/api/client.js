import axios from 'axios';

// All requests go to /api — in dev Vite proxies this to localhost:8000,
// in production nginx proxies it to http://api-gateway:8000.
// This means the browser never needs to know the gateway's real host,
// eliminating CORS preflight entirely.
const client = axios.create({
  baseURL: '/api',
  timeout: 10000,
});

// Stored separately so setApiKey can update it without recreating the instance.
let currentApiKey = 'acme-api-key-2024';
client.defaults.headers.common['X-API-Key'] = currentApiKey;

export function setApiKey(key) {
  currentApiKey = key;
  client.defaults.headers.common['X-API-Key'] = key;
}

// getApiKey: returns the currently active API key.
// Used by ReasoningStream to build the SSE URL (?api_key=...) since the native
// EventSource API cannot send custom HTTP headers — query param is the workaround.
export function getApiKey() {
  return currentApiKey;
}

export const ingestLog = (payload) => client.post('/v1/logs/ingest', payload);

// getSecurityEvents polls the security events endpoint for the Security Panel.
// params: { limit?: number, event_type?: 'injection' | 'pii' }
export const getSecurityEvents = (params) =>
  client.get('/v1/security/events', { params });

// getRecentLogs fetches the most recent logs for the current tenant.
// params: { service?: string, level?: string, limit?: number }
// Returns: { logs: LogEntry[], total: number }
export const getRecentLogs = (params) =>
  client.get('/v1/logs/recent', { params });

// getAlerts polls GET /api/v1/alerts for the current tenant.
// params: { severity?: string, service?: string, limit?: number }
// Returns: { alerts: AlertRow[], total: number }
export const getAlerts = (params) =>
  client.get('/v1/alerts', { params });

// floodErrors triggers POST /api/v1/simulate/flood for the current tenant.
// Sends 100 ERROR logs for the named service through the full ingest pipeline.
// service: string — the service name to flood (e.g. "payment-service")
export const floodErrors = (service) =>
  client.post('/v1/simulate/flood', { service });

// getAlertDetail fetches GET /api/v1/alerts/{alertId} for the AlertDrawer.
// Returns all alert fields plus linked incident data (compression stats)
// and RCA result if available. Null fields indicate the pipeline stage
// has not yet run for this alert.
export const getAlertDetail = (alertId) =>
  client.get(`/v1/alerts/${alertId}`);

// getCacheStats fetches GET /api/v1/cache/stats for the StatsBar component.
// Returns hit_count, miss_count, hit_rate, keys_stored, estimated_tokens_saved,
// and estimated_cost_saved_usd for the current tenant.
// All values are 0/0.0 when the cache is cold (no lookups yet).
export const getCacheStats = () =>
  client.get('/v1/cache/stats');

// getInvestigations fetches GET /api/v1/investigations for the investigations list.
// params: { status?: 'success' | 'failed' | 'retried', limit?: number }
// Returns: { investigations: InvestigationRow[], total: number }
export const getInvestigations = (params) =>
  client.get('/v1/investigations', { params });

// getInvestigation fetches GET /api/v1/investigations/{rca_id} for the RCADetail page.
// Returns the full investigation with reasoning_steps and recommendations.
// The RCADetail page polls this every 5 seconds while status='retried'/'pending'.
export const getInvestigation = (rcaId) =>
  client.get(`/v1/investigations/${rcaId}`);

// triggerInvestigation posts to POST /api/v1/investigations/trigger.
// incidentId: string UUID of the incident to investigate.
// Returns: { incident_id: string, rca_id: string } — the UI navigates to
//   /investigations/{rca_id} immediately after this call succeeds.
export const triggerInvestigation = (incidentId) =>
  client.post('/v1/investigations/trigger', { incident_id: incidentId });

// getEvalSummary fetches GET /api/v1/eval/summary for the EvalScoresPanel.
// Returns aggregate faithfulness, hallucination, cost, and pass rate stats.
// All fields are 0/0.0 when no evaluations have been recorded yet.
export const getEvalSummary = () =>
  client.get('/v1/eval/summary');

// labelAlert patches PATCH /api/v1/alerts/{alert_id}/label with a ground truth.
// Stores a human-verified root cause on the alert so the GroundTruthStrategy
// can produce more reliable faithfulness scores on future evaluations.
// alert_id: string UUID of the alert to label.
// ground_truth: string — human-verified root cause (min 10 chars, max 1000 chars).
export const labelAlert = (alert_id, ground_truth) =>
  client.patch(`/v1/alerts/${alert_id}/label`, { ground_truth });

// getKnowledgeBaseStats fetches GET /api/v1/knowledge-base/stats.
// Returns total_incidents, seed_count, auto_learned_count, and services array.
// Used by the EvalScoresPanel to show knowledge base growth over time.
export const getKnowledgeBaseStats = () =>
  client.get('/v1/knowledge-base/stats');

export default client;
