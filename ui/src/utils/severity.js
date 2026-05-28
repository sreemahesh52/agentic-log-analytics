// Shared severity colour mapping — single source of truth for all severity badges.
// Importing from here means changing one file updates DevPanel, AlertDrawer,
// RCADetail, and Dashboard simultaneously without touching business logic.
// Colour semantics (matches standard SRE traffic-light convention):
//   CRITICAL → red — system down or data loss; wake someone up
//   HIGH → orange — degraded; investigate within the hour
//   MEDIUM → yellow — minor degradation; investigate today
//   LOW → grey — informational; no urgency

export function getSeverityStyle(severity) {
  const styles = {
    // Red: demands immediate human action.
    CRITICAL: { background: '#e53e3e', color: 'white' },
    // Orange: urgent but not system-down.
    HIGH:     { background: '#ed8936', color: 'white' },
    // Yellow/amber: dark text because light yellow on white text fails WCAG contrast.
    MEDIUM:   { background: '#ecc94b', color: '#1a202c' },
    // Grey: lowest urgency — informational.
    LOW:      { background: '#a0aec0', color: 'white' },
  };
  // Unknown severity (e.g. future API additions) falls back to neutral grey.
  // Never crashes the UI for an unexpected value.
  return styles[severity] ?? { background: '#e2e8f0', color: '#1a202c' };
}
