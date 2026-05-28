import useApi from '../hooks/useApi.js';
import { getCacheStats } from '../api/client.js';

// --- Divider between stat items ---
// A vertical bar rendered between each metric pair for visual separation.
function StatDivider() {
  return (
    <span style={{
      display: 'inline-block',
      width: '1px',
      height: '20px',
      background: '#90caf9',
      margin: '0 14px',
      verticalAlign: 'middle',
    }} />
  );
}

// --- Single stat item ---
// label: descriptor string, e.g. "Cache hit rate"
// value: formatted display value, e.g. "84%" or "—" when loading
function StatItem({ label, value }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}>
      <span style={{ fontSize: '12px', color: '#1565c0', fontWeight: '500' }}>
        {label}
      </span>
      <span style={{
        fontSize: '13px',
        fontWeight: '700',
        color: '#0d47a1',
        fontFamily: 'monospace',
      }}>
        {value}
      </span>
    </span>
  );
}

// StatsBar is always visible at the top of DevPanel.
// It polls GET /api/v1/cache/stats every 10 seconds and displays four metrics.
// While loading or on error, each stat shows "—" so the bar never disappears.
// Why always visible (not hidden on error)?
// Even a cold cache with zero hits is informative — it tells the operator the
// cache is running but has not yet been warmed up. Hiding the bar on error
// removes that signal entirely.
export default function StatsBar() {
  // immediate: true fires on mount so stats appear before the first 10-second tick.
  // interval: 10000 — polling every 10 seconds is low enough not to create noise
  // while frequent enough to track cache warm-up during a live demo.
  const { data, loading } = useApi(getCacheStats, { immediate: true, interval: 10000 });

  // Format each metric. null/undefined (loading or error) shows "—".
  const hitRateDisplay = data?.hit_rate != null
    ? `${(data.hit_rate * 100).toFixed(0)}%`
    : '—';

  const tokensSavedDisplay = data?.estimated_tokens_saved != null
    // toLocaleString inserts comma separators: 12000 → "12,000"
    ? data.estimated_tokens_saved.toLocaleString()
    : '—';

  const costSavedDisplay = data?.estimated_cost_saved_usd != null
    ? `$${data.estimated_cost_saved_usd.toFixed(4)}`
    : '—';

  const keysStoredDisplay = data?.keys_stored != null
    ? String(data.keys_stored)
    : '—';

  const hitCountDisplay = data?.hit_count != null
    ? String(data.hit_count)
    : '—';

  const missCountDisplay = data?.miss_count != null
    ? String(data.miss_count)
    : '—';

  return (
    <div style={{
      background: '#e3f2fd',
      border: '1px solid #90caf9',
      borderRadius: '6px',
      padding: '10px 16px',
      marginBottom: '16px',
      display: 'flex',
      alignItems: 'center',
      flexWrap: 'wrap',
      gap: '4px',
      // Subtle opacity reduction while loading so users know an update is pending.
      opacity: loading ? 0.8 : 1,
      transition: 'opacity 0.2s',
    }}>
      {/* Title prefix */}
      <span style={{
        fontSize: '11px',
        fontWeight: '700',
        color: '#1565c0',
        textTransform: 'uppercase',
        letterSpacing: '0.05em',
        marginRight: '8px',
      }}>
        Semantic Cache
      </span>

      <StatItem label="Hit rate:" value={hitRateDisplay} />
      <StatDivider />
      <StatItem label="Hits:" value={hitCountDisplay} />
      <StatDivider />
      <StatItem label="Misses:" value={missCountDisplay} />
      <StatDivider />
      <StatItem label="Tokens saved:" value={tokensSavedDisplay} />
      <StatDivider />
      <StatItem label="Cost saved:" value={costSavedDisplay} />
      <StatDivider />
      <StatItem label="Cached results:" value={keysStoredDisplay} />
    </div>
  );
}
