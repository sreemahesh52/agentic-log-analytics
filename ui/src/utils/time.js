// Shared relative timestamp formatter — single implementation prevents format drift
// when the same pattern is needed in DevPanel, AlertDrawer, RCADetail, and Dashboard.
// Format contract (matches operational monitoring conventions):
//   null / undefined → "—" (always safe — never throws)
//   future timestamp → "just now" (handles clock skew between services)
//   < 60 seconds → "{N} secs ago"
//   < 60 minutes → "{N} mins ago"
//   < 24 hours → "{N} hours ago"
//   ≥ 24 hours → "{N} days ago"
// Usage pattern for accessible relative timestamps:
//   <td title={isoString}>{formatRelative(isoString)}</td>
//   The title attr gives a native browser tooltip with the absolute UTC time
//   on hover — no JavaScript needed, works with screen readers.

export function formatRelative(isoString) {
  // Guard: null/undefined returns a dash — never crashes or shows "NaN secs ago".
  if (isoString == null) return '—';

  const diffMs = Date.now() - new Date(isoString).getTime();

  // Negative diff: timestamp is in the future (clock skew between producer and consumer).
  // "just now" is more useful than "-3 secs ago".
  if (diffMs < 0) return 'just now';

  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `${diffSec} secs ago`;

  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin} mins ago`;

  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) return `${diffHours} hours ago`;

  // Days: used for historical alerts and past incidents.
  return `${Math.floor(diffHours / 24)} days ago`;
}
