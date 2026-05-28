// ABChart — pure CSS horizontal bar chart for prompt A/B performance comparison.
// Why no charting library?
//   recharts / chart.js add ~200 kB for two bars. A div with width set as
//   a percentage is a bar chart. The visual result is identical for this use case.
// Props:
//   v1_score — float 0–1: Prompt v1 average faithfulness score
//   v2_score — float 0–1: Prompt v2 average faithfulness score
//   label — descriptive title displayed above the bars
// Empty state: when both scores are null, undefined, or zero (no evaluations
// recorded yet), the component shows "No data yet" instead of two empty bars.
// Zero is treated as absent because a legitimate 0% score is indistinguishable
// from a missing score when the eval harness has not run.

import React from 'react';

// BarRow — one labelled bar for a single prompt variant.
// score: float 0–1. Clamped to [0, 100] percent for display.
// colour: hex string for the bar fill and the percentage label.
function BarRow({ label, score, colour }) {
  // Clamp to [0, 100] — guards against NaN or >1 from an API bug.
  const pct = Math.min(100, Math.max(0, Math.round((score ?? 0) * 100)));

  return (
    <div style={{ marginBottom: '14px' }}>
      {/* Row header: variant label left, percentage value right */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '5px',
      }}>
        <span style={{ fontSize: '12px', color: '#555', fontWeight: '500' }}>
          {label}
        </span>
        <span style={{ fontSize: '12px', fontWeight: '700', color: colour }}>
          {pct}%
        </span>
      </div>

      {/* Bar track: full-width grey background that the fill bar sits inside */}
      <div style={{
        height: '18px',
        background: '#f0f0f0',
        borderRadius: '4px',
        overflow: 'hidden',
      }}>
        {/* Bar fill: width = percentage of the track width (0–100%) */}
        <div style={{
          height: '100%',
          width: `${pct}%`,
          background: colour,
          borderRadius: '4px',
          // CSS transition animates width when score updates on the 15-second poll.
          transition: 'width 0.4s ease',
        }} />
      </div>
    </div>
  );
}

export default function ABChart({ v1_score, v2_score, label }) {
  // noData: treat null, undefined, and 0 as "no evaluations recorded yet".
  const noData = !v1_score && !v2_score;

  return (
    <div>
      {/* Chart label: describes what the two bars measure */}
      {label && (
        <div style={{
          fontSize: '12px',
          fontWeight: '600',
          color: '#555',
          marginBottom: '14px',
        }}>
          {label}
        </div>
      )}

      {noData ? (
        // Empty state: guides the user to run investigations rather than showing
        // two empty bars that look like a broken chart.
        <div style={{
          fontSize: '13px',
          color: '#aaa',
          fontStyle: 'italic',
          padding: '8px 0',
        }}>
          No data yet
        </div>
      ) : (
        <>
          {/* Prompt v1: blue — the systematic evidence-first strategy (baseline) */}
          <BarRow label="Prompt v1" score={v1_score} colour="#4299e1" />
          {/* Prompt v2: orange — the hypothesis-driven A/B variant */}
          <BarRow label="Prompt v2" score={v2_score} colour="#ed8936" />
        </>
      )}
    </div>
  );
}
