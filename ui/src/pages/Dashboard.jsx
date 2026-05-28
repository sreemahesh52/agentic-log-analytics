// Dashboard — operational metrics page surfacing investigation quality, cost,
// cache efficiency, and prompt A/B results in plain UI cards.
// Design decisions:
//   Why 15-second refresh not 5-second?
//     Eval metrics change only after a full investigation cycle (~60–120 s).
//     Polling every 5 s hammers the API without adding observable value.
//   Why three separate API calls not one combined endpoint?
//     Each endpoint (eval/summary, cache/stats, knowledge-base/stats) is
//     independently useful to other components. Combining would couple
//     unrelated concerns and require a new gateway endpoint with no other caller.
//   Why colour thresholds are hardcoded?
//     Portfolio project. Documented as a known simplification. In production,
//     thresholds would come from a per-tenant configuration table.
//   Why pure CSS bar chart (ABChart) not recharts / chart.js?
//     recharts adds ~200 kB for two bars. A div with proportional width is
//     a bar chart. The visual result is identical for this use case.

import React, { useEffect, useState } from 'react';
import TenantSelector from '../components/TenantSelector.jsx';
import MetricCard from '../components/MetricCard.jsx';
import ABChart from '../components/ABChart.jsx';
import Spinner from '../components/Spinner.jsx';
import useApi from '../hooks/useApi.js';
import { getEvalSummary, getKnowledgeBaseStats, getCacheStats } from '../api/client.js';

// REFRESH_INTERVAL_MS: polling cadence for all three data sources.
const REFRESH_INTERVAL_MS = 15000;

// --- Colour threshold helpers ---

// passRateColour: pass_rate is 0.0–1.0 fraction. > 0.7 green, < 0.5 red, middle orange.
function passRateColour(rate) {
  if (rate == null) return 'grey';
  if (rate > 0.7) return 'green';
  if (rate < 0.5) return 'red';
  return 'orange';
}

// hallucinationColour: higher score = better (fewer hallucinations).
// > 70% green, < 50% red, middle orange.
function hallucinationColour(score) {
  if (score == null) return 'grey';
  const pct = score * 100;
  if (pct > 70) return 'green';
  if (pct < 50) return 'red';
  return 'orange';
}

// cacheHitColour: > 60% green, < 30% red, middle orange.
function cacheHitColour(rate) {
  if (rate == null) return 'grey';
  const pct = rate * 100;
  if (pct > 60) return 'green';
  if (pct < 30) return 'red';
  return 'orange';
}

// --- Section heading style reused across all three sections ---
const SECTION_HEADING_STYLE = {
  fontSize: '13px',
  fontWeight: '700',
  color: '#555',
  textTransform: 'uppercase',
  letterSpacing: '0.06em',
  marginBottom: '12px',
  marginTop: '0',
};

// ErrorBox: inline red alert shown when a section's API call fails.
// Includes a Retry button that calls the section's refetch function.
// Why per-section not page-level? Allows one section to fail while others
// still show data — a network blip should not blank the entire dashboard.
function ErrorBox({ message, onRetry }) {
  return (
    <div style={{
      padding: '12px 16px',
      background: '#fff5f5',
      border: '1px solid #fed7d7',
      borderRadius: '6px',
      fontSize: '13px',
      color: '#c53030',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: '12px',
    }}>
      <span>{message}</span>
      <button
        onClick={onRetry}
        style={{
          padding: '4px 12px',
          fontSize: '12px',
          background: '#c53030',
          color: '#fff',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer',
          whiteSpace: 'nowrap',
        }}
      >
        Retry
      </button>
    </div>
  );
}

export default function Dashboard() {
  // Set browser tab title on mount; restore on unmount so navigating back
  // to DevPanel shows its own title rather than "Dashboard — ...".
  useEffect(() => {
    document.title = 'Dashboard — Agentic Log Analytics';
    return () => { document.title = 'Agentic Log Analytics'; };
  }, []);

  // --- Three independent data sources ---
  // immediate: true fires on mount before the first 15-second tick.
  const {
    data: evalData,
    loading: evalLoading,
    error: evalError,
    execute: refetchEval,
    lastUpdated: evalUpdated,
  } = useApi(getEvalSummary, { immediate: true, interval: REFRESH_INTERVAL_MS });

  const {
    data: kbData,
    loading: kbLoading,
    error: kbError,
    execute: refetchKb,
  } = useApi(getKnowledgeBaseStats, { immediate: true, interval: REFRESH_INTERVAL_MS });

  const {
    data: cacheData,
    loading: cacheLoading,
    error: cacheError,
    execute: refetchCache,
  } = useApi(getCacheStats, { immediate: true, interval: REFRESH_INTERVAL_MS });

  // --- "Last updated" counter ---
  // secsAgo: seconds since the most recent eval/summary fetch completed.
  // Updates every second via setInterval so the indicator is always accurate.
  const [secsAgo, setSecsAgo] = useState(0);

  useEffect(() => {
    // Guard: skip until the first successful fetch.
    if (!evalUpdated) return;
    // Reset counter to 0 immediately on new data arrival.
    setSecsAgo(0);
    // Tick every second — clearInterval on cleanup prevents accumulating timers.
    const id = setInterval(() => {
      setSecsAgo(Math.floor((Date.now() - evalUpdated.getTime()) / 1000));
    }, 1000);
    // clearInterval is called when evalUpdated changes (new data) or component unmounts.
    return () => clearInterval(id);
  }, [evalUpdated]);

  // allFirstLoading: true while all three sources are still on their initial fetch.
  // Used to show a single page-level spinner instead of 7 skeleton cards.
  const allFirstLoading =
    evalLoading && !evalData && kbLoading && !kbData && cacheLoading && !cacheData;

  // Shorthand aliases for cleaner JSX.
  const evalSummary = evalData;
  const kbStats = kbData;
  const cacheStats = cacheData;

  return (
    <div style={{ fontFamily: 'sans-serif', maxWidth: '900px', margin: '0 auto', padding: '24px' }}>

      {/* ── Header ── */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        marginBottom: '24px',
      }}>
        <div>
          <h1 style={{ margin: 0, fontSize: '20px', fontWeight: '700' }}>Dashboard</h1>
          {/* Last-updated indicator: shows elapsed seconds since last data fetch.
              evalUpdated is null before the first fetch completes.*/}
          <div style={{ fontSize: '12px', color: '#888', marginTop: '4px' }}>
            {evalUpdated
              ? `Last updated: ${secsAgo} second${secsAgo !== 1 ? 's' : ''} ago`
              : 'Fetching data…'}
          </div>
        </div>
        {/* TenantSelector: same component as DevPanel — switches X-API-Key globally */}
        <TenantSelector />
      </div>

      {/* ── Page-level first-load spinner ── */}
      {allFirstLoading && (
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <Spinner size="medium" label="Loading dashboard…" />
        </div>
      )}

      {/* ── Empty state: no data and no errors after load ── */}
      {/* Shown when the system is running but no investigations have completed yet. */}
      {!allFirstLoading && !evalData && !kbData && !cacheData
        && !evalError && !kbError && !cacheError && (
        <div style={{
          textAlign: 'center',
          color: '#888',
          fontSize: '14px',
          padding: '40px 20px',
          border: '1px dashed #e0e0e0',
          borderRadius: '8px',
        }}>
          Run some investigations to see metrics here.
        </div>
      )}

      {/* ── Section 1: Investigation Quality ── */}
      {/* 4 MetricCards in a CSS grid — auto-fit collapses to 1 column on narrow screens */}
      <section style={{ marginBottom: '28px' }}>
        <h2 style={SECTION_HEADING_STYLE}>Investigation Quality</h2>

        {evalError ? (
          <ErrorBox message="Failed to load evaluation metrics." onRetry={refetchEval} />
        ) : (
          <div style={{
            display: 'grid',
            // auto-fit + minmax: 2 columns when viewport width allows, 1 on narrow.
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: '16px',
          }}>
            {/* Card 1: Pass Rate — faithfulness > 0.7 AND hallucination > 0.7 */}
            <MetricCard
              title="Pass Rate (24h)"
              value={evalSummary?.pass_rate != null
                ? `${(evalSummary.pass_rate * 100).toFixed(0)}%`
                : '—'}
              colour={passRateColour(evalSummary?.pass_rate)}
              subtitle={evalSummary?.total_evaluations != null
                ? `${evalSummary.total_evaluations} evaluations`
                : '—'}
              loading={evalLoading && !evalSummary}
            />

            {/* Card 2: Hallucination Resistance — higher = fewer hallucinations */}
            <MetricCard
              title="Avg Hallucination Resistance"
              value={evalSummary?.avg_hallucination_score != null
                ? `${(evalSummary.avg_hallucination_score * 100).toFixed(0)}%`
                : '—'}
              colour={hallucinationColour(evalSummary?.avg_hallucination_score)}
              subtitle="1.0 = no hallucination (best)"
              loading={evalLoading && !evalSummary}
            />

            {/* Card 3: Cost Today — sum of cost_usd from eval_results */}
            <MetricCard
              title="Total Cost Today"
              value={evalSummary?.total_cost_usd != null
                ? `$${evalSummary.total_cost_usd.toFixed(4)}`
                : '—'}
              colour="orange"
              subtitle="Across all investigations"
              loading={evalLoading && !evalSummary}
            />

            {/* Card 4: Cache Hit Rate — from /api/v1/cache/stats */}
            {cacheError ? (
              <ErrorBox message="Failed to load cache stats." onRetry={refetchCache} />
            ) : (
              <MetricCard
                title="Cache Hit Rate"
                value={cacheStats?.hit_rate != null
                  ? `${(cacheStats.hit_rate * 100).toFixed(0)}%`
                  : '—'}
                colour={cacheHitColour(cacheStats?.hit_rate)}
                subtitle={cacheStats?.estimated_cost_saved_usd != null
                  ? `$${cacheStats.estimated_cost_saved_usd.toFixed(4)} saved`
                  : '—'}
                loading={cacheLoading && !cacheStats}
              />
            )}
          </div>
        )}
      </section>

      {/* ── Section 2: Prompt A/B Performance ── */}
      {/* Full-width section — grid layout not used here since it is one chart */}
      <section style={{
        background: '#fff',
        border: '1px solid #e0e0e0',
        borderRadius: '8px',
        padding: '20px',
        marginBottom: '28px',
      }}>
        <h2 style={{ ...SECTION_HEADING_STYLE, marginBottom: '4px' }}>Prompt A/B Performance</h2>

        {evalError ? (
          <ErrorBox message="Failed to load A/B metrics." onRetry={refetchEval} />
        ) : evalLoading && !evalSummary ? (
          <Spinner size="small" label="Loading…" />
        ) : (
          <>
            {/* ABChart: pure CSS bars for v1 and v2 faithfulness scores */}
            <ABChart
              v1_score={evalSummary?.faithfulness_by_prompt_version?.v1 ?? 0}
              v2_score={evalSummary?.faithfulness_by_prompt_version?.v2 ?? 0}
              label="Faithfulness Score by Prompt Version (24h avg)"
            />
            <p style={{ fontSize: '11px', color: '#aaa', margin: '4px 0 0' }}>
              Higher = better. Run more investigations to get statistically significant data.
            </p>
          </>
        )}
      </section>

      {/* ── Section 3: Knowledge Base & Cache ── */}
      <section style={{ marginBottom: '28px' }}>
        <h2 style={SECTION_HEADING_STYLE}>Knowledge Base &amp; Cache</h2>

        {/* Per-source error boxes — each appears only if its source failed */}
        {kbError && (
          <div style={{ marginBottom: '12px' }}>
            <ErrorBox message="Failed to load knowledge base stats." onRetry={refetchKb} />
          </div>
        )}

        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
          gap: '16px',
        }}>
          {/* Card 5: Knowledge Base Size — seeded + auto-learned */}
          <MetricCard
            title="Knowledge Base Size"
            value={kbStats?.total_incidents ?? '—'}
            colour="blue"
            subtitle={kbStats
              ? `${kbStats.seed_count} seeded + ${kbStats.auto_learned_count} auto-learned`
              : '—'}
            loading={kbLoading && !kbStats}
          />

          {/* Card 6: Tokens Saved — estimated tokens not sent to LLM via cache */}
          <MetricCard
            title="Tokens Saved by Cache"
            value={cacheStats?.estimated_tokens_saved != null
              // toLocaleString adds comma separators: 12000 → "12,000"
              ? cacheStats.estimated_tokens_saved.toLocaleString()
              : '—'}
            colour="green"
            subtitle="Est. tokens not sent to LLM"
            loading={cacheLoading && !cacheStats}
          />

          {/* Card 7: Cached Investigations — unique cache keys stored in Redis */}
          <MetricCard
            title="Cached Investigations"
            value={cacheStats?.keys_stored ?? '—'}
            colour="blue"
            subtitle="Results available for instant replay"
            loading={cacheLoading && !cacheStats}
          />
        </div>
      </section>

    </div>
  );
}
