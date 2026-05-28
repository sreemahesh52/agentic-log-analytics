// RCADetail — full investigation detail page at /investigations/:rca_id
// Design decisions:
//   Auto-refresh: when status is 'retried' (investigation in progress),
//     polls GET /api/v1/investigations/{rca_id} every 5 seconds until
//     status changes to 'success' or 'failed'. Stops polling on unmount.
//   Reasoning timeline: each ReasoningStep renders as a card in a vertical
//     timeline, showing Thought → Action → Observation per step.
//   Back navigation: uses react-router Link to="/" — reliable regardless of
//     whether the user arrived from AlertDrawer or directly via URL.
//   Confidence bar: visual fill bar 0–100% with colour thresholds.
//   Model badge: GPT-4 (red) / GPT-3.5 (green) consistent with AlertDrawer.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import Spinner from '../components/Spinner.jsx';
import { getApiKey, getInvestigation, labelAlert } from '../api/client.js';
// ReasoningStream replaces the static reasoning timeline:
//   - is_complete=false → connects to SSE, renders steps live as they arrive
//   - is_complete=true → renders stored steps with a collapsible toggle
import ReasoningStream from '../components/ReasoningStream.jsx';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Poll interval in ms when the investigation is still in progress.
const POLL_INTERVAL_MS = 5000;

// Confidence colour thresholds — matches the investigation quality bands.
const CONFIDENCE_COLOR = (conf) => {
  if (conf >= 0.85) return '#2e7d32'; // green — high confidence
  if (conf >= 0.6)  return '#f9a825'; // amber — moderate confidence
  return '#b71c1c';                   // red — low confidence
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

// PageHeader uses react-router Link instead of navigate(-1) / window.history.back
// because Link navigates reliably regardless of how the user arrived at this page.
// navigate(-1) fails if the user landed directly on this URL (e.g. from a bookmark
// or after a page refresh), where there is no previous history entry to go back to.
function PageHeader() {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '12px',
      padding: '16px 24px',
      borderBottom: '1px solid #e0e0e0',
      background: '#fafafa',
    }}>
      {/* Link component: same as <a href="/"> but keeps navigation inside React Router
          so the browser tab title and history stack update correctly.*/}
      <Link
        to="/"
        style={{
          display: 'inline-block',
          background: 'none',
          border: '1px solid #ccc',
          borderRadius: '4px',
          padding: '5px 12px',
          fontSize: '13px',
          color: '#555',
          textDecoration: 'none',
        }}
      >
        ← Back to Dev Panel
      </Link>
      <span style={{ fontWeight: '700', fontSize: '17px', color: '#212121' }}>
        RCA Investigation
      </span>
    </div>
  );
}

// Confidence percentage bar with colour coded fill.
function ConfidenceBar({ confidence }) {
  if (confidence == null) return null;
  const pct = Math.round(confidence * 100);
  const color = CONFIDENCE_COLOR(confidence);
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
        <span style={{ fontSize: '12px', color: '#555' }}>Confidence</span>
        <span style={{ fontSize: '13px', fontWeight: '700', color }}>{pct}%</span>
      </div>
      <div style={{
        height: '6px',
        background: '#e0e0e0',
        borderRadius: '3px',
        overflow: 'hidden',
      }}>
        <div style={{
          height: '100%',
          width: `${pct}%`,
          background: color,
          borderRadius: '3px',
          transition: 'width 0.4s ease',
        }} />
      </div>
    </div>
  );
}

// Inline badge pill — reused for status, model, cascade labels.
function BadgePill({ text, bgColor, textColor = '#fff', style = {} }) {
  return (
    <span style={{
      display: 'inline-block',
      padding: '2px 9px',
      borderRadius: '12px',
      fontSize: '11px',
      fontWeight: '700',
      whiteSpace: 'nowrap',
      background: bgColor,
      color: textColor,
      ...style,
    }}>
      {text}
    </span>
  );
}

// Model badge: GPT-4 (red) / GPT-3.5 (green) — same colours as AlertDrawer.
function ModelBadge({ modelUsed }) {
  if (!modelUsed || modelUsed === 'pending') {
    return <span style={{ fontSize: '12px', color: '#aaa', fontStyle: 'italic' }}>Pending</span>;
  }
  if (modelUsed.includes('gpt-4')) {
    return (
      <span>
        <BadgePill text="GPT-4" bgColor="#b71c1c" />
        <span style={{ fontSize: '11px', color: '#888', marginLeft: '8px' }}>{modelUsed}</span>
      </span>
    );
  }
  if (modelUsed.includes('gpt-3.5')) {
    return (
      <span>
        <BadgePill text="GPT-3.5" bgColor="#2e7d32" />
        <span style={{ fontSize: '11px', color: '#888', marginLeft: '8px' }}>{modelUsed}</span>
      </span>
    );
  }
  return <span style={{ fontSize: '13px' }}>{modelUsed}</span>;
}

// One row in the summary metadata section.
function MetaRow({ label, children }) {
  return (
    <div style={{ display: 'flex', gap: '12px', alignItems: 'flex-start', marginBottom: '10px' }}>
      <span style={{ fontSize: '11px', color: '#888', minWidth: '130px', paddingTop: '2px', flexShrink: 0 }}>
        {label}
      </span>
      <span style={{ fontSize: '13px', color: '#212121' }}>{children}</span>
    </div>
  );
}

// Section heading with bottom border.
function SectionHeading({ title }) {
  return (
    <div style={{
      fontSize: '11px',
      fontWeight: '700',
      color: '#555',
      textTransform: 'uppercase',
      letterSpacing: '0.06em',
      marginTop: '24px',
      marginBottom: '12px',
      paddingBottom: '4px',
      borderBottom: '1px solid #f0f0f0',
    }}>
      {title}
    </div>
  );
}

// Horizontal score bar reused for faithfulness and hallucination.
function ScoreBar({ label, score, passingThreshold = 0.7 }) {
  if (score == null) return null;
  const pct = Math.round(score * 100);
  const passing = score > passingThreshold;
  const color = passing ? '#2e7d32' : score > 0.4 ? '#f9a825' : '#b71c1c';
  return (
    <div style={{ marginBottom: '12px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
        <span style={{ fontSize: '12px', color: '#555' }}>{label}</span>
        <span style={{ fontSize: '13px', fontWeight: '700', color }}>{pct}%</span>
      </div>
      <div style={{ height: '6px', background: '#e0e0e0', borderRadius: '3px', overflow: 'hidden' }}>
        <div style={{
          height: '100%',
          width: `${pct}%`,
          background: color,
          borderRadius: '3px',
          transition: 'width 0.4s ease',
        }} />
      </div>
    </div>
  );
}

// EvalModeBadge — colour-coded by reliability tier.
// ground_truth = most reliable (actual human label available)
// similarity = moderate (compared against similar past incidents)
// heuristic = least reliable (derived from content patterns)
function EvalModeBadge({ evalMode }) {
  if (!evalMode) return null;
  // Spec colours: ground_truth=blue, similarity=purple, heuristic=orange.
  // These differ from generic SRE traffic-light colours because eval mode
  // describes methodology, not urgency.
  const colors = {
    ground_truth: { bg: '#4299e1', label: 'Ground Truth ✓' },
    similarity:   { bg: '#805ad5', label: 'Similarity ≈' },
    heuristic:    { bg: '#ed8936', label: 'Heuristic' },
  };
  const cfg = colors[evalMode] || { bg: '#757575', label: evalMode };
  return <BadgePill text={cfg.label} bgColor={cfg.bg} />;
}

// EvalScoresPanel — shows faithfulness, hallucination, cost, and passed status.
// Only rendered once investigation.eval_id is non-null (eval harness has processed it).
function EvalScoresPanel({ investigation }) {
  const { eval_id, faithfulness_score, hallucination_score, eval_mode,
          eval_cost_usd, eval_passed } = investigation;

  if (!eval_id) {
    return (
      <div style={{
        background: '#fff',
        borderRadius: '8px',
        padding: '20px 24px',
        boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
        marginBottom: '20px',
      }}>
        <SectionHeading title="Evaluation Scores" />
        <p style={{ fontSize: '12px', color: '#aaa', margin: 0, fontStyle: 'italic' }}>
          Evaluation pending — the eval harness will process this investigation shortly.
        </p>
      </div>
    );
  }

  return (
    <div style={{
      background: '#fff',
      borderRadius: '8px',
      padding: '20px 24px',
      boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
      marginBottom: '20px',
    }}>
      <SectionHeading title="Evaluation Scores" />

      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
        <span style={{ fontSize: '11px', color: '#888' }}>Eval mode:</span>
        <EvalModeBadge evalMode={eval_mode} />
        {eval_passed != null && (
          <BadgePill
            text={eval_passed ? '✓ PASSED' : '✗ FAILED'}
            bgColor={eval_passed ? '#2e7d32' : '#b71c1c'}
            style={{ marginLeft: '8px' }}
          />
        )}
      </div>

      <ScoreBar label="Faithfulness" score={faithfulness_score} passingThreshold={0.7} />
      <ScoreBar label="Hallucination (1.0 = none)" score={hallucination_score} passingThreshold={0.7} />

      {eval_cost_usd != null && (
        <div style={{
          marginTop: '12px',
          fontSize: '12px',
          color: '#555',
          display: 'flex',
          gap: '8px',
        }}>
          <span style={{ color: '#888' }}>Eval cost:</span>
          <span style={{ fontWeight: '600' }}>${eval_cost_usd.toFixed(6)} USD</span>
        </div>
      )}
    </div>
  );
}

// GroundTruthInput — allows operators to set a verified root cause on the linked alert.
// Only shown when the investigation has a linked alert (alert_ids[0] exists).
function GroundTruthInput({ alertId }) {
  const [groundTruth, setGroundTruth] = useState('');
  const [status, setStatus] = useState(null); // null | 'saving' | 'saved' | 'error'
  const [errorMsg, setErrorMsg] = useState('');

  if (!alertId) return null;

  const handleSubmit = async () => {
    const trimmed = groundTruth.trim();
    if (trimmed.length < 10) {
      setErrorMsg('Ground truth must be at least 10 characters.');
      return;
    }
    if (trimmed.length > 1000) {
      setErrorMsg('Ground truth must be at most 1000 characters.');
      return;
    }

    setStatus('saving');
    setErrorMsg('');

    try {
      await labelAlert(alertId, trimmed);
      setStatus('saved');
      setGroundTruth('');
    } catch (err) {
      setStatus('error');
      const code = err?.response?.data?.error?.code;
      setErrorMsg(code === 'ALERT_NOT_FOUND'
        ? 'Alert not found — cannot label.'
        : 'Failed to save ground truth. Please try again.');
    }
  };

  return (
    <div style={{
      background: '#fff',
      borderRadius: '8px',
      padding: '20px 24px',
      boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
      marginBottom: '20px',
    }}>
      <SectionHeading title="Set Ground Truth" />
      <p style={{ fontSize: '12px', color: '#888', margin: '0 0 12px 0' }}>
        Provide a verified root cause for this alert. This upgrades future faithfulness
        evaluations from heuristic to ground-truth tier (most reliable).
      </p>

      <textarea
        value={groundTruth}
        onChange={e => { setGroundTruth(e.target.value); setStatus(null); setErrorMsg(''); }}
        placeholder="Enter verified root cause (min 10 characters)…"
        rows={4}
        style={{
          width: '100%',
          boxSizing: 'border-box',
          fontSize: '13px',
          padding: '8px 10px',
          border: '1px solid #ccc',
          borderRadius: '4px',
          fontFamily: 'inherit',
          resize: 'vertical',
          color: '#212121',
        }}
      />

      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginTop: '10px' }}>
        <button
          onClick={handleSubmit}
          disabled={status === 'saving'}
          style={{
            padding: '7px 18px',
            background: status === 'saving' ? '#90caf9' : '#1565c0',
            color: '#fff',
            border: 'none',
            borderRadius: '4px',
            cursor: status === 'saving' ? 'not-allowed' : 'pointer',
            fontSize: '13px',
            fontWeight: '600',
          }}
        >
          {status === 'saving' ? 'Saving…' : 'Save Ground Truth'}
        </button>

        {status === 'saved' && (
          <span style={{ fontSize: '12px', color: '#2e7d32' }}>
            ✓ Ground truth saved — future evaluations will use ground_truth tier.
          </span>
        )}
        {(status === 'error' || errorMsg) && (
          <span style={{ fontSize: '12px', color: '#b71c1c' }}>
            {errorMsg || 'An error occurred.'}
          </span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main RCADetail component
// ---------------------------------------------------------------------------

export default function RCADetail() {
  const { rca_id } = useParams();

  const [investigation, setInvestigation] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Set browser tab title using the first 8 chars of the RCA ID — unique enough
  // to identify the investigation in multiple tabs. Reset on unmount.
  useEffect(() => {
    const shortId = rca_id ? rca_id.slice(0, 8) : '…';
    document.title = `RCA #${shortId} — Agentic Log Analytics`;
    return () => { document.title = 'Agentic Log Analytics'; };
  }, [rca_id]);

  // pollRef: keeps track of the interval so we can cancel it on unmount
  // and when polling is no longer needed (status != 'retried').
  const pollRef = useRef(null);

  const fetchInvestigation = useCallback(async () => {
    try {
      const response = await getInvestigation(rca_id);
      const data = response.data;
      setInvestigation(data);
      setError(null);
      setLoading(false);

      // Stop polling when the investigation is no longer "in progress".
      // status='retried' with failure_reason='pending' = still running.
      const isInProgress = data.status === 'retried' && data.failure_reason === 'pending';
      if (!isInProgress && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    } catch (err) {
      setError(err);
      setLoading(false);
    }
  }, [rca_id]);

  useEffect(() => {
    // Initial fetch.
    fetchInvestigation();

    // Start polling — if the investigation is already complete, the first
    // fetch will clear the interval immediately via the isInProgress check above.
    pollRef.current = setInterval(fetchInvestigation, POLL_INTERVAL_MS);

    // Cleanup: always clear the interval on unmount to prevent state updates
    // on an unmounted component ("Can't perform a state update on an unmounted...").
    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [fetchInvestigation]);

  // --- Loading state ---
  if (loading) {
    return (
      <div style={{ minHeight: '100vh', background: '#f5f5f5' }}>
        <PageHeader />
        <div style={{ padding: '40px', textAlign: 'center' }}>
          <Spinner size="medium" label="Loading investigation…" />
        </div>
      </div>
    );
  }

  // --- Error state ---
  if (error) {
    return (
      <div style={{ minHeight: '100vh', background: '#f5f5f5' }}>
        <PageHeader />
        <div style={{ padding: '24px' }}>
          <p style={{ color: '#b71c1c', fontSize: '13px', marginBottom: '12px' }}>
            Failed to load investigation.
          </p>
          <button
            onClick={fetchInvestigation}
            style={{
              padding: '6px 14px',
              background: '#1565c0',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '12px',
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!investigation) return null;

  const isInProgress = investigation.status === 'retried' && investigation.failure_reason === 'pending';
  const isFailed = investigation.status === 'failed';

  // is_complete drives the ReasoningStream mode: static toggle vs live SSE.
  // 'retried' with failure_reason='pending' = in-progress → SSE connects.
  // 'success' or 'failed' = done → render stored steps with toggle.
  const is_complete = investigation.status === 'success' || investigation.status === 'failed';

  return (
    <div style={{ minHeight: '100vh', background: '#f5f5f5' }}>
      <PageHeader />

      <div style={{ maxWidth: '860px', margin: '0 auto', padding: '24px' }}>

        {/* ── In-progress banner ── */}
        {isInProgress && (
          <div style={{
            padding: '12px 16px',
            background: '#e3f2fd',
            border: '1px solid #90caf9',
            borderRadius: '6px',
            marginBottom: '20px',
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            fontSize: '13px',
            color: '#1565c0',
          }}>
            <span style={{ fontSize: '16px' }}>⏳</span>
            <span>Investigation in progress — this page auto-refreshes every 5 seconds.</span>
          </div>
        )}

        {/* ── Failed banner ── */}
        {isFailed && (
          <div style={{
            padding: '12px 16px',
            background: '#fff3e0',
            border: '1px solid #ffe0b2',
            borderRadius: '6px',
            marginBottom: '20px',
            fontSize: '13px',
            color: '#e65100',
          }}>
            ⚠️ Investigation failed — <strong>{investigation.failure_reason || 'unknown reason'}</strong>.
            Check the DLQ for details.
          </div>
        )}

        {/* ── Summary card ── */}
        <div style={{
          background: '#fff',
          borderRadius: '8px',
          padding: '20px 24px',
          boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
          marginBottom: '20px',
        }}>
          <SectionHeading title="Summary" />

          <MetaRow label="RCA ID">
            <code style={{ fontSize: '11px', background: '#f5f5f5', padding: '2px 5px', borderRadius: '3px' }}>
              {investigation.rca_id}
            </code>
          </MetaRow>

          <MetaRow label="Incident ID">
            <code style={{ fontSize: '11px', background: '#f5f5f5', padding: '2px 5px', borderRadius: '3px' }}>
              {investigation.incident_id}
            </code>
          </MetaRow>

          <MetaRow label="Status">
            {isInProgress
              ? <BadgePill text="IN PROGRESS" bgColor="#1565c0" />
              : isFailed
                ? <BadgePill text="FAILED" bgColor="#b71c1c" />
                : <BadgePill text="SUCCESS" bgColor="#2e7d32" />
            }
          </MetaRow>

          <MetaRow label="Model">
            <ModelBadge modelUsed={investigation.model_used} />
          </MetaRow>

          <MetaRow label="Prompt Version">
            {investigation.prompt_version || '—'}
          </MetaRow>

          <MetaRow label="Created">
            {investigation.created_at
              ? new Date(investigation.created_at).toLocaleString()
              : '—'}
          </MetaRow>

          {/* Confidence bar — only show for successful investigations */}
          {!isInProgress && !isFailed && investigation.confidence != null && (
            <div style={{ marginTop: '12px' }}>
              <ConfidenceBar confidence={investigation.confidence} />
            </div>
          )}
        </div>

        {/* ── Root cause ── */}
        {!isInProgress && investigation.root_cause && (
          <div style={{
            background: '#fff',
            borderRadius: '8px',
            padding: '20px 24px',
            boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
            marginBottom: '20px',
          }}>
            <SectionHeading title="Root Cause" />
            <p style={{ fontSize: '14px', color: '#212121', lineHeight: '1.6', margin: 0 }}>
              {investigation.root_cause}
            </p>
          </div>
        )}

        {/* ── Recommendations ── */}
        {!isInProgress && investigation.recommendations?.length > 0 && (
          <div style={{
            background: '#fff',
            borderRadius: '8px',
            padding: '20px 24px',
            boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
            marginBottom: '20px',
          }}>
            <SectionHeading title="Recommendations" />
            <ol style={{ margin: 0, paddingLeft: '20px' }}>
              {investigation.recommendations.map((rec, i) => (
                <li key={i} style={{ fontSize: '13px', color: '#212121', marginBottom: '6px', lineHeight: '1.5' }}>
                  {rec}
                </li>
              ))}
            </ol>
          </div>
        )}

        {/* ── Performance metrics ── */}
        {!isInProgress && (
          <div style={{
            background: '#fff',
            borderRadius: '8px',
            padding: '20px 24px',
            boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
            marginBottom: '20px',
          }}>
            <SectionHeading title="Performance" />

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '16px' }}>
              {[
                { label: 'Total Latency', value: `${investigation.total_latency_ms ?? 0} ms` },
                { label: 'LLM Latency',   value: `${investigation.llm_latency_ms ?? 0} ms` },
                { label: 'Tool Latency',  value: `${investigation.tool_latency_ms ?? 0} ms` },
                { label: 'Input Tokens',  value: (investigation.input_tokens ?? 0).toLocaleString() },
                { label: 'Output Tokens', value: (investigation.output_tokens ?? 0).toLocaleString() },
                { label: 'Compression',   value: investigation.compression_ratio != null
                    ? `${Math.round((1 - investigation.compression_ratio) * 100)}% reduced`
                    : '—' },
              ].map(({ label, value }) => (
                <div key={label} style={{
                  background: '#fafafa',
                  borderRadius: '6px',
                  padding: '10px 14px',
                  textAlign: 'center',
                }}>
                  <div style={{ fontSize: '11px', color: '#888', marginBottom: '4px' }}>{label}</div>
                  <div style={{ fontSize: '15px', fontWeight: '700', color: '#212121' }}>{value}</div>
                </div>
              ))}
            </div>

            {investigation.cache_hit && (
              <div style={{
                marginTop: '12px',
                padding: '6px 12px',
                background: '#e8f5e9',
                borderRadius: '4px',
                fontSize: '12px',
                color: '#2e7d32',
                display: 'inline-block',
              }}>
                ✓ Served from semantic cache (zero token cost)
              </div>
            )}
          </div>
        )}

        {/* ── Evaluation Scores ── */}
        {!isInProgress && (
          <EvalScoresPanel investigation={investigation} />
        )}

        {/* ── Set Ground Truth ── */}
        {!isInProgress && investigation.alert_ids?.length > 0 && (
          <GroundTruthInput alertId={investigation.alert_ids[0]} />
        )}

        {/* ── Reasoning Timeline (live via SSE or static from stored JSONB) ── */}
        {/* ReasoningStream handles both modes:
              is_complete=false → opens EventSource, shows steps as they arrive
              is_complete=true → renders stored steps with Show/Hide toggle
            Rendered unconditionally so SSE connects immediately when in-progress.*/}
        <ReasoningStream
          rca_id={rca_id}
          is_complete={is_complete}
          static_steps={investigation.reasoning_steps || []}
          api_key={getApiKey()}
        />
      </div>
    </div>
  );
}
