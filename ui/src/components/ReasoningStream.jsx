// ReasoningStream — renders RCA Agent reasoning steps either live (via SSE)
// or statically (from stored JSONB).
// Props:
//   rca_id: string — investigation UUID, used to build the SSE URL and filter events.
//   is_complete: boolean — true if status is 'success' or 'failed'; false when still running.
//   static_steps: array — pre-stored reasoning steps from GET /investigations/{rca_id}.
//   api_key: string — passed as ?api_key= query param to the SSE endpoint.
//                           Native EventSource cannot set custom headers — query param is
//                           the standard workaround for SSE authentication.
// Live mode (is_complete=false):
//   Opens EventSource to /api/v1/stream/{rca_id}?api_key={api_key}.
//   Steps arrive as JSON events and are appended to local state.
//   Each new step fades in with a 0.3 s CSS animation.
//   A pulsing blue dot ("Agent is reasoning...") shows between steps.
//   On {type:'complete'}: closes EventSource, switches to static toggle mode.
//   On timeout/error/disconnect: shows error notice, falls back to static_steps.
// Static mode (is_complete=true, or after stream ends):
//   Renders steps with a collapsible toggle (default: collapsed).
//   "Show reasoning (N steps)" / "Hide reasoning" toggle button.
// Graceful degradation:
//   Any SSE failure (network error, Kafka down, 401) → error notice shown,
//   static_steps displayed immediately. The UI is never broken by streaming failures.

import React, { useEffect, useRef, useState } from 'react';

// ---------------------------------------------------------------------------
// CSS — injected once at module load time (no CSS file needed).
// The style tag ID prevents duplicate injection if the module is hot-reloaded.
// ---------------------------------------------------------------------------

const _STYLE_TAG_ID = 'reasoning-stream-css';

const _ANIMATION_CSS = `
@keyframes sse-fade-in {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}
.sse-step-enter {
  animation: sse-fade-in 0.3s ease forwards;
}
@keyframes sse-pulse {
  0%, 100% { opacity: 0.3; transform: scale(1); }
  50%       { opacity: 1;   transform: scale(1.3); }
}
`;

// Inject at module load — runs before any React render so the animation class
// is defined before the first step card receives it.
if (typeof document !== 'undefined' && !document.getElementById(_STYLE_TAG_ID)) {
  const style = document.createElement('style');
  style.id = _STYLE_TAG_ID;
  style.textContent = _ANIMATION_CSS;
  document.head.appendChild(style);
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Duration after which the "new step" CSS class is removed.
// Must exceed the animation duration (0.3 s) plus a small buffer.
const STEP_ANIMATION_CLEAR_MS = 400;

// ---------------------------------------------------------------------------
// StepCard sub-component
// ---------------------------------------------------------------------------

// Renders one ReasoningStep: Thought → Action (with input) → Observation.
// isNew: when true, applies the fade-in CSS class (cleared after animation ends).
function StepCard({ step, isNew }) {
  return (
    <div
      className={isNew ? 'sse-step-enter' : ''}
      style={{
        borderLeft: '3px solid #1565c0',
        paddingLeft: '14px',
        marginBottom: '16px',
        position: 'relative',
      }}
    >
      {/* Timeline dot — marks the left border at the start of each step */}
      <div style={{
        position: 'absolute',
        left: '-8px',
        top: '4px',
        width: '12px',
        height: '12px',
        borderRadius: '50%',
        background: '#1565c0',
        border: '2px solid #fff',
      }} />

      {/* Step number label */}
      <div style={{ marginBottom: '6px' }}>
        <span style={{
          fontSize: '10px',
          fontWeight: '700',
          color: '#9e9e9e',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
        }}>
          Step {step.step_number}
        </span>
      </div>

      {/* Thought — grey italic, always visible */}
      {step.thought && (
        <div style={{
          fontSize: '12px',
          color: '#555',
          fontStyle: 'italic',
          lineHeight: '1.6',
          marginBottom: '8px',
        }}>
          💭 {step.thought}
        </div>
      )}

      {/* Action — blue pill badge + grey code block for action_input */}
      {step.action && (
        <div style={{ marginBottom: '8px' }}>
          <span style={{
            display: 'inline-block',
            padding: '2px 8px',
            borderRadius: '10px',
            fontSize: '10px',
            fontWeight: '700',
            background: '#e3f2fd',
            color: '#1565c0',
            marginBottom: '5px',
          }}>
            {step.action}
          </span>

          {step.action_input != null && (
            <div style={{
              background: '#f5f5f5',
              borderRadius: '4px',
              padding: '6px 10px',
              fontSize: '11px',
              fontFamily: 'monospace',
              color: '#333',
              wordBreak: 'break-all',
            }}>
              {typeof step.action_input === 'string'
                ? step.action_input
                : JSON.stringify(step.action_input, null, 2)}
            </div>
          )}
        </div>
      )}

      {/* Observation — scrollable pre block, max 180 px */}
      {step.observation && (
        <div>
          <span style={{
            fontSize: '10px',
            color: '#888',
            fontWeight: '600',
            display: 'block',
            marginBottom: '3px',
          }}>
            Observation:
          </span>
          <pre style={{
            background: '#f5f5f5',
            borderRadius: '4px',
            padding: '6px 10px',
            fontSize: '11px',
            fontFamily: 'monospace',
            color: '#33691e',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            maxHeight: '180px',
            overflowY: 'auto',
            margin: 0,
          }}>
            {step.observation}
          </pre>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// PulsingDot sub-component
// ---------------------------------------------------------------------------

// Shown between steps while the SSE stream is open to signal active reasoning.
// The CSS pulse animation is defined in _ANIMATION_CSS above.
function PulsingDot() {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      padding: '8px 0',
      fontSize: '12px',
      color: '#1565c0',
    }}>
      <span style={{
        display: 'inline-block',
        width: '10px',
        height: '10px',
        borderRadius: '50%',
        background: '#1565c0',
        // sse-pulse is defined in _ANIMATION_CSS — not inline style to keep it simple.
        animation: 'sse-pulse 1.2s ease-in-out infinite',
      }} />
      Agent is reasoning…
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main ReasoningStream component
// ---------------------------------------------------------------------------

export default function ReasoningStream({ rca_id, is_complete, static_steps, api_key }) {
  // liveSteps: steps received via the SSE connection, accumulated as events arrive.
  const [liveSteps, setLiveSteps] = useState([]);

  // streamFinished: true after {type:'complete'}, timeout, error, or disconnect.
  // Switches the component from live streaming mode to static toggle mode.
  const [streamFinished, setStreamFinished] = useState(false);

  // streamErrorMsg: human-readable notice shown when SSE fails unexpectedly.
  const [streamErrorMsg, setStreamErrorMsg] = useState(null);

  // expanded: controls the "Show / Hide reasoning" toggle in static mode.
  // Default collapsed so the summary card is immediately visible after completion.
  const [expanded, setExpanded] = useState(false);

  // newStepNumbers: set of step_number values that should play the fade-in animation.
  // Added on step arrival, removed after STEP_ANIMATION_CLEAR_MS via setTimeout.
  const [newStepNumbers, setNewStepNumbers] = useState(new Set());

  // esRef: holds the active EventSource so we can close it imperatively
  // on component unmount without depending on the stale closure's reference.
  const esRef = useRef(null);

  // --- SSE connection lifecycle ---
  // Opens when is_complete=false and api_key is truthy.
  // Closed on: {type:'complete'}, error, timeout, or component unmount.
  useEffect(() => {
    // Static display only for completed investigations — do not connect.
    if (is_complete) return;
    // Guard: if api_key is empty, the SSE endpoint would return 401 immediately.
    if (!api_key) return;

    // encodeURIComponent: handles special characters in API keys safely.
    // The query param approach is required because EventSource has no headers API.
    const url = `/api/v1/stream/${rca_id}?api_key=${encodeURIComponent(api_key)}`;

    // EventSource: browser-native SSE client. Automatically reconnects on network
    // errors unless we close it explicitly. We close on completion to prevent
    // duplicate step delivery on the automatic reconnect.
    const es = new EventSource(url);
    esRef.current = es;

    // onmessage fires for each "data: {...}\n\n" event from the server.
    es.onmessage = (event) => {
      let data;
      try {
        // SSE events carry a JSON string — parse before processing.
        data = JSON.parse(event.data);
      } catch {
        // Malformed event — skip. The stream continues normally.
        return;
      }

      if (data.type === 'step') {
        setLiveSteps(prev => {
          // De-duplicate by step_number: EventSource auto-reconnects and may
          // replay events. If we already have this step, skip the append.
          const alreadyHave = prev.some(s => s.step_number === data.step_number);
          if (alreadyHave) return prev;
          return [...prev, data];
        });

        // Mark as "new" so StepCard receives the fade-in CSS class.
        setNewStepNumbers(prev => new Set([...prev, data.step_number]));
        // Remove the flag after the animation duration so the class is not permanent.
        setTimeout(() => {
          setNewStepNumbers(prev => {
            const next = new Set(prev);
            next.delete(data.step_number);
            return next;
          });
        }, STEP_ANIMATION_CLEAR_MS);

      } else if (data.type === 'complete') {
        // Investigation finished — switch to static toggle mode.
        setStreamFinished(true);
        es.close();

      } else if (data.type === 'timeout') {
        // Server-side 60 s silence — investigation may have stalled silently.
        setStreamErrorMsg('Stream timed out. Showing stored steps.');
        setStreamFinished(true);
        es.close();

      } else if (data.type === 'error') {
        // Server-side Kafka or consumer error.
        setStreamErrorMsg('Stream error. Showing stored steps.');
        setStreamFinished(true);
        es.close();
      }
    };

    // onerror fires on: network failure, HTTP error (e.g. 401 auth), or server close.
    // EventSource auto-reconnects by default — we close explicitly to stop reconnects
    // and show the fallback immediately rather than retrying indefinitely.
    es.onerror = () => {
      setStreamErrorMsg('Stream disconnected. Showing stored steps.');
      setStreamFinished(true);
      es.close();
    };

    // Cleanup: close the EventSource when the component unmounts or rca_id changes.
    // Without this, the browser keeps a persistent HTTP connection to the server
    // even after the user navigates away — leaking a Kafka consumer group slot.
    return () => {
      es.close();
      esRef.current = null;
    };
  }, [rca_id, is_complete, api_key]);

  // ---------------------------------------------------------------------------
  // Derived state — decide what to render
  // ---------------------------------------------------------------------------

  // isStreaming: true while the SSE connection is open and accepting steps.
  // False when investigation is complete, stream has ended, or an error occurred.
  const isStreaming = !is_complete && !streamFinished;

  // displaySteps: select the correct source based on current mode.
  //   is_complete: use stored static_steps from the API (investigation already done).
  //   stream finished with live steps: use liveSteps (all steps received via SSE).
  //   stream finished but empty liveSteps: fall back to static_steps
  //     (edge case: user refreshed during investigation, SSE connected but
  //     immediately received an error before any steps arrived).
  //   still streaming: show liveSteps as they accumulate.
  const displaySteps = (() => {
    if (is_complete) return static_steps || [];
    if (streamFinished && liveSteps.length > 0) return liveSteps;
    if (streamFinished) return static_steps || [];
    return liveSteps;
  })();

  // Sort ascending by step_number to guarantee order regardless of Kafka delivery.
  // Kafka within a partition is ordered, but safe to sort defensively.
  const sortedSteps = [...displaySteps].sort(
    (a, b) => (a.step_number ?? 0) - (b.step_number ?? 0)
  );

  const stepCount = sortedSteps.length;

  // ---------------------------------------------------------------------------
  // Render — empty state
  // ---------------------------------------------------------------------------

  if (!isStreaming && stepCount === 0) {
    return (
      <div style={{
        background: '#fff',
        borderRadius: '8px',
        padding: '20px 24px',
        boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
        marginBottom: '20px',
      }}>
        <SectionLabel>Reasoning Timeline</SectionLabel>
        <p style={{ fontSize: '12px', color: '#aaa', margin: 0, fontStyle: 'italic' }}>
          No reasoning steps recorded.
        </p>
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // Render — live stream or static display
  // ---------------------------------------------------------------------------

  return (
    <div style={{
      background: '#fff',
      borderRadius: '8px',
      padding: '20px 24px',
      boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
      marginBottom: '20px',
    }}>
      {/* Section header row — title left, toggle button right */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '12px',
        paddingBottom: '4px',
        borderBottom: '1px solid #f0f0f0',
      }}>
        <span style={{
          fontSize: '11px',
          fontWeight: '700',
          color: '#555',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
        }}>
          Reasoning Timeline
          {/* "— live" indicator visible only while SSE is open */}
          {isStreaming && (
            <span style={{
              marginLeft: '8px',
              fontSize: '10px',
              color: '#1565c0',
              fontWeight: '600',
              textTransform: 'none',
              letterSpacing: 'normal',
            }}>
              — live
            </span>
          )}
        </span>

        {/* Toggle button — only available once streaming is finished */}
        {!isStreaming && stepCount > 0 && (
          <button
            onClick={() => setExpanded(e => !e)}
            style={{
              background: 'none',
              border: '1px solid #ccc',
              borderRadius: '4px',
              padding: '3px 10px',
              fontSize: '11px',
              cursor: 'pointer',
              color: '#555',
              fontWeight: '600',
            }}
          >
            {expanded
              ? 'Hide reasoning'
              : `Show reasoning (${stepCount} step${stepCount !== 1 ? 's' : ''})`
            }
          </button>
        )}
      </div>

      {/* Error notice — shown after SSE disconnect, timeout, or server error */}
      {streamErrorMsg && (
        <div style={{
          fontSize: '12px',
          color: '#e65100',
          marginBottom: '12px',
          padding: '6px 10px',
          background: '#fff3e0',
          borderRadius: '4px',
          border: '1px solid #ffe0b2',
        }}>
          ⚠️ {streamErrorMsg}
        </div>
      )}

      {/* Steps list — always shown while streaming; gated by toggle when done */}
      {(isStreaming || expanded) && (
        <div style={{ paddingLeft: '8px', paddingTop: '4px' }}>
          {sortedSteps.map((step) => (
            <StepCard
              // step_number is the natural key — unique per investigation.
              // Fallback to timestamp if step_number is missing (defensive).
              key={step.step_number ?? step.timestamp ?? Math.random()}
              step={step}
              isNew={newStepNumbers.has(step.step_number)}
            />
          ))}

          {/* Pulsing dot shown only while SSE is open, waiting for next step */}
          {isStreaming && <PulsingDot />}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// SectionLabel — small utility component reused for the heading style.
// Defined after the export so it is local to this file.
// ---------------------------------------------------------------------------

function SectionLabel({ children }) {
  return (
    <div style={{
      fontSize: '11px',
      fontWeight: '700',
      color: '#555',
      textTransform: 'uppercase',
      letterSpacing: '0.06em',
      marginBottom: '12px',
      paddingBottom: '4px',
      borderBottom: '1px solid #f0f0f0',
    }}>
      {children}
    </div>
  );
}
