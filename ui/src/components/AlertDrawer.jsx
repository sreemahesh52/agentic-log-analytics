// AlertDrawer — sliding right-side panel that shows alert detail.
// Design decisions:
//   Fixed position: the drawer overlays the page without reflowing content.
//   CSS transform/transition: hardware-accelerated slide animation that does
//     not trigger layout recalculation — faster than animating left/right.
//   Backdrop: semi-transparent overlay behind the drawer so clicking outside
//     closes it — standard drawer UX pattern.
//   Fetch on open: we only call the API when isOpen=true AND alertId changes
//     so switching between alerts loads fresh data without stale cache issues.

import React, { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getAlertDetail, triggerInvestigation } from '../api/client.js';
import useApi from '../hooks/useApi.js';
import StatusBadge from './StatusBadge.jsx';
// Shared severity style and relative time — single source of truth.
// Changing colours/format in utils/ updates AlertDrawer, DevPanel, and RCADetail.
import { getSeverityStyle } from '../utils/severity.js';
import { formatRelative } from '../utils/time.js';

// Width of the drawer panel in pixels.
const DRAWER_WIDTH = 420;

// Inline badge pill — reused for CASCADE/SINGLE and anomaly type labels.
function BadgePill({ text, bgColor, textColor = '#fff' }) {
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
    }}>
      {text}
    </span>
  );
}

// A single labelled row in the detail section.
function DetailRow({ label, children }) {
  return (
    <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-start', marginBottom: '10px' }}>
      <span style={{ fontSize: '11px', color: '#888', minWidth: '110px', paddingTop: '2px', flexShrink: 0 }}>
        {label}
      </span>
      <span style={{ fontSize: '13px', color: '#212121' }}>{children}</span>
    </div>
  );
}

// Section heading inside the drawer.
function SectionHeading({ title }) {
  return (
    <div style={{
      fontSize: '11px',
      fontWeight: '700',
      color: '#555',
      textTransform: 'uppercase',
      letterSpacing: '0.06em',
      marginTop: '18px',
      marginBottom: '10px',
      paddingBottom: '4px',
      borderBottom: '1px solid #f0f0f0',
    }}>
      {title}
    </div>
  );
}

export default function AlertDrawer({ alertId, isOpen, onClose }) {
  const navigate = useNavigate();

  // useApi wraps the async getAlertDetail call with loading/error/data state.
  // immediate=false: we trigger the fetch manually when alertId changes.
  const { data: alert, loading, error, execute: fetchDetail } = useApi(
    () => getAlertDetail(alertId),
    { immediate: false },
  );

  // triggerLoading: true while POST /investigations/trigger is in flight.
  // triggerError: string message if the trigger call failed.
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerError, setTriggerError] = useState(null);

  // Fetch fresh data whenever the drawer opens for a new alertId.
  // useCallback stabilises the fetchDetail reference so it does not trigger
  // an infinite loop in the useEffect dependency array.
  const stableFetch = useCallback(() => {
    if (alertId && isOpen) {
      fetchDetail();
    }
  }, [alertId, isOpen]); // eslint-disable-line react-hooks/exhaustive-deps

  // Run the fetch when the drawer opens or the selected alert changes.
  useEffect(() => {
    stableFetch();
  }, [stableFetch]);

  // Close on Escape key — standard keyboard accessibility pattern.
  useEffect(() => {
    function handleKeyDown(e) {
      if (e.key === 'Escape' && isOpen) onClose();
    }
    document.addEventListener('keydown', handleKeyDown);
    // Cleanup: remove the listener when the component unmounts or isOpen changes.
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  // --- Render helpers ---

  function renderBody() {
    if (loading) {
      return (
        <div style={{ padding: '24px', textAlign: 'center', color: '#888', fontSize: '13px' }}>
          Loading…
        </div>
      );
    }
    if (error) {
      return (
        <div style={{ padding: '24px' }}>
          <p style={{ color: '#b71c1c', fontSize: '13px', margin: '0 0 12px' }}>
            Failed to load alert details.
          </p>
          <button
            onClick={() => fetchDetail()}
            style={{
              padding: '6px 12px',
              fontSize: '12px',
              background: '#1565c0',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
            }}
          >
            Retry
          </button>
        </div>
      );
    }
    if (!alert) return null;

    // getSeverityStyle returns { background, color } — same API as the old local constant.
    const severityStyle = getSeverityStyle(alert.severity);

    return (
      <div style={{ padding: '20px' }}>
        {/* ── Alert summary ── */}
        <SectionHeading title="Alert" />

        <DetailRow label="Service">
          <strong>{alert.service}</strong>
        </DetailRow>

        <DetailRow label="Severity">
          <BadgePill
            text={alert.severity}
            bgColor={severityStyle.background}
            textColor={severityStyle.color}
          />
        </DetailRow>

        <DetailRow label="Type">
          {alert.anomaly_type}
        </DetailRow>

        <DetailRow label="Cascade">
          {alert.is_cascade ? (
            <BadgePill text="CASCADE" bgColor="#6a1b9a" />
          ) : (
            <BadgePill text="SINGLE" bgColor="#757575" />
          )}
          {/* If cascade, list the affected services as a comma-separated string. */}
          {alert.is_cascade && alert.affected_services?.length > 0 && (
            <span style={{ fontSize: '11px', color: '#666', marginLeft: '8px' }}>
              ({alert.affected_services.join(', ')})
            </span>
          )}
        </DetailRow>

        <DetailRow label="Confidence">
          {alert.confidence != null
            ? `${Math.round(alert.confidence * 100)}%`
            : '—'}
        </DetailRow>

        <DetailRow label="Created">
          {/* title attr: native browser tooltip shows the absolute UTC timestamp on hover */}
          <span title={alert.created_at}>{formatRelative(alert.created_at)}</span>
        </DetailRow>

        {/* ── Compression section (only shown after Context Compressor runs) ── */}
        {/* was_compressed can be false (below threshold) or true (GPT called). */}
        {/* We show the section as long as original_log_count > 0 so operators can
            see how many logs were analysed even when compression was not needed.*/}
        {alert.original_log_count > 0 && (
          <>
            <SectionHeading title="Log Context" />

            <DetailRow label="Logs analysed">
              {alert.original_log_count}
            </DetailRow>

            {alert.was_compressed ? (
              <DetailRow label="Compression">
                {/* compression_ratio is compressed/original — lower = more compact. */}
                {/* (1 - ratio) * 100 gives the percentage reduced. */}
                {`${Math.round((1 - alert.compression_ratio) * 100)}% reduced`}
                <span style={{ fontSize: '11px', color: '#888', marginLeft: '6px' }}>
                  (ratio {alert.compression_ratio?.toFixed(2)})
                </span>
              </DetailRow>
            ) : (
              <DetailRow label="Compression">
                <span style={{ color: '#888', fontSize: '12px' }}>
                  Not needed (below token threshold)
                </span>
              </DetailRow>
            )}
          </>
        )}

        {/* ── Model section: GPT-4 (red) or GPT-3.5 (green) badge from rca_results. ── */}
        {/* model_used is populated after the RCA Agent completes its investigation. */}
        <SectionHeading title="Model" />
        {renderModelBadge(alert.model_used)}

        {/* ── RCA section ── */}
        <SectionHeading title="Investigation" />
        {renderRcaSection(alert)}
      </div>
    );
  }

  // Render the model badge based on which LLM was selected by the Model Router.
  // model_used is null until the RCA Agent completes its first investigation —
  // it is written to rca_results.model_used and joined by the alert detail endpoint.
  function renderModelBadge(modelUsed) {
    if (!modelUsed) {
      // No RCA has run yet — model selection happens during RCA, not before.
      return (
        <div style={{ fontSize: '12px', color: '#aaa', fontStyle: 'italic', marginBottom: '10px' }}>
          Awaiting RCA
        </div>
      );
    }
    if (modelUsed.includes('gpt-4')) {
      // Red badge: GPT-4 family — high capability, higher cost.
      return (
        <DetailRow label="Model">
          <BadgePill text="GPT-4" bgColor="#b71c1c" />
          <span style={{ fontSize: '11px', color: '#888', marginLeft: '8px' }}>
            {modelUsed}
          </span>
        </DetailRow>
      );
    }
    if (modelUsed.includes('gpt-3.5')) {
      // Green badge: GPT-3.5 family — sufficient for most alerts, lower cost.
      return (
        <DetailRow label="Model">
          <BadgePill text="GPT-3.5" bgColor="#2e7d32" />
          <span style={{ fontSize: '11px', color: '#888', marginLeft: '8px' }}>
            {modelUsed}
          </span>
        </DetailRow>
      );
    }
    // Unknown model variant — display the raw name without a colour-coded badge.
    // Handles future model families (e.g. gpt-5) gracefully without a code change.
    return <DetailRow label="Model">{modelUsed}</DetailRow>;
  }

  async function handleTriggerRca() {
    // alert.incident_id must be set for the trigger to work.
    // It is populated after the Alert Correlator runs (step 8).
    if (!alert?.incident_id) {
      setTriggerError('No incident linked to this alert yet. Wait for the Alert Correlator to run.');
      return;
    }

    setTriggerLoading(true);
    setTriggerError(null);

    try {
      const response = await triggerInvestigation(alert.incident_id);
      const { rca_id } = response.data;
      // Navigate to the RCADetail page immediately — the placeholder row
      // already exists so the page shows "In progress..." while the agent runs.
      navigate(`/investigations/${rca_id}`);
    } catch (err) {
      const message = err?.response?.data?.error?.message || 'Failed to trigger investigation.';
      setTriggerError(message);
    } finally {
      setTriggerLoading(false);
    }
  }

  function renderRcaSection(alert) {
    // No RCA yet: show the live Trigger RCA button.
    if (!alert.rca_id) {
      return (
        <div>
          <button
            onClick={handleTriggerRca}
            disabled={triggerLoading || !alert.incident_id}
            title={!alert.incident_id ? 'Waiting for Alert Correlator to link an incident' : 'Start an RCA investigation for this alert'}
            style={{
              padding: '7px 14px',
              fontSize: '12px',
              fontWeight: '600',
              background: triggerLoading ? '#e0e0e0' : '#1565c0',
              color: triggerLoading ? '#888' : '#fff',
              border: 'none',
              borderRadius: '4px',
              cursor: triggerLoading || !alert.incident_id ? 'not-allowed' : 'pointer',
              transition: 'background 0.15s ease',
            }}
          >
            {triggerLoading ? 'Triggering…' : 'Trigger RCA'}
          </button>
          {triggerError && (
            <div style={{ fontSize: '11px', color: '#b71c1c', marginTop: '6px' }}>
              {triggerError}
            </div>
          )}
        </div>
      );
    }

    // RCA failed: show a warning with the failure reason.
    if (alert.rca_status === 'failed') {
      return (
        <div style={{
          padding: '10px 12px',
          background: '#fff3e0',
          border: '1px solid #ffe0b2',
          borderRadius: '4px',
          fontSize: '12px',
          color: '#e65100',
        }}>
          Investigation Failed
        </div>
      );
    }

    // RCA exists: navigate to the full investigation detail page.
    // Using onClick+navigate instead of <a href> keeps navigation within
    // React Router so the SPA history stack is maintained correctly.
    return (
      <button
        onClick={() => navigate(`/investigations/${alert.rca_id}`)}
        style={{
          display: 'inline-block',
          padding: '7px 14px',
          fontSize: '12px',
          fontWeight: '600',
          background: '#1565c0',
          color: '#fff',
          borderRadius: '4px',
          border: 'none',
          cursor: 'pointer',
          textDecoration: 'none',
        }}
      >
        View Investigation →
      </button>
    );
  }

  // --- Main render ---

  return (
    <>
      {/* Backdrop: semi-transparent overlay behind the drawer.
          Clicking it calls onClose — standard drawer dismiss pattern.
          pointer-events: none when closed so clicks pass through to the page.*/}
      <div
        onClick={onClose}
        style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(0, 0, 0, 0.35)',
          zIndex: 99,
          // opacity transition creates a smooth fade-in/out effect.
          opacity: isOpen ? 1 : 0,
          // pointer-events none when hidden prevents the invisible overlay from
          // blocking clicks on the page content.
          pointerEvents: isOpen ? 'auto' : 'none',
          transition: 'opacity 0.2s ease',
        }}
      />

      {/* Drawer panel: slides in from the right edge of the viewport.
          translateX(DRAWER_WIDTH) when closed = fully off-screen to the right.
          translateX(0) when open = fully visible.*/}
      <div
        style={{
          position: 'fixed',
          right: 0,
          top: 0,
          height: '100vh',
          width: DRAWER_WIDTH,
          background: '#fff',
          // box-shadow creates depth separation from the page content.
          boxShadow: '-4px 0 16px rgba(0, 0, 0, 0.15)',
          zIndex: 100,
          // transform slides the panel on/off screen without affecting layout.
          transform: isOpen ? 'translateX(0)' : `translateX(${DRAWER_WIDTH}px)`,
          transition: 'transform 0.2s ease',
          overflowY: 'auto',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Drawer header: service name + close button */}
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '16px 20px',
          borderBottom: '1px solid #e0e0e0',
          background: '#fafafa',
          flexShrink: 0,
        }}>
          <span style={{ fontWeight: '700', fontSize: '15px', color: '#212121' }}>
            {alert?.service ?? 'Alert Detail'}
          </span>
          {/* × close button: accessible with aria-label */}
          <button
            onClick={onClose}
            aria-label="Close drawer"
            style={{
              background: 'none',
              border: 'none',
              fontSize: '20px',
              color: '#888',
              cursor: 'pointer',
              lineHeight: 1,
              padding: '0 4px',
            }}
          >
            ×
          </button>
        </div>

        {/* Drawer body: scrollable content */}
        <div style={{ flex: 1 }}>
          {renderBody()}
        </div>
      </div>
    </>
  );
}
