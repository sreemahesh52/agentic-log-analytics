// MetricCard — reusable card for displaying a single operational metric.
// Used 7 times across Dashboard.jsx with different props for each metric.
// Colour-coded values let operators scan the grid and spot problems instantly
// without reading every number — red is bad, green is good, orange is marginal.
// Loading state renders fixed-size grey skeleton blocks so the grid layout
// does not shift as cards load at different speeds.
// Props:
//   title — metric name (small, grey, uppercase header above the value)
//   value — formatted display string: "84%", "$0.0012", "40", "—"
//   subtitle — secondary context: "20 investigations today", "$0.0004 saved"
//   colour — 'green' | 'red' | 'blue' | 'orange' | 'grey' (semantic names)
//   loading — boolean: when true, renders skeleton placeholders

import React from 'react';

// Map semantic colour names to hex values.
// Callers use names ('green', 'orange') not hex — business logic stays in Dashboard,
// not in this presentational component.
const COLOUR_MAP = {
  green:  '#2e7d32',
  red:    '#b71c1c',
  blue:   '#1565c0',
  orange: '#e65100',
  grey:   '#757575',
};

export default function MetricCard({ title, value, subtitle, colour = 'grey', loading = false }) {
  // Resolve colour name to hex; unknown names fall back to grey.
  const valueColour = COLOUR_MAP[colour] ?? COLOUR_MAP.grey;

  return (
    <div style={{
      background: '#fff',
      border: '1px solid #e0e0e0',
      borderRadius: '8px',
      padding: '16px',
    }}>
      {/* Title: small, grey, uppercase — identifies the metric */}
      <div style={{
        fontSize: '11px',
        fontWeight: '600',
        color: '#888',
        textTransform: 'uppercase',
        letterSpacing: '0.06em',
        marginBottom: '8px',
      }}>
        {title}
      </div>

      {loading ? (
        // Skeleton: greyed blocks matching the expected value/subtitle dimensions.
        // Fixed dimensions prevent the card from shrinking while data is absent.
        <>
          <div style={{
            height: '28px',
            width: '60%',
            background: '#f0f0f0',
            borderRadius: '4px',
            marginBottom: '8px',
          }} />
          <div style={{
            height: '14px',
            width: '80%',
            background: '#f0f0f0',
            borderRadius: '4px',
          }} />
        </>
      ) : (
        <>
          {/* Value: large, bold, colour-coded by health threshold */}
          <div style={{
            fontSize: '28px',
            fontWeight: '700',
            color: valueColour,
            marginBottom: '6px',
            // lineHeight: 1.2 prevents clipping on tall characters like $, %
            lineHeight: 1.2,
          }}>
            {value}
          </div>

          {/* Subtitle: optional secondary context shown below the value */}
          {subtitle && (
            <div style={{ fontSize: '12px', color: '#888', lineHeight: 1.4 }}>
              {subtitle}
            </div>
          )}
        </>
      )}
    </div>
  );
}
