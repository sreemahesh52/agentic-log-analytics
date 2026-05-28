// Spinner — CSS rotating arc used as a loading indicator in every table and section
// that fetches data. Replaces bare "Loading..." text with a visual signal.
// Implementation: a div styled as a circle where three border sides are transparent
// and one side is coloured. The spinner-rotate @keyframes rotates the element so
// the coloured segment appears to chase itself around the circle.
// Why not a GIF or SVG? Pure CSS keeps the bundle smaller and animates smoothly
// at 60fps on the GPU (CSS animation uses the compositor thread, not the main thread).
// Props:
//   size — 'small' (16 px) | 'medium' (32 px). Default: 'medium'.
//   label — optional text displayed beside the spinner.

import React from 'react';

// Inject @keyframes once at module load time.
// The ID check prevents duplicate injection on Vite hot-module reload.
const STYLE_TAG_ID = 'spinner-anim-css';

const SPINNER_CSS = `
@keyframes spinner-rotate {
  to { transform: rotate(360deg); }
}
`;

// typeof document guard makes the module safe if ever imported in an SSR context.
if (typeof document !== 'undefined' && !document.getElementById(STYLE_TAG_ID)) {
  const tag = document.createElement('style');
  tag.id = STYLE_TAG_ID;
  tag.textContent = SPINNER_CSS;
  // Inject into <head> so the keyframe is defined before any component renders it.
  document.head.appendChild(tag);
}

// SIZE_PX maps named sizes to pixel values — avoids magic numbers in render logic.
const SIZE_PX = { small: 16, medium: 32 };

export default function Spinner({ size = 'medium', label }) {
  // px: resolved pixel dimension for the spinner circle.
  const px = SIZE_PX[size] ?? SIZE_PX.medium;

  // borderWidth: proportional to size so the arc thickness looks consistent.
  // max(2, ...) ensures at least 2 px on the smallest size.
  const borderWidth = Math.max(2, Math.round(px / 8));

  return (
    // Inline-flex keeps the spinner and label on the same baseline row.
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '8px' }}>
      <span
        style={{
          display: 'inline-block',
          width: px,
          height: px,
          borderRadius: '50%',
          // Three sides transparent + one coloured = rotating arc visual.
          border: `${borderWidth}px solid #e0e0e0`,
          borderTopColor: '#1565c0',
          // spinner-rotate is defined in SPINNER_CSS above.
          animation: 'spinner-rotate 0.7s linear infinite',
          // flexShrink: 0 prevents the circle from being squashed by long labels.
          flexShrink: 0,
        }}
      />
      {/* label is optional — rendered only when provided */}
      {label && (
        <span style={{ fontSize: '12px', color: '#888' }}>{label}</span>
      )}
    </span>
  );
}
