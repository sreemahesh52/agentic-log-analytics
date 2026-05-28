import React from 'react';

const STATUS_COLORS = {
  success: '#2e7d32',
  error: '#c62828',
  warning: '#e65100',
  info: '#1565c0',
  pending: '#616161',
};

export default function StatusBadge({ status, text }) {
  const background = STATUS_COLORS[status] ?? STATUS_COLORS.pending;

  return (
    <span
      style={{
        display: 'inline-block',
        padding: '3px 10px',
        borderRadius: '12px',
        fontSize: '12px',
        fontWeight: '600',
        color: '#ffffff',
        background,
        whiteSpace: 'nowrap',
      }}
    >
      {text}
    </span>
  );
}
