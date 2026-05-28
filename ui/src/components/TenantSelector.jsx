import React, { useState } from 'react';
import { setApiKey } from '../api/client.js';

const TENANTS = [
  { apiKey: 'acme-api-key-2024', label: 'acme-corp (premium)' },
  { apiKey: 'startup-api-key-2024', label: 'startup-co (standard)' },
];

export default function TenantSelector() {
  const [selected, setSelected] = useState(TENANTS[0].apiKey);

  function handleChange(e) {
    const key = e.target.value;
    setSelected(key);
    setApiKey(key);
  }

  const selectedTenant = TENANTS.find((t) => t.apiKey === selected);

  return (
    <div>
      <select
        value={selected}
        onChange={handleChange}
        style={{
          padding: '6px 10px',
          fontSize: '14px',
          border: '1px solid #ccc',
          borderRadius: '4px',
          cursor: 'pointer',
        }}
      >
        {TENANTS.map((t) => (
          <option key={t.apiKey} value={t.apiKey}>
            {t.label}
          </option>
        ))}
      </select>
      <div style={{ fontSize: '11px', color: '#666', marginTop: '3px' }}>
        Active: {selectedTenant?.label}
      </div>
    </div>
  );
}
