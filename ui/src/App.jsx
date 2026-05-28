import React from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import DevPanel from './pages/DevPanel.jsx';
import RCADetail from './pages/RCADetail.jsx';
import Dashboard from './pages/Dashboard.jsx';

// NavBar: always-visible top navigation rendered outside <Routes> so it appears
// on every page — including RCADetail which is a full-screen overlay page.
// Why NavLink not Link?
//   NavLink injects an isActive boolean into its style/className callback.
//   We use it to bold the active link and add a bottom border — a standard
//   tab UI pattern. Link does not provide active state detection.
// Why position:sticky?
//   Keeps the nav visible as users scroll long alert or log lists without
//   forcing a fixed-height layout that would require padding compensation.
function NavBar() {
  // navLinkStyle: function called by NavLink on every render.
  // isActive is true when the current URL matches this link's `to` prop.
  const navLinkStyle = ({ isActive }) => ({
    textDecoration: 'none',
    fontWeight: isActive ? '700' : '400',
    color: isActive ? '#1565c0' : '#555',
    fontSize: '14px',
    padding: '4px 0',
    // Bottom border: visual tab indicator for the active page.
    borderBottom: isActive ? '2px solid #1565c0' : '2px solid transparent',
    transition: 'border-color 0.15s ease, color 0.15s ease',
  });

  return (
    <nav style={{
      background: '#fff',
      borderBottom: '1px solid #e0e0e0',
      padding: '12px 20px',
      display: 'flex',
      alignItems: 'center',
      gap: '24px',
      // sticky: stays at top of viewport while the user scrolls page content.
      position: 'sticky',
      top: 0,
      // zIndex 50: above most page content but below AlertDrawer (zIndex 100).
      zIndex: 50,
    }}>
      {/* App brand label — not a link, just an identifier */}
      <span style={{ fontSize: '13px', fontWeight: '700', color: '#212121', marginRight: '4px' }}>
        Log Analytics
      </span>

      {/* "end" prop: prevents "/" from matching as a prefix of every route.
          Without "end", Dev Panel would be active on /dashboard too.*/}
      <NavLink to="/" end style={navLinkStyle}>Dev Panel</NavLink>
      <NavLink to="/dashboard" style={navLinkStyle}>Dashboard</NavLink>
    </nav>
  );
}

export default function App() {
  return (
    // BrowserRouter provides the React Router context (history, location, params)
    // to all descendant components via React context.
    <BrowserRouter>
      {/* NavBar is outside Routes so it renders regardless of which route is active */}
      <NavBar />
      <Routes>
        <Route path="/" element={<DevPanel />} />
        {/* RCA investigation detail page — navigated to from AlertDrawer
            after Trigger RCA or by clicking View Investigation button.*/}
        <Route path="/investigations/:rca_id" element={<RCADetail />} />
        {/* Dashboard: operational metrics page (Step 18a) */}
        <Route path="/dashboard" element={<Dashboard />} />
      </Routes>
    </BrowserRouter>
  );
}
