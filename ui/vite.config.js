import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],

  build: {
    outDir: 'dist',
  },

  server: {
    port: 3001,
    hmr: true,
    // In dev mode the browser talks to Vite on :3001.
    // The gateway runs on :8000 on a different origin, which the browser
    // would block as a CORS violation. This proxy rewrites any request
    // whose path starts with /api to http://localhost:8000, making it
    // same-origin from the browser's perspective.
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
});
