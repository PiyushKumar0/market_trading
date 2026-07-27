import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The engine serves `dist/` as static files mounted at "/"
// (engine.api.app._mount_dashboard_if_present), so asset URLs are emitted RELATIVE. The LAN page
// must be fully self-contained: no CDN, no external host of any kind (R10).
export default defineConfig({
  base: './',
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true },
})
