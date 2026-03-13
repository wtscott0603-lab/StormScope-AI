import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
  },
  build: {
    // MapLibre GL is intentionally isolated into its own production chunk.
    chunkSizeWarningLimit: 1200,
    rollupOptions: {
      output: {
        manualChunks: {
          map: ['maplibre-gl'],
          react: ['react', 'react-dom'],
          icons: ['lucide-react'],
        },
      },
    },
  },
})
