import { fileURLToPath, URL } from 'node:url'
import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

const rootDir = fileURLToPath(new URL('.', import.meta.url))
const embeddedDist = fileURLToPath(new URL('../internal/api/dist', import.meta.url))

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, rootDir, '')
  return {
    root: rootDir,
    plugins: [react()],
    resolve: {
      alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
    },
    server: {
      host: '127.0.0.1',
      port: 5173,
      strictPort: true,
      proxy: {
        '/api': {
          target: env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8060',
          changeOrigin: true,
        },
      },
    },
    build: {
      outDir: env.VITE_OUT_DIR || embeddedDist,
      emptyOutDir: true,
      sourcemap: false,
      assetsDir: 'assets',
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (!id.includes('/node_modules/')) return
            if (/\/node_modules\/(?:react|react-dom|scheduler|react-router|react-router-dom)\//.test(id)) return 'react-vendor'
            if (id.includes('/node_modules/@tanstack/')) return 'query-vendor'
          },
        },
      },
    },
    test: {
      environment: 'jsdom',
      setupFiles: ['./src/test/setup.ts'],
      restoreMocks: true,
      clearMocks: true,
    },
  }
})
