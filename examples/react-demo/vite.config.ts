import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      'static-shard': path.resolve(__dirname, '../../dist/client/index.js')
    }
  }
})
