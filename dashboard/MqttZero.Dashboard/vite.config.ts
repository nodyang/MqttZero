import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'node:path'

export default defineConfig({
  base: '/mqttzero/',
  plugins: [vue()],
  build: {
    outDir: resolve(process.cwd(), '../../src/MqttZero.AspNetCore.Dashboard/wwwroot'),
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 2000
  }
})
