import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'node:path'
import { readFile, writeFile } from 'node:fs/promises'

const outputDirectory = resolve(
  process.cwd(),
  '../../src/MqttZero.AspNetCore.Dashboard/wwwroot'
)

function normalizeGeneratedIndex(): {
  name: string
  closeBundle: () => Promise<void>
} {
  return {
    name: 'mqttzero-normalize-generated-index',
    async closeBundle() {
      const indexPath = resolve(outputDirectory, 'index.html')
      const content = await readFile(indexPath, 'utf8')
      await writeFile(
        indexPath,
        content.replace(/\r+\n/g, '\n').replace(/\n/g, '\r\n')
      )
    }
  }
}

export default defineConfig({
  base: './',
  plugins: [vue(), normalizeGeneratedIndex()],
  build: {
    outDir: outputDirectory,
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 2000
  }
})
