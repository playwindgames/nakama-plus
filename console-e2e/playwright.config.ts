import { defineConfig } from '@playwright/test';
import { resolveTarget } from './lib/target';

export default defineConfig({
  testDir: './tests',
  globalSetup: './global-setup.ts',
  globalTeardown: './global-teardown.ts',
  // 🔴 共用同一个实例，并行会互相干扰快照（写流程会改数据）
  fullyParallel: false,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: resolveTarget(process.env),
    storageState: './.auth.json',
  },
});
