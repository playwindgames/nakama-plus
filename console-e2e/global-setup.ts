import { chromium, type FullConfig } from '@playwright/test';
import { execSync } from 'node:child_process';
import { resolveTarget } from './lib/target';

const COMPOSE = 'docker compose -f docker-compose.e2e.yml';

export default async function globalSetup(_config: FullConfig) {
  const target = resolveTarget(process.env);          // 🔴 守卫在最前，先于任何网络动作
  const isLocal = target.includes('127.0.0.1') || target.includes('localhost');

  if (isLocal) {
    // --wait 让 compose 自己等 healthcheck，不必手写轮询
    execSync(`${COMPOSE} up -d --build --wait`, { stdio: 'inherit', cwd: __dirname });
  }

  // 登录一次，所有路由共用 —— 省掉 N 次登录
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(`${target}/`);
  await page.getByRole('textbox', { name: 'Username' })
    .fill(process.env.CONSOLE_USER ?? 'admin');
  await page.getByRole('textbox', { name: 'Password' })
    .fill(process.env.CONSOLE_PASS ?? 'password');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await page.waitForURL(/#\/$/, { timeout: 20_000 });   // 登录成功跳到 #/
  await page.context().storageState({ path: `${__dirname}/.auth.json` });
  await browser.close();
}
