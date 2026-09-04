import { execSync } from 'node:child_process';

export default async function globalTeardown() {
  if (process.env.KEEP_STACK === '1') return;          // 排障时留着实例
  if (process.env.CONSOLE_URL) return;                 // 不是我们起的栈，不要去停
  execSync('docker compose -f docker-compose.e2e.yml down -v',
    { stdio: 'inherit', cwd: __dirname });
}
