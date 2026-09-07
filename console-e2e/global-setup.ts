import { chromium, type FullConfig } from '@playwright/test';
import { execSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolveTarget } from './lib/target';

const COMPOSE = 'docker compose -f docker-compose.e2e.yml';
const SEED = '../scripts/data-integrity/seed/seed.py';

export default async function globalSetup(_config: FullConfig) {
  const target = resolveTarget(process.env);          // 🔴 守卫在最前，先于任何网络动作
  const isLocal = target.includes('127.0.0.1') || target.includes('localhost');

  if (isLocal) {
    execSync(`${COMPOSE} up -d --build --wait`, { stdio: 'inherit', cwd: __dirname });

    // 复用既有的 data-integrity seed —— 走真实游戏 API 造数据（账号 / storage / 好友）。
    // ⚠️ seed.py 里 API=7350、CONSOLE_PORT=7351 是硬编码的，compose 的端口必须与之一致。
    // ⚠️ 'defaultkey' 是 nakama 的默认 socket.server_key（server/config.go:908）。
    // 🔵 不传 --dsn-cmd：那部分要建榜，而本实例不加载游戏模块（见 lib/routes.ts 的说明）。
    execSync(`python3 ${SEED} defaultkey`, { stdio: 'inherit', cwd: __dirname });
  }

  const browser = await chromium.launch();
  const page = await browser.newPage();

  // 🔴 globalSetup 里出错，Playwright 一条测试都没跑 ⇒ 既没有 report 也没有 test-results
  //    ⇒ CI 上传的产物是【空的】。2026-09-07 首次 CI 运行就是这样失败的：只有一行
  //    "waitForURL 超时"，查不下去。所以这里的失败必须自己把证据留在盘上。
  const consoleErrors: string[] = [];
  const authResponses: string[] = [];
  page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()); });
  page.on('pageerror', (e) => consoleErrors.push(`PAGEERROR ${String(e)}`));
  page.on('requestfailed', (r) =>
    authResponses.push(`FAILED ${r.method()} ${r.url()} —— ${r.failure()?.errorText}`));
  page.on('response', async (r) => {
    if (!r.url().includes('/v2/console/')) return;
    let body = '';
    try { body = (await r.text()).slice(0, 300); } catch { body = '<读不到>'; }
    authResponses.push(`${r.status()} ${r.request().method()} ${r.url()} —— ${body}`);
  });

  try {
    await page.goto(`${target}/`);
    await page.getByRole('textbox', { name: 'Username' })
      .fill(process.env.CONSOLE_USER ?? 'admin');
    await page.getByRole('textbox', { name: 'Password' })
      .fill(process.env.CONSOLE_PASS ?? 'password');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await page.waitForURL(/#\/$/, { timeout: 20_000 });
  } catch (err) {
    await dumpLoginFailure(page, err, consoleErrors, authResponses, isLocal);
    throw err;
  }
  await page.context().storageState({ path: `${__dirname}/.auth.json` });

  // 详情路由要拼 id。🔵 从 console API 查，而不是解析 seed 的输出 ——
  //    这样不与 seed 的内部实现耦合。
  //
  // 🔴 不能靠 storageState：它存的是 cookie / localStorage，而 console API 认的是
  //    Authorization: Bearer。request.get 不会自动带上 —— 2026-09-04 实测直接返回
  //    {"code":16,"message":"Console authentication required."}
  //    ⇒ 显式调认证端点拿 token。
  const api = await browser.newContext();
  const authResp = await api.request.post(`${target}/v2/console/authenticate`, {
    data: {
      username: process.env.CONSOLE_USER ?? 'admin',
      password: process.env.CONSOLE_PASS ?? 'password',
    },
  });
  const authRaw = await authResp.text();
  let token = '';
  try { token = JSON.parse(authRaw)?.token ?? ''; } catch { /* 落到下面 */ }
  if (!token) {
    throw new Error(`console 认证失败。响应前 400 字：\n${authRaw.slice(0, 400)}`);
  }

  // 字段名是 users（console.proto 的 AccountList.users），不是 accounts
  const resp = await api.request.get(`${target}/v2/console/account?limit=20`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const raw = await resp.text();

  // 🔴 必须排除【系统用户】。nakama 在 initial_schema.sql:47 插了一个 id 全零的系统用户，
  //    而 ListAccounts 会把它排在最前 —— 2026-09-04 实测第一版取到的正是它。
  //    拿它去测详情路由毫无意义：它没有 storage / 好友 / 钱包，还可能有特殊行为。
  const SYSTEM_USER = '00000000-0000-0000-0000-000000000000';
  let accountId = '';
  try {
    const users: Array<{ id?: string }> = JSON.parse(raw)?.users ?? [];
    accountId = users.map((u) => u.id ?? '').find((id) => id && id !== SYSTEM_USER) ?? '';
  } catch { /* 落到下面的报错 */ }
  if (!accountId) {
    // 🔴 不静默跳过：取不到就是 seed 没造出账号，后面的详情路由会全错
    throw new Error(
      `seed 后仍取不到【非系统】账号。ListAccounts 响应前 500 字：\n${raw.slice(0, 500)}`,
    );
  }

  // 🔴 快照头部记 seed 的哈希：seed 一改，全部快照都会变，而那种变化【没有判读价值】。
  //    记下它，就能把「预期的全量重录」与「UI 变了」在 diff 里分开（spec §13 风险 3）。
  const seedHash = createHash('sha256')
    .update(readFileSync(`${__dirname}/${SEED}`))
    .digest('hex')
    .slice(0, 12);

  // 🔴 专供 ACL 写流程的测试账号 —— 绝不能改 admin 自己，改错会把自己锁在外面。
  // ⚠️ 不设 newsletter_subscription：为 true 时 AddUser 会 POST 到
  //    cloud.heroiclabs.com（F10-6 查证）。
  // 🔵 键必须是 console.AclResources 的枚举名（STORAGE_DATA 而非 storage）——
  //    acl.New() 对不认识的键是静默 continue（F10-6 实测）。
  const aclUser = 'e2e-acl-target';
  const mk = await api.request.post(`${target}/v2/console/user`, {
    headers: { Authorization: `Bearer ${token}` },
    data: {
      username: aclUser,
      email: `${aclUser}@example.invalid`,
      acl: { STORAGE_DATA: { read: true, write: false, delete: false } },
    },
  });
  if (!mk.ok()) {
    const t = await mk.text();
    // 已存在不算错（实例是一次性的，正常不会撞；但重跑时留着栈会）
    if (!t.includes('exists')) {
      throw new Error(`建 ACL 测试账号失败 ${mk.status()}：${t.slice(0, 300)}`);
    }
  }

  writeFileSync(`${__dirname}/fixtures.json`,
    JSON.stringify({ accountId, seedHash, aclUser }, null, 2));
  console.log(`[setup] accountId=${accountId}  seedHash=${seedHash}  aclUser=${aclUser}`);

  await browser.close();
}

/** 登录失败时把现场落盘到 test-results/ —— CI 的 upload-artifact 正好覆盖这个目录。 */
async function dumpLoginFailure(
  page: import('@playwright/test').Page,
  err: unknown,
  consoleErrors: string[],
  responses: string[],
  isLocal: boolean,
) {
  const dir = `${__dirname}/test-results`;
  mkdirSync(dir, { recursive: true });

  let serverLog = '（远端实例，不取日志）';
  if (isLocal) {
    try {
      serverLog = execSync(`${COMPOSE} logs --no-color --tail 200 nakama`,
        { cwd: __dirname, encoding: 'utf8' });
    } catch (e) { serverLog = `取日志失败: ${String(e)}`; }
  }

  const report = [
    `# globalSetup 登录失败`,
    ``,
    `错误: ${String(err).split('\n')[0]}`,
    `URL:  ${page.url()}`,
    `标题: ${await page.title().catch(() => '<取不到>')}`,
    ``,
    `## console API 响应 (${responses.length})`,
    ...(responses.length ? responses : ['（一条都没有 —— 请求根本没发出去）']),
    ``,
    `## 浏览器控制台错误 (${consoleErrors.length})`,
    ...(consoleErrors.length ? consoleErrors : ['（无）']),
    ``,
    `## nakama 服务端日志`,
    serverLog,
  ].join('\n');

  writeFileSync(`${dir}/login-failure.md`, report);
  await page.screenshot({ path: `${dir}/login-failure.png`, fullPage: true }).catch(() => {});
  await page.content()
    .then((html) => writeFileSync(`${dir}/login-failure.html`, html))
    .catch(() => {});
  console.error(`\n${report}\n`);   // 🔵 也打到 CI 日志：产物上传若也失败，日志里还有一份
}
