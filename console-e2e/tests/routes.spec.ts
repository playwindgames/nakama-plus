import { test, expect } from '@playwright/test';
import { readFileSync } from 'node:fs';
import { ROUTES } from '../lib/routes';
import { startRecording, settle, ALLOWED_EXTERNAL } from '../lib/recorder';
import { resolveTarget } from '../lib/target';

const target = resolveTarget(process.env);
const fx = JSON.parse(readFileSync(`${__dirname}/../fixtures.json`, 'utf8'));

const fill = (s: string) => s.replace(/\{accountId\}/g, fx.accountId);

for (const route of ROUTES) {
  if (route.skip) {
    test.skip(`${route.name} —— ${route.skip}`, () => {});
    continue;
  }

  test(route.name, async ({ page }) => {
    // 🔵 每条路由一个独立 test ⇒ 全新页面 ⇒ 每次都是【整页加载】。
    //    这一点不是随手写的：extensions / setting 是应用初始化调用，只在整页加载时发；
    //    若复用同一页面只改 hash，它们第二次就不出现，快照随即不稳（2026-09-04 实测）。
    const stop = startRecording(page, target);
    await page.goto(fill(route.url));
    await settle(page);
    const rec = stop();

    // ── 层 1：硬判据，红了就是坏了 ─────────────────────────────
    // 🔴 已知问题【逐条】豁免，且只对本路由生效。
    //    未被任何 pattern 命中的错误照常报红 —— 判据本身没有放松。
    const known = route.knownConsoleErrors ?? [];
    const unexpected = rec.consoleErrors.filter(
      (e) => !known.some((k) => k.pattern.test(e)),
    );
    expect(unexpected, `${route.name} 有【未登记的】控制台错误`).toEqual([]);

    const outOfBounds = rec.externalOrigins.filter((o) => !ALLOWED_EXTERNAL.includes(o));
    expect(outOfBounds, `${route.name} 发出了白名单之外的外部请求`).toEqual([]);

    // 🔴 判据是「标题等于预期」，不是「找不到 404 字样」——
    //    这个 UI 根本不渲染 404 字样，不存在的路由会【保留上一页的标题】
    //    （#/this-route-does-not-exist → "Dashboard | Nakama"，2026-09-04 实测）。
    if (route.title) {
      expect(await page.title(), `${route.name} 标题不对 —— 多半是路由没了或 URL 变了`)
        .toBe(`${fill(route.title)} (127)`);
    }

    // 🔴 玩家详情类的七条路由标题完全相同，标题判据对它们无效 ⇒ 用端点当指纹。
    if (route.mustCall) {
      expect(rec.endpoints, `${route.name} 没有发出它的特征请求 ${route.mustCall}`)
        .toContain(route.mustCall);
    }

    // ── 层 2：快照，变了不等于坏了 ────────────────────────────
    // 🔴 toMatchSnapshot 只接受 string / Buffer（Playwright 类型定义原文），传对象不行。
    expect(JSON.stringify({
      seedHash: fx.seedHash,          // seed 一改全部快照都变，记下它以便区分（spec §13 风险 3）
      endpoints: rec.endpoints,
      externalOrigins: rec.externalOrigins,
    }, null, 2)).toMatchSnapshot(`${route.name}.json`);
  });
}

// 🔴 登录页必须单独测，而且要【清掉登录态】——否则会被直接重定向到 Dashboard。
//
// 为什么它不在 ROUTES 里：ROUTES 的每条都复用 globalSetup 存下的 storageState（已登录），
// 而登录页只有未登录时才可达。
//
// ⚠️ 这是本套件曾经的一个【结构性空洞】：那 14 个 heroiclabs 请求（1 个 RSS + 13 张图）
//    只在登录页发，而登录是在 globalSetup 里做的、那里没有 recorder
//    ⇒ 白名单判据在其余 21 条路由上【永远不会红】，守的东西根本没被访问。
//    2026-09-07 做 CSP 验收时发现：把白名单收紧成 [] 跑全量，21 条竟然全绿。
// 🔵 而 09-04 我"验证"这条阴性对照时用的是 `-g login`，那个过滤匹配不到任何 test，
//    Playwright 报 "No tests found"，我把它当成通过了。⇒ 绿灯不是证据，能红才是。
test.describe('login', () => {
  test.use({ storageState: { cookies: [], origins: [] } });   // 🔴 不带登录态

  test('login', async ({ page }) => {
    const stop = startRecording(page, target);
    await page.goto('#/login');
    await settle(page);
    const rec = stop();

    expect(await page.title(), '登录页标题不对 —— 多半被重定向了').toBe('Nakama');

    // 🔴 CSP 拦截 heroiclabs RSS 时，浏览器必然在控制台留下违规提示 ——
    //    这是 CSP【生效】的表现，不是缺陷。逐条登记，不放松判据本身。
    //    2026-09-07 实测三条，全部同源于一次被拒的请求，无其他错误混入：
    //      [0] Connecting to 'https://heroiclabs.com/heroic-news-recent-rss.xml' violates
    //          the following Content Security Policy directive: "connect-src 'self'".
    //      [1] Fetch API cannot load … Refused to connect …
    //      [2] TypeError: Failed to fetch      ← 组件对上面那次拒绝的反应
    //    ⚠️ 登记前逐条读过全文，没有「大概是 CSP 提示」就写例外 ——
    //       一个写得太宽的例外，和一个空转的断言效果一样。
    const CSP_EXPECTED = [
      /Content Security Policy/,
      /Refused to connect/,
      /TypeError: Failed to fetch/,
    ];
    const unexpected = rec.consoleErrors.filter((e) => !CSP_EXPECTED.some((p) => p.test(e)));
    expect(unexpected, 'login 有【CSP 拦截之外的】控制台错误').toEqual([]);

    const outOfBounds = rec.externalOrigins.filter((o) => !ALLOWED_EXTERNAL.includes(o));
    expect(outOfBounds, 'login 发出了白名单之外的外部请求').toEqual([]);

    expect(JSON.stringify({
      seedHash: fx.seedHash,
      endpoints: rec.endpoints,
      externalOrigins: rec.externalOrigins,
    }, null, 2)).toMatchSnapshot('login.json');
  });
});
