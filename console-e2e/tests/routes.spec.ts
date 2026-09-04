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
