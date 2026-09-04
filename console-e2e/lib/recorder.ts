import type { Page } from '@playwright/test';

export type Recording = {
  endpoints: string[];
  externalOrigins: string[];
  consoleErrors: string[];
};

// 🔴 允许的外部来源 = 现状。2026-09-03 实测：登录页会向 heroiclabs.com 发 14 个请求
//    （1 个 heroic-news-recent-rss.xml + 13 张图）。
//    CSP 落地后把这里收紧成空数组，这条判据就变成 CSP 的验收（spec §12 判据 7）。
export const ALLOWED_EXTERNAL: string[] = ['https://heroiclabs.com'];

// 🔴 全应用轮询的端点，必须排除，否则快照每次都不一样。
//    2026-09-04 实测：/v2/console/status 在 #/players、#/storage、#/settings/config 里
//    出现，在 #/matches、#/players/{id}/friends 里不出现 —— 纯粹是轮询周期落在
//    采样窗口内外。⚠️ spec §5 把它记成「Dashboard 轮询」是【范围判断错了】，
//    它是全应用的，不只 Dashboard。
const POLLED = ['/v2/console/status'];

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

// 🔴 storage 的 version 是 32 位十六进制，且【每次实例都不同】——
//    2026-09-04 实测两轮：8807e8d418f4ad13f6a3a825d6e5e66e → 2c690d6c8570e9366ba5da78f374bb7e
//    内容完全一样（同一份 seed），说明它不是纯内容哈希、含随机量。
//    ⚠️ spec §5 的归一化清单列了 UUID / 时间戳 / 请求体的值 / 轮询，【漏了版本号】。
// 🔵 归一化不影响 OCC 判据：那条靠【路径段数】区分 4 段/5 段变体，
//    把 version 换成 {version} 仍然是 7 段。
const VERSION = /\/[0-9a-f]{32}(?=\/|$)/gi;

export function normalizePath(url: string): string {
  return new URL(url).pathname               // 查询串整体丢弃
    .replace(UUID, '{id}')
    .replace(VERSION, '/{version}');
}

export function startRecording(page: Page, selfOrigin: string): () => Recording {
  // 🔴 用集合，不记次数与顺序 —— 轮询与并发让这两者每次都不同。
  const endpoints = new Set<string>();
  const external = new Set<string>();
  const errors: string[] = [];

  page.on('request', (r) => {
    const url = r.url();
    if (url.startsWith(selfOrigin)) {
      const p = normalizePath(url);
      if (p.startsWith('/v2/console/') && !POLLED.includes(p)) {
        endpoints.add(`${r.method()} ${p}`);
      }
    } else if (url.startsWith('http')) {
      external.add(new URL(url).origin);
    }
  });
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('pageerror', (e) => errors.push(String(e)));

  return () => ({
    endpoints: [...endpoints].sort(),
    externalOrigins: [...external].sort(),
    consoleErrors: errors,
  });
}

// 🔴 SPA 的异步请求在 networkidle 之后才发，直接收网会把请求算到下一条路由头上。
//    2026-09-04 实测过这个错位：#/settings/config 记到了 /v2/console/runtime，
//    #/players/{id}/friends 记到了 /v2/console/config。
//    ⇒ networkidle 之后必须再等一个沉降窗口。
export async function settle(page: Page): Promise<void> {
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(2500);
}

export type BodyRecord = { method: string; path: string; fields: string[] };

/**
 * 只记请求体的【字段名】，不记值 —— 值里有 uuid、时间戳、随机串，入快照就是噪声。
 *
 * 🔴 这是写流程独有的信息：路由扫描只在页面加载时收网，而写请求只有点了按钮才发。
 */
export function startBodyRecording(page: Page, selfOrigin: string): () => BodyRecord[] {
  const out: BodyRecord[] = [];
  page.on('request', (r) => {
    const url = r.url();
    if (!url.startsWith(selfOrigin)) return;
    if (!['POST', 'PUT', 'DELETE', 'PATCH'].includes(r.method())) return;
    const p = normalizePath(url);
    if (!p.startsWith('/v2/console/')) return;
    let fields: string[] = [];
    try {
      const raw = r.postData();
      if (raw) fields = Object.keys(JSON.parse(raw)).sort();
    } catch {
      fields = ['<非 JSON>'];
    }
    out.push({ method: r.method(), path: p, fields });
  });
  return () => out;
}
