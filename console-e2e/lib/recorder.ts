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

export function normalizePath(url: string): string {
  return new URL(url).pathname.replace(UUID, '{id}');   // 查询串整体丢弃
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
