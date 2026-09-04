import { test, expect } from '@playwright/test';
import { resolveTarget } from './target';

// 🔴 F22-1：nkmad 的 e2e 曾在「分析完宣布安全」之后，第一次实跑就打到 fd 的 SG 生产。
//    ⇒ 这里把「打哪」收敛到唯一一个函数，并为它写对照。
test.describe('resolveTarget 守卫', () => {
  test('默认走本地', () => {
    expect(resolveTarget({})).toBe('http://127.0.0.1:7351');
  });

  test('非本地目标被拒，除非显式放行', () => {
    expect(() => resolveTarget({ CONSOLE_URL: 'https://nkmad-dev-cluster.pwghub.com' }))
      .toThrow(/非本地目标/);
  });

  test('显式放行后可用非本地目标', () => {
    expect(resolveTarget({
      CONSOLE_URL: 'https://nkmad-dev-cluster.pwghub.com',
      ALLOW_REMOTE_CONSOLE: 'yes-i-mean-it',
    })).toBe('https://nkmad-dev-cluster.pwghub.com');
  });

  test('🔴 命中生产特征时无条件拒绝 —— 放行开关也不管用', () => {
    expect(() => resolveTarget({
      CONSOLE_URL: 'https://console-cluster-sg.nkmfd.pwglab.com:7351',
      ALLOW_REMOTE_CONSOLE: 'yes-i-mean-it',
    })).toThrow(/生产/);
  });

  test('本地但带端口也放行', () => {
    expect(resolveTarget({ CONSOLE_URL: 'http://127.0.0.1:7399' }))
      .toBe('http://127.0.0.1:7399');
  });
});
