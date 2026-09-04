const LOCAL = 'http://127.0.0.1:7351';

// 🔴 生产特征 —— 命中即无条件拒绝，没有放行开关。
//
// 依据（2026-09-03 / 09-04 实地踩过两次）：
//   · F22-1：nkmad 的 e2e 在「分析完宣布安全」之后，第一次实跑就打到了 fd 的 SG 生产。
//     成因是只查了「配置怎么被选」，没查「谁在调用它」。
//   · F22：ad 仓里躺着一份名为 "Local Environment" 而 baseUrl 指向 fd 生产的配置。
//   · 2026-09-04：~/.config/nkmfd/console.env 里的凭据指向
//     console-cluster-sg.nkmfd.pwglab.com —— 名字看不出是生产。
//
// ⚠️ 这份清单宁可过宽：误拒只是跑不了，误放是打到生产。
const PROD_PATTERNS = [/prod/i, /console-cluster/i, /pwglab\.com/i];

export function resolveTarget(
  env: NodeJS.ProcessEnv | Record<string, string | undefined>,
): string {
  const url = (env.CONSOLE_URL ?? '').trim();
  if (!url) return LOCAL;

  for (const p of PROD_PATTERNS) {
    if (p.test(url)) {
      throw new Error(`拒绝：目标疑似生产（命中 ${p}）：${url}。此项无放行开关。`);
    }
  }

  let host: string;
  try {
    host = new URL(url).hostname;
  } catch {
    throw new Error(`CONSOLE_URL 不是合法 URL：${url}`);
  }

  const isLocal = host === '127.0.0.1' || host === 'localhost' || host === '::1';
  if (!isLocal && env.ALLOW_REMOTE_CONSOLE !== 'yes-i-mean-it') {
    throw new Error(
      `拒绝：非本地目标 ${url}。确需远程请设 ALLOW_REMOTE_CONSOLE=yes-i-mean-it`,
    );
  }
  return url;
}
