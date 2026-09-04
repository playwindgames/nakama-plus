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

/**
 * 🔴 写流程专用：**无条件要求本地**，`ALLOW_REMOTE_CONSOLE` 对它不生效。
 *
 * 为什么比 resolveTarget 更严：本套件的 4 条写流程会【真的改数据】——
 *   · DeleteStorageObject 真的删掉一个 storage 对象
 *   · UpdateUser 真的改一个 console 账号的 ACL
 *   · UpdateAccount / WriteStorageObject 改玩家账号与存档
 * 而且它们**替你过了 console 自带的确认摩擦**（storage 删除要输 `delete`、
 * 改 ACL 要输 `update`）—— 那道摩擦本来就是拦人为误操作的。
 *
 * 🔴 而 staging 也不是安全的去处（另一条会话线 2026-09-04 实测并记录）：
 *    staging 宿主上的 CockroachDB 是 `nkmad_dev` / `nkmfd_dev` / `nkmwd_dev`
 *    与 review **四家共享的同一个实例**。在那上面跑写流程会波及 fd dev 与 wd dev。
 *
 * ⇒ 写流程只允许打一次性本地实例。层 1 的路由扫描是纯读，不受此限。
 */
export function requireLocalForWrites(
  env: NodeJS.ProcessEnv | Record<string, string | undefined>,
): string {
  const url = (env.CONSOLE_URL ?? '').trim();
  if (!url) return 'http://127.0.0.1:7351';
  throw new Error(
    `拒绝：写流程只能打本地一次性实例，当前 CONSOLE_URL=${url}。\n` +
    `它们会真的删数据/改权限，且会绕过 console 的确认摩擦；` +
    `staging 的库还是 ad/fd/wd/review 四家共享的。此项无放行开关。`,
  );
}
