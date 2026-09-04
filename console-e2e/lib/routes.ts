export type RouteDef = {
  name: string;
  url: string;
  /** 🔴 该路由的预期页面标题 —— 层 1 的核心判据，见文件头说明 */
  title?: string;
  /**
   * 🔴 该路由【必须】发出的 console 端点（归一化后，如 GET /v2/console/account/{id}/wallet）。
   *
   * 为什么需要它：玩家详情类路由的标题【七条完全相同】
   * （都是 "{id} - Players | Nakama"，2026-09-04 实测）⇒ 标题判据对它们形同虚设，
   * 把 wallet 写成 wallett 也照样"通过"。端点是它们唯一可区分的指纹。
   */
  mustCall?: string;
  /** 需要 seed 出的实体 id 才能拼出完整 URL */
  needsFixture?: 'accountId';
  /** 🔴 不测就必须写原因 */
  skip?: string;
};

// 🔴 本清单由【实测】产出，不是从 bundle 推断的。
//
// 2026-09-04 探测输出（tests/_discover.spec.ts，已删）证实了一件事：
//   不存在的路由【不会】把标题改成 404，而是【保留上一个页面的标题】。
//     #/this-route-does-not-exist   →  title="Dashboard | Nakama"
//   同理，我 2026-09-03 猜错的三个 URL 也都停在 "Dashboard"：
//     #/accounts  #/configuration  #/apiexplorer      （台账 F25-5）
//
// ⇒ 判据必须是「标题等于该路由的预期标题」。
//   「页面上找不到 404 字样」对这个 UI 【无效】——它根本不渲染 404 字样。
//   ⇒ 猜错 URL 时的表现是「页面正常、测试通过」，是最坏的一种失败。

export const ROUTES: RouteDef[] = [
  // ── 2026-09-04 逐个实测确认，标题各不相同 ──────────────────
  { name: 'dashboard',                url: '#/',                          title: 'Dashboard | Nakama' },
  { name: 'players',                  url: '#/players',                   title: 'Players | Nakama', mustCall: 'GET /v2/console/account' },
  { name: 'storage',                  url: '#/storage',                   title: 'Storage | Nakama', mustCall: 'GET /v2/console/storage' },
  { name: 'leaderboards',             url: '#/leaderboards',              title: 'Leaderboards | Nakama' },
  { name: 'matches',                  url: '#/matches',                   title: 'Matches | Nakama', mustCall: 'GET /v2/console/match' },
  { name: 'chat',                     url: '#/chat',                      title: 'Chat | Nakama' },
  { name: 'notifications',            url: '#/notifications',             title: 'Notifications | Nakama' },
  { name: 'groups',                   url: '#/groups',                    title: 'Groups | Nakama' },
  { name: 'api-explorer',             url: '#/api-explorer',              title: 'API Explorer | Nakama' },
  { name: 'runtime',                  url: '#/runtime',                   title: 'Runtime | Nakama' },
  { name: 'settings-users',           url: '#/settings/users',            title: 'Settings - Users | Nakama' },
  { name: 'settings-general',         url: '#/settings/general',          title: 'Settings - General | Nakama' },
  { name: 'settings-configuration',   url: '#/settings/config',           title: 'Settings - Configuration | Nakama', mustCall: 'GET /v2/console/config' },
  { name: 'settings-audit-log',       url: '#/settings/audit-log',        title: 'Settings - Audit Log | Nakama' },

  // ── 实测不可达 / 有意排除 ────────────────────────────────
  { name: 'settings-data-management', url: '#/settings/data-management',
    skip: '页面可达（title="Settings - Data Management"），但含 DeleteAllData —— 不在自动化里碰（spec §14）' },

  { name: 'purchases', url: '#/purchases',
    skip: '🔴 实测不可达：访问后【重定向回 /#/】且标题为 Dashboard。2026-09-04 有账号后复测仍如此 ⇒ 确认不可达（该路由名在 bundle 里 path 为空，是父级路由）' },
  { name: 'subscriptions', url: '#/subscriptions',
    skip: '🔴 实测不可达：URL 留在 /#/subscriptions 但标题为 Dashboard（= 落到了 404 组件）。2026-09-04 有账号后复测仍如此 ⇒ 确认落到 404 组件' },

  { name: 'player-hiro',              url: '', skip: 'Hiro 是 Heroic Labs 商业模块，我方未采购 —— 服务端无此端点（F25 查证）' },
  { name: 'player-hiro-inventory',    url: '', skip: '同上' },
  { name: 'player-hiro-progressions', url: '', skip: '同上' },
  { name: 'player-satori',            url: '', skip: 'Satori 未接入' },
  { name: 'player-satori-messages',   url: '', skip: '同上' },
  { name: 'leaderboard-details',      url: '', skip: '🔴 console 无建榜端点，榜由 runtime 建；本实例不加载游戏模块 ⇒ 无榜可点' },

  // ── 玩家详情（2026-09-04 有 seed 账号后实测补入）────────────────
  // 🔴 这七条的【标题完全相同】（都是 "{id} - Players | Nakama"），靠 mustCall 区分。
  //    七条的端点均由 2026-09-04 首轮快照实测确认，不是推断。
  // ⚠️ dashboard 与 settings-general 的端点集合完全相同（只有 app-init 的
  //    extensions|setting），它们靠标题区分 —— 标题不同，故仍可分辨。
  { name: 'player-profile',       url: '#/players/{accountId}',               needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/account/{id}' },
  { name: 'player-friends',       url: '#/players/{accountId}/friends',       needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/account/{id}/friend' },
  { name: 'player-wallet',        url: '#/players/{accountId}/wallet',        needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/account/{id}/wallet' },
  { name: 'player-groups',        url: '#/players/{accountId}/groups',        needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/account/{id}/group' },
  { name: 'player-payments',      url: '#/players/{accountId}/payments',      needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/purchase' },
  { name: 'player-storage',       url: '#/players/{accountId}/storage',       needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/storage' },
  { name: 'player-notifications', url: '#/players/{accountId}/notifications', needsFixture: 'accountId',
    title: '{accountId} - Players | Nakama', mustCall: 'GET /v2/console/notification' },
];
