import { test, expect } from '@playwright/test';
import { readFileSync } from 'node:fs';
import { startBodyRecording, settle } from '../lib/recorder';
import { resolveTarget } from '../lib/target';

const target = resolveTarget(process.env);
const fx = JSON.parse(readFileSync(`${__dirname}/../fixtures.json`, 'utf8'));

// 🔴 UpdateAccount 的风险方向与直觉相反（spec §7 ④）。
//    12 个可选字段全是 google.protobuf.StringValue（wrapper ⇒ 可空），
//    【字段缺席 = 不改动】⇒ 「UI 少送字段」在这里反而是安全的。
//    危险的是反方向：运营只改了一个字段，而 UI 把整份内容序列化后
//    连带送出 wallet / metadata / password ⇒ 玩家钱包被清空。
//
// 🔵 与用户 2026-09-02 的手测同形：「改 storage 的 value、不碰权限 → 权限是否被重置」。
//    台账 F10 里写着那就是第 3 层的一个手工样本。
//
// ⚠️ 实测发现账号编辑是【monaco 整份 JSON 编辑】（控件是 textbox "Editor content"），
//    不是逐字段表单 —— 这比逐字段更容易「多送字段」，所以这条断言更有价值。
test('UpdateAccount：只改一个字段时不得连带送出 wallet / password', async ({ page }) => {
  const stop = startBodyRecording(page, target);

  await page.goto(`#/players/${fx.accountId}`);
  await settle(page);

  // 控件名由 2026-09-04 实测确定：Edit → Editor content → Save
  await page.getByRole('button', { name: 'Edit' }).first().click();
  await page.waitForTimeout(1500);

  const editor = page.getByRole('textbox', { name: 'Editor content' });
  await expect(editor, '点 Edit 后没出现编辑器 —— UI 结构变了').toBeVisible();

  await page.getByRole('button', { name: 'Save' }).click();
  await settle(page);

  const posts = stop().filter((r) => r.path === '/v2/console/account/{id}');
  expect(posts, '没有捕获到 UpdateAccount 请求 —— Save 点到了吗？').toHaveLength(1);

  // 🔴 否定式断言：这次操作没有触碰这些字段，请求体里就不该出现它们。
  for (const forbidden of ['wallet', 'password']) {
    expect(posts[0].fields, `🔴 未触碰却送出了 ${forbidden} —— 会覆盖玩家数据`)
      .not.toContain(forbidden);
  }

  // 🔴 toMatchSnapshot 只接受 string / Buffer（Playwright 类型定义原文），传对象不行。
  expect(JSON.stringify({ seedHash: fx.seedHash, ...posts[0] }, null, 2))
    .toMatchSnapshot('update-account.json');
});

// 🔵 与用户 2026-09-02 的手测同形：「改 storage 的 value、不碰权限 → 权限是否被重置」。
//    台账 F10 里写着那就是第 3 层的一个手工样本，这里把它固化。
//
// ⚠️ storage 详情页与账号页同构（2026-09-04 实测）：
//    列表点行 → /#/storage/{collection}/{key}/{user_id} → Edit → Editor content → Save
test('WriteStorageObject：改 value 时权限字段必须随同送出', async ({ page }) => {
  const stop = startBodyRecording(page, target);

  await page.goto('#/storage');
  await settle(page);

  const rows = await page.getByRole('row').all();
  expect(rows.length, 'storage 列表是空的 —— seed 没跑？').toBeGreaterThan(1);
  await rows[1].click();                       // 第 1 行是表头
  await page.waitForTimeout(2000);
  expect(page.url(), '点行后没进详情页').toMatch(/#\/storage\/[^/]+\/[^/]+\/[^/]+/);

  await page.getByRole('button', { name: 'Edit' }).first().click();
  await page.waitForTimeout(1500);
  await expect(page.getByRole('textbox', { name: 'Editor content' }),
    '点 Edit 后没出现编辑器 —— UI 结构变了').toBeVisible();

  await page.getByRole('button', { name: 'Save' }).click();
  await settle(page);

  const writes = stop().filter((r) => r.path.startsWith('/v2/console/storage'));
  expect(writes, '没有捕获到 storage 写请求 —— Save 点到了吗？').not.toHaveLength(0);

  // 🔴 权限字段必须随同送出。若 UI 只送 value，服务端会把权限当成【未提供】——
  //    而 WriteStorageObjectRequest 的 permission_read/write 是 Int32Value（可空），
  //    缺席的语义由服务端决定，不能想当然。这条钉住「UI 确实送了它们」。
  for (const need of ['permission_read', 'permission_write']) {
    expect(writes[0].fields, `🔴 写 storage 时没送 ${need} —— 权限可能被重置`)
      .toContain(need);
  }

  expect(JSON.stringify({ seedHash: fx.seedHash, ...writes[0] }, null, 2))
    .toMatchSnapshot('write-storage.json');
});

// 🔴 全 proto 里【唯一】带 additional_bindings 的 rpc（F10-5）：
//      4 段  /storage/{collection}/{key}/{user_id}            无版本，不走 OCC
//      5 段  /storage/{collection}/{key}/{user_id}/{version}  走 OCC
//    F10-5 靠静态读 dist 得出「新 UI 三个调用点全走 OCC」，但下次 UI 重写后静态分析要重做；
//    这条是自动的，每次跑都在验。
//
// 🔴 而且它验的不只是「路径拼对了」，而是「UI 记得把【读回来的 version】带进删除请求」——
//    version 不在前端路由 URL 里（那是 4 段），只能来自详情数据。
//    这类「把 A 接口的返回值正确传给 B 接口」的链路，静态分析看不出来。
//
// ⚠️ 已知脆弱点（spec §8 的 2026-09-04 订正）：触发用的三点图标按钮【没有可访问名称】
//    （storage 页 31 个 button 里 30 个 aria-label/title 都是 null）⇒ 只能按结构定位。
//    这里的折中是「先按 role=row 圈定范围，再取行内最后一个 button」——
//    比 CSS 类链稳（类名改了不受影响），但行内按钮顺序变了就会失效。
//    🔵 popover 里的 "Delete" 有文字，那一步是稳的。
test('DeleteStorageObject：UI 必须走带 version 的 OCC 变体', async ({ page }) => {
  const stop = startBodyRecording(page, target);

  await page.goto('#/storage');
  await settle(page);

  const row = page.getByRole('row').nth(1);          // 第 0 行是表头
  const trigger = row.getByRole('button').last();     // 三点菜单：无名，按位置
  await expect(trigger, '行内找不到操作按钮 —— 表格结构变了').toBeVisible();
  await trigger.click();
  await page.waitForTimeout(800);

  const del = page.getByText('Delete', { exact: true }).first();
  await expect(del, 'popover 里没有 Delete —— 菜单结构或权限变了').toBeVisible();
  await del.click();
  await page.waitForTimeout(800);

  // 🔴 确认对话框【要求手输 "delete"】—— 2026-09-04 实测：
  //      "Confirm Deletion / Are you sure you want to delete 'big-r0w0'? /
  //       Please type 'delete' to confirm. / Cancel | Delete"
  //    不输字，Delete 按钮点了也不生效。
  // ⚠️ 初版写的是 `if (await confirm.isVisible()) click()` —— 【选择性执行】，
  //    没点到就静默跳过。之所以没蒙混过去，是因为下面的断言要求【恰好一个】删除请求。
  //    ⇒ 这里改成必然断言：确认框找不到就该红，不能"有就点、没有就算了"。
  const dialog = page.getByText(/Please type 'delete' to confirm/i);
  await expect(dialog, '没有出现删除确认框 —— 流程或文案变了').toBeVisible();

  await page.getByRole('textbox').last().fill('delete');
  await page.getByRole('button', { name: /^delete$/i }).last().click();
  await settle(page);

  const dels = stop().filter((r) => r.method === 'DELETE' && r.path.includes('/storage/'));
  expect(dels, '没有捕获到删除请求 —— 确认对话框点到了吗？').toHaveLength(1);

  // /v2/console/storage/{collection}/{key}/{user_id}[/{version}]
  //   4 段 ⇒ v2,console,storage,c,k,u        = 6 段
  //   5 段 ⇒ v2,console,storage,c,k,u,version = 7 段
  const segs = dels[0].path.split('/').filter(Boolean);
  expect(segs.length,
    `🔴 走了【无版本】的删除变体，绕过 OCC。实际路径：${dels[0].path}`).toBe(7);

  expect(JSON.stringify({ seedHash: fx.seedHash, ...dels[0] }, null, 2))
    .toMatchSnapshot('delete-storage.json');
});
