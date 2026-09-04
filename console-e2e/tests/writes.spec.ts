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
