package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/doublemo/nakama-plus/v3/console"
	"github.com/doublemo/nakama-plus/v3/console/acl"
	"github.com/gofrs/uuid/v5"
)

// ACL 写路径的 round-trip（台账 F10 第 3 层）。
//
// 🔴 3.40 把权限模型从 4 档 role 换成按资源的细粒度 ACL，role 列被 DROP ——
//    这是本次升级【唯一直接改动】的 console 写路径，所以单列一个测试。
//
// ⚠️ UpdateUser 第一行是 ctx.Value(ctxConsoleUserAclKey{}).(acl.Permission)，
//    【无 comma-ok】—— 不塞这个 ctx 值会 panic 而不是返回错误。
//    这是纯上游写法（v3.40.0 逐字相同），不是我方引入、也不需要修，
//    但测试必须自己塞。
//
// ⚠️ AddUser 在 NewsletterSubscription 为 true 时会 POST 到
//    cloud.heroiclabs.com —— 本测试不设该字段（零值 false）。
func TestConsoleUserAclRoundTrip(t *testing.T) {
	db := NewDB(t)
	defer db.Close()

	adminID := uuid.Must(uuid.NewV4())
	ctx := context.WithValue(context.Background(), ctxConsoleUserIdKey{}, adminID)
	ctx = context.WithValue(ctx, ctxConsoleUsernameKey{}, "acl-test-admin")
	ctx = context.WithValue(ctx, ctxConsoleUserAclKey{}, acl.Admin())
	// 🔴 ctxConsoleEmailKey 不可省：AddUser 里
	//    inviterEmail := ctx.Value(ctxConsoleEmailKey{}).(string) 同样【无 comma-ok】。
	//    2026-09-03 实测：不塞它 ⇒ panic "interface conversion: interface {} is nil, not string"。
	//    ⇒ 需要塞的 ctx 值一共【四个】，不是三个。
	ctx = context.WithValue(ctx, ctxConsoleEmailKey{}, "acl-test-admin@example.invalid")

	// ⚠️ consoleSessionCache 不可省：UpdateUser 末尾无条件调它的 RemoveAll，
	//    留 nil 会 panic。cookie / httpClient 是 AddUser 用的。
	s := &ConsoleServer{
		logger:              logger,
		db:                  db,
		config:              cfg,
		cookie:              "console-regression-test",
		httpClient:          &http.Client{},
		consoleSessionCache: NewLocalSessionCache(cfg.GetConsole().TokenExpirySec, 0),
	}

	uname := "acl-target-" + uuid.Must(uuid.NewV4()).String()[:8]

	// 🔵 3.40 的 AddUserRequest 【没有 password 字段】——
	//    这正是发布说明里「不要用删了重建」的原因。
	if _, err := s.AddUser(ctx, &console.AddUserRequest{
		Username: uname,
		Email:    uname + "@example.invalid",
		Acl: map[string]*console.Permissions{
			// 🔴 键必须是 console.AclResources 的枚举名。写成 "storage" 会被
			//    acl.New() 静默跳过，权限变成空。
			"STORAGE_DATA": {Read: true, Write: false, Delete: false},
		},
	}); err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	defer db.ExecContext(context.Background(),
		`DELETE FROM console_user WHERE username = $1`, uname)

	// 改 ACL：给 write，不给 delete
	if _, err := s.UpdateUser(ctx, &console.UpdateUserRequest{
		Username: uname,
		Acl: map[string]*console.Permissions{
			"STORAGE_DATA": {Read: true, Write: true, Delete: false},
		},
	}); err != nil {
		t.Fatalf("改 ACL 失败: %v", err)
	}

	got, err := s.GetUser(ctx, &console.Username{Username: uname})
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}

	p := got.Acl["STORAGE_DATA"]
	if p == nil {
		t.Fatalf("🔴 STORAGE_DATA 的 ACL 整个丢了。现有键: %v", keysOfAcl(got.Acl))
	}
	if !p.Read || !p.Write {
		t.Errorf("read/write 期望 true/true，得到 %v/%v", p.Read, p.Write)
	}
	if p.Delete {
		t.Error("🔴 delete 期望 false —— 授予了没要求的权限，这比丢权限更危险")
	}

	// 钉住未知资源键的现状（characterization，不是主张它对）。
	//
	// 🔵 实际行为分两种：
	//    · 【全部是未知键】⇒ acl.New 得到 None ⇒ updateUser 显式返回
	//      InvalidArgument "User must have at least some permissions."
	//    · 【有效键 + 未知键混着】⇒ 调用成功，未知键被 acl.New 里的
	//      `// Unknown resource value, skip.` 静默丢弃
	//
	// 方向都是 fail-closed（少给权限，不是多给）⇒ 不是漏洞。
	// 钉它是因为：将来若改成 fail-open，第二条断言会红，有人会看见。

	// ① 全是未知键 ⇒ 必须报错
	if _, err := s.UpdateUser(ctx, &console.UpdateUserRequest{
		Username: uname,
		Acl: map[string]*console.Permissions{
			"this_is_not_a_resource": {Read: true, Write: true, Delete: true},
		},
	}); err == nil {
		t.Error("🔴 ACL 全是未知键时应报 InvalidArgument，却成功了")
	}

	// ② 有效键 + 未知键 ⇒ 成功，且未知键被静默丢弃
	if _, err := s.UpdateUser(ctx, &console.UpdateUserRequest{
		Username: uname,
		Acl: map[string]*console.Permissions{
			"USER":                   {Read: true, Write: false, Delete: false},
			"this_is_not_a_resource": {Read: true, Write: true, Delete: true},
		},
	}); err != nil {
		t.Fatalf("有效键混未知键时应成功: %v", err)
	}
	reread, err := s.GetUser(ctx, &console.Username{Username: uname})
	if err != nil {
		t.Fatal(err)
	}
	if _, bad := reread.Acl["this_is_not_a_resource"]; bad {
		t.Error("🔴 未知资源键被写进了 ACL")
	}
	if p := reread.Acl["USER"]; p == nil || !p.Read {
		t.Errorf("USER 的 read 应保留，实得 %v", keysOfAcl(reread.Acl))
	}
}

func keysOfAcl(m map[string]*console.Permissions) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
