package server

import (
	"context"
	"testing"

	"github.com/doublemo/nakama-plus/v3/console"
	"github.com/gofrs/uuid/v5"
)

// 账号备注的 write→read-back 不变量（台账 F10 第 3 层）。
//
// 🔵 选备注做 console_account 这一组的第一个，是因为它是该组里
//    【唯一不涉及不可逆动作】的写路径（封号/解绑/删钱包都会改真实玩家状态）。
//
// 🔴 AddAccountNote 从 ctx 取 console 用户身份（comma-ok，取不到返回
//    FailedPrecondition，不会 panic）。而 UpdateUser 那两个不是 comma-ok，
//    见 console_user_acl_test.go 的说明。
func TestConsoleAccountNoteRoundTrip(t *testing.T) {
	db := NewDB(t)
	defer db.Close()

	adminID := uuid.Must(uuid.NewV4())
	adminName := "note-admin-" + adminID.String()[:8]
	ctx := context.WithValue(context.Background(), ctxConsoleUserIdKey{}, adminID)
	ctx = context.WithValue(ctx, ctxConsoleUsernameKey{}, adminName)

	s := &ConsoleServer{logger: logger, db: db}

	// 🔴 这一行不可省。ListAccountNotes 的 SQL 是
	//      LEFT JOIN console_user AS cuc ON un.create_id = cuc.id
	//    —— create_id 只是个 UUID 列、【没有外键约束】，插一个不存在的 id 不会报错，
	//    但 JOIN 不到 ⇒ create_username 读回来是空串，而不是塞进 ctx 的那个名字。
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO console_user (id, username, email, password) VALUES ($1, $2, $3, $4)`,
		adminID, adminName, adminName+"@example.invalid", []byte("x")); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(),
		`DELETE FROM console_user WHERE id = $1`, adminID)

	// 备注挂在 users_notes.user_id 上，该列有外键指向 users(id)
	playerID := uuid.Must(uuid.NewV4())
	if _, err := db.ExecContext(ctx,
		`INSERT INTO users (id, username) VALUES ($1, $2)`,
		playerID, "player-"+playerID.String()[:8]); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), `DELETE FROM users WHERE id = $1`, playerID)

	const body = "运营备注：这一行必须原样读回"

	added, err := s.AddAccountNote(ctx, &console.AddAccountNoteRequest{
		AccountId: playerID.String(),
		Note:      body,
	})
	if err != nil {
		t.Fatalf("写入备注失败: %v", err)
	}
	if added.Id == "" {
		t.Fatal("返回的备注没有 id")
	}

	listed, err := s.ListAccountNotes(ctx, &console.ListAccountNotesRequest{
		AccountId: playerID.String(),
	})
	if err != nil {
		t.Fatalf("读回备注失败: %v", err)
	}

	var found *console.AccountNote
	for _, n := range listed.Notes {
		if n.Id == added.Id {
			found = n
			break
		}
	}
	if found == nil {
		t.Fatalf("🔴 写入成功但读不回来 —— 正是 F10 担心的静默失败。现有 %d 条", len(listed.Notes))
	}
	if found.Note != body {
		t.Errorf("内容不一致\n  写入 %q\n  读回 %q", body, found.Note)
	}
	if found.CreateUsername != adminName {
		t.Errorf("创建者用户名期望 %q，得到 %q（若为空串，多半是 console_user 那行没插）",
			adminName, found.CreateUsername)
	}

	// 🔴 删除断言必须有个「幸存者」，否则是空转的。
	//    2026-09-03 实测：若只写一条、删掉后遍历空列表，
	//    把断言改成它的【反面】测试竟然照样绿 —— 循环体根本不执行。
	//    ⇒ 那样的话 ListAccountNotes 整个坏掉返回空，测试也发现不了。
	survivor, err := s.AddAccountNote(ctx, &console.AddAccountNoteRequest{
		AccountId: playerID.String(),
		Note:      "这一条不删，用来证明列表本身还在工作",
	})
	if err != nil {
		t.Fatalf("写入第二条备注失败: %v", err)
	}

	if _, err := s.DeleteAccountNote(ctx, &console.DeleteAccountNoteRequest{
		AccountId: playerID.String(),
		NoteId:    added.Id,
	}); err != nil {
		t.Fatalf("删除备注失败: %v", err)
	}

	after, err := s.ListAccountNotes(ctx, &console.ListAccountNotesRequest{
		AccountId: playerID.String(),
	})
	if err != nil {
		t.Fatal(err)
	}

	var gone, kept bool
	for _, n := range after.Notes {
		if n.Id == added.Id {
			gone = true
		}
		if n.Id == survivor.Id {
			kept = true
		}
	}
	if gone {
		t.Error("🔴 删除后仍能读到")
	}
	if !kept {
		t.Errorf("🔴 未被删除的那条也不见了 —— 列表返回 %d 条，删除可能误伤",
			len(after.Notes))
	}
}
