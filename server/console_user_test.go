package server

import (
	"context"
	"testing"

	"github.com/doublemo/nakama-plus/v3/console"
	"github.com/gofrs/uuid/v5"
	"golang.org/x/crypto/bcrypt"
)

// console 的首个写路径回归测试（台账 F10 第 3 层的开端）。
//
// 断言的是【功能正确性】——不是随机性质量，后者测不了也不该测。
// 与台账 F16 相关：该函数的 rand.Read 必须来自 crypto/rand。
//
// ResetUserPassword 只用到 ConsoleServer 的 config / db / logger 三个字段，
// 可直接构造结构体字面量，不必走 26 参数的 StartConsoleServer。
func TestResetUserPasswordScramblesOldCredential(t *testing.T) {
	db := NewDB(t)
	defer db.Close()
	ctx := context.Background()

	uid := uuid.Must(uuid.NewV4())
	username := "reset-" + uid.String()[:8]
	email := username + "@example.invalid"
	oldPlain := []byte("old-password-under-test")
	oldHash, err := bcrypt.GenerateFromPassword(oldPlain, bcryptHashCost)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO console_user (id, username, email, password) VALUES ($1, $2, $3, $4)`,
		uid, username, email, oldHash); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, `DELETE FROM console_user WHERE id = $1`, uid)

	s := &ConsoleServer{logger: logger, db: db, config: cfg}

	resp, err := s.ResetUserPassword(ctx, &console.Username{Username: username})
	if err != nil {
		t.Fatalf("ResetUserPassword 失败: %v", err)
	}
	if resp.GetCode() == "" {
		t.Fatal("未返回一次性 code（JWT），重置流程无法继续")
	}

	var newHash []byte
	if err := db.QueryRowContext(ctx,
		`SELECT password FROM console_user WHERE id = $1`, uid).Scan(&newHash); err != nil {
		t.Fatal(err)
	}

	if string(newHash) == string(oldHash) {
		t.Fatal("password 未被改写 —— 重置没有生效")
	}
	if _, err := bcrypt.Cost(newHash); err != nil {
		t.Fatalf("新 password 不是合法的 bcrypt 哈希: %v", err)
	}
	// 核心语义：旧密码必须失效
	if err := bcrypt.CompareHashAndPassword(newHash, oldPlain); err == nil {
		t.Fatal("旧密码仍然可用 —— 重置未打乱原凭据")
	}
}

func TestResetUserPasswordUnknownUser(t *testing.T) {
	db := NewDB(t)
	defer db.Close()

	s := &ConsoleServer{logger: logger, db: db, config: cfg}
	if _, err := s.ResetUserPassword(context.Background(),
		&console.Username{Username: "no-such-user-" + uuid.Must(uuid.NewV4()).String()}); err == nil {
		t.Fatal("用户不存在时应返回错误")
	}
}
