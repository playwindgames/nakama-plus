package server

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-plus/v3/console"
	"github.com/gofrs/uuid/v5"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// console 写路径的 round-trip 不变量（台账 F10 第 3 层）。
//
// 🔵 这是【单版本自洽】的断言：判据来自「写进去的值」本身，不来自任何旧版本，
//    因此不会随升级过期，可以永久留下来。
//
// 抓的是人工冒烟抓不到的那类失败：界面报成功、实则没落库或落错字段。
// console 是唯一由人直接操作生产数据的界面，而 storage 存的是玩家存档。
func TestConsoleWriteStorageObjectRoundTrip(t *testing.T) {
	db := NewDB(t)
	defer db.Close()
	ctx := context.Background()

	s := &ConsoleServer{
		logger:       logger,
		db:           db,
		config:       cfg,
		metrics:      metrics,
		storageIndex: storageIdx,
	}

	uid := uuid.Must(uuid.NewV4())
	collection := "console_rt"
	key := "k-" + uid.String()[:8]
	value := `{"hp":42,"name":"round-trip"}`

	// 🔴 storage 有外键 storage_user_id_fkey 指向 users(id) ——
	//    不先建账号，WriteStorageObject 会以 Internal 失败（SQLSTATE 23503）。
	//    users 表只有 id 与 username 是 NOT NULL 且无默认值，其余可省。
	if _, err := db.ExecContext(ctx,
		`INSERT INTO users (id, username) VALUES ($1, $2)`,
		uid, "storage-rt-"+uid.String()[:8]); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, uid)

	ack, err := s.WriteStorageObject(ctx, &console.WriteStorageObjectRequest{
		Collection:      collection,
		Key:             key,
		UserId:          uid.String(),
		Value:           value,
		PermissionRead:  &wrapperspb.Int32Value{Value: 2},
		PermissionWrite: &wrapperspb.Int32Value{Value: 1},
	})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	defer db.ExecContext(ctx,
		`DELETE FROM storage WHERE collection = $1 AND key = $2`, collection, key)

	got, err := s.GetStorage(ctx, &api.ReadStorageObjectId{
		Collection: collection,
		Key:        key,
		UserId:     uid.String(),
	})
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}

	// 🔴 value 【不是字节保真的】——storage.value 是 JSONB 列，读回时被数据库归一化：
	//      写入 {"hp":42,"name":"round-trip"}
	//      读回 {"hp": 42, "name": "round-trip"}   ← 加了空格
	//    2026-09-03 实测得到（CockroachDB v23.2.31）。
	//    ⇒ 正确的不变量是【语义相等】，不是字节相等。
	//    ⚠️ 任何依赖 console 写入后字节形态不变的调用方，这个假设是错的。
	var wantJSON, gotJSON any
	if err := json.Unmarshal([]byte(value), &wantJSON); err != nil {
		t.Fatalf("测试自身的 value 不是合法 JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(got.Value), &gotJSON); err != nil {
		t.Fatalf("读回的 value 不是合法 JSON: %v（原文 %s）", err, got.Value)
	}
	if !reflect.DeepEqual(wantJSON, gotJSON) {
		t.Errorf("value 语义不一致\n  写入 %s\n  读回 %s", value, got.Value)
	}
	if got.Version != ack.Version {
		t.Errorf("version 不一致：ack %q, 读回 %q", ack.Version, got.Version)
	}
	if got.PermissionRead != 2 {
		t.Errorf("permission_read 期望 2，得到 %d", got.PermissionRead)
	}
	if got.PermissionWrite != 1 {
		t.Errorf("permission_write 期望 1，得到 %d", got.PermissionWrite)
	}
}

// 删除的 OCC 必须真的生效。
//
// 🔵 背景（台账 F10-5）：console 的 delete 有两个 HTTP 绑定 ——
//    4 段（无版本）与 5 段（带 {version}，走 OCC）。实测新 Vue UI 的
//    3 个组件调用点【全部】走带版本的那个，所以这是运营真实会走的路径。
//    服务端把 in.Version 透传给 StorageDeleteObjects 并有 OCC 分支。
//    本测试钉住「透传确实生效」，防止将来某次重构把 Version 丢掉而无人察觉。
func TestConsoleDeleteStorageObjectEnforcesOCC(t *testing.T) {
	db := NewDB(t)
	defer db.Close()
	ctx := context.Background()

	s := &ConsoleServer{
		logger:       logger,
		db:           db,
		config:       cfg,
		metrics:      metrics,
		storageIndex: storageIdx,
	}

	uid := uuid.Must(uuid.NewV4())
	collection := "console_occ"
	key := "k-" + uid.String()[:8]

	// storage 有外键指向 users(id)，见 round-trip 测试里的说明
	if _, err := db.ExecContext(ctx,
		`INSERT INTO users (id, username) VALUES ($1, $2)`,
		uid, "storage-occ-"+uid.String()[:8]); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, uid)

	ack, err := s.WriteStorageObject(ctx, &console.WriteStorageObjectRequest{
		Collection:      collection,
		Key:             key,
		UserId:          uid.String(),
		Value:           `{"v":1}`,
		PermissionRead:  &wrapperspb.Int32Value{Value: 2},
		PermissionWrite: &wrapperspb.Int32Value{Value: 1},
	})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// ① 用过期 version 删除：必须失败
	_, err = s.DeleteStorageObject(ctx, &console.DeleteStorageObjectRequest{
		Collection: collection,
		Key:        key,
		UserId:     uid.String(),
		Version:    "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err == nil {
		t.Fatal("🔴 过期 version 竟然删成功了 —— OCC 没有生效")
	}

	// ② 对象必须还在
	if _, err := s.GetStorage(ctx, &api.ReadStorageObjectId{
		Collection: collection, Key: key, UserId: uid.String(),
	}); err != nil {
		t.Fatalf("🔴 OCC 失败后对象却不见了: %v", err)
	}

	// ③ 用正确 version 删除：必须成功
	if _, err := s.DeleteStorageObject(ctx, &console.DeleteStorageObjectRequest{
		Collection: collection,
		Key:        key,
		UserId:     uid.String(),
		Version:    ack.Version,
	}); err != nil {
		t.Fatalf("正确 version 删除失败: %v", err)
	}

	// ④ 现在必须读不到了
	if _, err := s.GetStorage(ctx, &api.ReadStorageObjectId{
		Collection: collection, Key: key, UserId: uid.String(),
	}); err == nil {
		t.Fatal("🔴 删除后仍然读得到")
	}
}
