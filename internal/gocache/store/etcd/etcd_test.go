package etcd

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	lib_store "github.com/eko/gocache/lib/v4/store"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 本组测试需要一个【真实可达的 etcd】。
//
// 🔴 原实现把端点硬编码成 192.168.0.127:1/2/32379 —— 那是上游作者自己的内网地址。
//
//	对任何其他人：clientv3.New 不会立刻报错（它是惰性连接），随后的 Set/Delete
//	会一直重试到 go test 的 10 分钟默认超时，整个包以 panic 收场。
//	实测：`go test ./...` 总耗时 605s，其中 600.089s 全耗在这一个包上。
//
// ⇒ 改为从环境变量取端点，未提供则 Skip。想跑的人：
//
//	ETCD_TEST_ENDPOINTS=127.0.0.1:2379 go test ./internal/gocache/store/etcd/
//
// ⚠️ 签名用 testing.TB 而非 *testing.T —— 同目录的 etcd_bench_test.go 也调它，
//
//	Benchmark 传的是 *testing.B。
//
// run go test -run='TestEtcd*' -race -cover -coverprofile=coverage.txt -covermode=atomic -v ./...
func testGetEtcdClicent(t testing.TB) *clientv3.Client {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("ETCD_TEST_ENDPOINTS"))
	if raw == "" {
		t.Skip("需要 etcd：设置 ETCD_TEST_ENDPOINTS（逗号分隔）后重跑")
	}

	endpoints := make([]string, 0, 3)
	for _, e := range strings.Split(raw, ",") {
		if e = strings.TrimSpace(e); e != "" {
			endpoints = append(endpoints, e)
		}
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// 🔴 这一段不可省，且 DialTimeout 单独【不够】。
	//    clientv3.New 是惰性的：地址不可达时它照样返回一个 client，
	//    随后的 Set/Delete 会无限重试直到 go test 的全局超时（实测挂满 3 分钟仍未返回）。
	//    ⇒ 必须在这里用一个有界 context 主动探一次，把「连不上」变成秒级的明确失败。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, endpoints[0]); err != nil {
		t.Fatalf("连不上 etcd %v: %v", endpoints, err)
	}
	return client
}
func TestEtcdSet(t *testing.T) {
	ctx := context.Background()
	etcdClient := testGetEtcdClicent(t)

	cacheKey := "my-key"
	cacheValue := "my-cache-value"
	store := NewEtcd(etcdClient, "test", lib_store.WithExpiration(6*time.Second))
	store.OnPut(func(evt *clientv3.Event) {
		t.Log("evt:", evt.Type, string(evt.Kv.Key))
	})

	// When
	err := store.Set(ctx, cacheKey, cacheValue, lib_store.WithExpiration(5*time.Second))

	// Then
	assert.Nil(t, err)
}

func TestEtcdDelete(t *testing.T) {
	ctx := context.Background()
	etcdClient := testGetEtcdClicent(t)

	cacheKey := "my-key"
	store := NewEtcd(etcdClient, "test", lib_store.WithExpiration(6*time.Second))
	store.OnPut(func(evt *clientv3.Event) {
		t.Log("evt:", evt.Type, string(evt.Kv.Key))
	})

	// When
	err := store.Delete(ctx, cacheKey)

	// Then
	assert.Nil(t, err)
}
