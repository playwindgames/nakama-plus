//go:build lockorderfixture

// 🔴 上游把 Start() 挪到了 NewLocalPeer **之后**。
// 期望：转红 —— 这正是 D14 选 B 之后唯一会让 ls.started 变成真实竞态的改动，
// 而除了这一项检查，没有任何东西会发现它（锁序检查不管可见性，
// -race 也只在两个 goroutine 真的并发碰到时才报）。

package main

func main() {
	cfg := setup()
	runtime := server.NewRuntime(cfg)
	peer := server.NewLocalPeer(cfg, runtime) // worker.New(128) 在这里就起来了
	leaderboardScheduler.Start(runtime)       // 🔴 裸写 started = true，已晚于 worker 创建
	_ = peer
}
