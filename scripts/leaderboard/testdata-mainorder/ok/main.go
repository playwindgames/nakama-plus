//go:build lockorderfixture

// 正确的启动顺序：Start() 夹在 NewRuntime 与 NewLocalPeer 之间。
// 期望：main.go 这一项 ✅

package main

func main() {
	cfg := setup()
	runtime := server.NewRuntime(cfg)
	leaderboardScheduler.Start(runtime)
	peer := server.NewLocalPeer(cfg, runtime)
	_ = peer
}
