//go:build lockorderfixture

// 🔴 三个锚点里有一个不见了（这里是 Start()）——多半是上游重构。
// 期望：转红并要求人工复查，而**不是**默默判过。
//
// 「锚点找不到」和「顺序正确」必须是不同的结果。前者说明这项检查
// 已经不知道自己在检查什么了，那时候给绿灯是最危险的。

package main

func main() {
	cfg := setup()
	runtime := server.NewRuntime(cfg)
	peer := server.NewLocalPeer(cfg, runtime)
	_ = peer
}
