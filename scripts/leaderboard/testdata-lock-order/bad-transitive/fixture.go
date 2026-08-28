//go:build lockorderfixture

// 跨函数一层：持锁 → 调用一个会直接调 Update() 的方法。
// 期望：退出 1，1 处高置信跨函数命中

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type cacheStub struct {
	sync.Mutex
	items map[string]int
}

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

// 这个方法会直接调 Update()。
func (l *cacheStub) Delete(scheduler LeaderboardScheduler, id string) {
	delete(l.items, id)
	scheduler.Update()
}

// 持着 h.mu 调它 ⇒ 等价于持锁调 Update()。
func (h *holder) sweep(c *cacheStub) {
	h.mu.Lock()
	c.Delete(h.leaderboardScheduler, "x") // 🔴 接收者 c 解析为 cacheStub
	h.mu.Unlock()
}
