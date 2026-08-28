//go:build lockorderfixture

// 🔴 词法检查会判反的那一类：defer Unlock。
// 期望：退出 1，1 处违规（Unlock 在文本上位于 Update 之前）

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type cacheStub struct {
	sync.Mutex
	items map[string]int
}

// 词法判据「位于 Lock() 与 Unlock() 之间」在这里会说「不在锁内」——
// 而实际上从 defer 那行到函数结束全程持锁。
func (l *cacheStub) Delete(scheduler LeaderboardScheduler, id string) {
	l.Lock()
	defer l.Unlock()

	delete(l.items, id)
	scheduler.Update() // 🔴 仍然持 l.Lock
}

// 读锁的同一形态。
type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

func (h *holder) refresh() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	h.leaderboardScheduler.Update() // 🔴 仍然持 h.mu 读锁
}
