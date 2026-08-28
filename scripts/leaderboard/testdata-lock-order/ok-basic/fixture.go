//go:build lockorderfixture

// 正常形态：先解锁再调，或压根不持锁。
// 期望：退出 0，2 个调用点

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

// 与 leaderboard_cache.go:848-850 同形：显式 Unlock 之后才调。
func (l *cacheStub) Delete(scheduler LeaderboardScheduler, id string) {
	l.Lock()
	delete(l.items, id)
	l.Unlock()

	scheduler.Update()
}

// 完全不涉及锁。
func (h *holder) create() {
	h.leaderboardScheduler.Update()
}
