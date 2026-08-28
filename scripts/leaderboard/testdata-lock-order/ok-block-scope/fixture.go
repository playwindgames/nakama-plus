//go:build lockorderfixture

// 块作用域：锁在 if 块里取、块里放，块外调用不算持锁。
// 期望：退出 0，1 个调用点

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

func (h *holder) refresh(force bool) {
	if force {
		h.mu.Lock()
		h.mu.Unlock()
	}

	// 读锁同理：RLock/RUnlock 配对后不再持有。
	h.mu.RLock()
	h.mu.RUnlock()

	h.leaderboardScheduler.Update()
}
