//go:build lockorderfixture

// 🔴 词法检查会假阳的那一类：持锁函数体内把闭包交给 worker 池。
// 期望：退出 0（闭包不继承锁），但必须在盲区清单里点名该闭包

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

type workerPool struct{}

func (w *workerPool) Submit(fn func()) { go fn() }

// 与 peer_binary_log.go:125-127 同形。闭包词法上在 h.mu 的持锁区内，
// 但它在 worker 的 goroutine 上跑 ⇒ 不继承锁。
// ⚠️ 检查器**假定**它异步 —— 这条假设必须被打印出来供人工确认，
// 否则同步回调形态（WithLock(func(){…})）就成了假阴。
func (h *holder) replicate(wk *workerPool) {
	h.mu.Lock()
	wk.Submit(func() {
		h.leaderboardScheduler.Update()
	})
	h.mu.Unlock()
}
