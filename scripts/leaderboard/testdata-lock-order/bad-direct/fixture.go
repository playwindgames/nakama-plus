//go:build lockorderfixture

// 最朴素的违规：Lock 与 Unlock 之间直接调 Update()。
// 期望：退出 1，1 处违规

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type cacheStub struct {
	sync.Mutex
	items map[string]int
}

func (l *cacheStub) Delete(scheduler LeaderboardScheduler, id string) {
	l.Lock()
	delete(l.items, id)
	scheduler.Update() // 🔴 持 l.Lock
	l.Unlock()
}
