//go:build lockorderfixture

// 接口分派：持锁时通过**接口值**调用，实现里到达 Update()。
// 期望：退出 1 —— 靠结构匹配把接口展开到实现
//
// 与真实代码同形：leaderboard_cache.go 的 Delete 是接口 LeaderboardCache 的方法，
// 而会调 Update() 的是具体类型 *LocalLeaderboardCache 上的那个。
// 没有接口展开的话，"LeaderboardCache.Delete" 查不到、只能落到低置信档 ⇒ 假阴。

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

// 方法数 ≥3，够结构匹配（<3 的小接口刻意不展开，见工具注释）。
type Cache interface {
	Get(id string) int
	Insert(id string, v int)
	Delete(scheduler LeaderboardScheduler, id string)
}

type localCache struct {
	sync.RWMutex
	items map[string]int
}

func (l *localCache) Get(id string) int          { return l.items[id] }
func (l *localCache) Insert(id string, v int)    { l.items[id] = v }
func (l *localCache) Delete(s LeaderboardScheduler, id string) {
	delete(l.items, id)
	s.Update()
}

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
	cache                Cache // ← 声明成接口类型
}

func (h *holder) sweep() {
	h.mu.Lock()
	h.cache.Delete(h.leaderboardScheduler, "x") // 🔴 接口分派，实现里会 Update()
	h.mu.Unlock()
}
