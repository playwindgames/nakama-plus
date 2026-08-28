package server

// P28 探针 —— 验证「3.40 的调度器架构重写是否消化了我方自研补丁 02 / 04」，
// 并确认 #2538（补丁 03）在裸 v3.40.0 上确实缺失。
//
// 预期（写在跑之前，避免事后解释）：
//   S1  绿  —— Update() 不再 Stop 定时器 ⇒ 补丁 02 的竞态没有存身之处
//   S2  红  —— v3.40.0:leaderboard_scheduler.go:277 是 `if expiry > 0`，缺守卫
//   S3  绿  —— Update() 只发 channel 信号 ⇒ 补丁 04 的并发覆盖不可能发生
//
// S1 或 S3 红 ⇒ P28 的「架构消除」判断有误，02 / 04 需在 3.40 基底上重新移植。
// S2 绿 ⇒ 测试没测到点上，设计有问题。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-plus/v3/internal/cronexpr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// 只覆盖 scheduleLoop 会碰到的那一个方法，其余留空接口（不调用即不 panic）。
type p28NoopRankCache struct{ LeaderboardRankCache }

func (p28NoopRankCache) TrimExpired(int64) bool { return true }

// 手工装配一个可跑 scheduleLoop 的调度器，绕开 Start() 对 *Runtime 的依赖。
func p28NewScheduler(boards []*Leaderboard) (*LocalLeaderboardScheduler, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ls := &LocalLeaderboardScheduler{
		logger:      zap.NewNop(),
		cache:       &LocalLeaderboardCache{allList: boards},
		rankCache:   p28NoopRankCache{},
		queue:       make(chan *LeaderboardSchedulerCallback, 128),
		active:      atomic.NewUint32(1),
		updateCh:    make(chan struct{}, 1),
		ctx:         ctx,
		ctxCancelFn: cancel,
		started:     true, // Update() 有 `if !ls.started { return }` 守卫

		// 🔴 必须设：processEndActive 首行是 `if ls.fnTournamentEnd == nil { return }`，
		// 不设则永远不入队，S1 会以「回调丢失」的形式假红。
		fnTournamentEnd: func(context.Context, *api.Tournament, int64, int64) error { return nil },
	}
	return ls, cancel
}

// ---------------------------------------------------------------------------
// S1 · 对应补丁 02-cancel-if-pending（形态 1）
//
// 旧架构：Update() 直接 Stop 掉定时器再重设。在边界秒上重算出的目标会跨到下一
// 周期，与已排定的时刻不等，于是把那个「正要投递」的定时器掐死 —— 回调永久
// 丢失且不产生任何日志（issue #2429）。
//
// 本用例在定时器待触发的整个窗口内高频调用 Update()，若架构仍会因此掐死定时器，
// 回调就收不到。
// ---------------------------------------------------------------------------
func TestP28_S1_HighFrequencyUpdateDoesNotDropCallback(t *testing.T) {
	// 让 endActive 落在 ~2 秒后：一个 duration=3600 的 tournament，start 在 3598 秒前。
	now := time.Now().UTC()
	start := now.Add(-3598 * time.Second).Unix()

	ls, cancel := p28NewScheduler([]*Leaderboard{
		{Id: "s1-tournament", Duration: 3600, StartTime: start},
	})
	defer cancel()

	go ls.scheduleLoop()
	ls.Update() // 触发首次计算

	// 在等待窗口内持续 Update()，模拟生产里高频的榜变更。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ls.Update()
			}
		}
	}()

	var got *LeaderboardSchedulerCallback
	select {
	case cb := <-ls.queue:
		got = cb
	case <-time.After(6 * time.Second):
	}
	close(stop)
	wg.Wait()

	assert.NotNil(t, got, "S1: 高频 Update() 期间回调丢失 ⇒ P28 判断有误，补丁 02 仍然需要")
	if got != nil {
		assert.Equal(t, "s1-tournament", got.id)
	}
}

// ---------------------------------------------------------------------------
// S2 · 对应补丁 03-expiry-future-guard == 上游 #2538（形态 2）
//
// 两个用例逐字取自 patches/upstream-2538-as-fetched.patch，未作改动。
// 在裸 v3.40.0 上**预期为红** —— 该版本第 277 行仍是 `if expiry > 0`。
// ---------------------------------------------------------------------------
func TestP28_S2_EndedTournamentHidesLiveExpiry(t *testing.T) {
	const tournamentEnd int64 = 1_700_000_000

	hourly, err := cronexpr.Parse("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}

	ls := &LocalLeaderboardScheduler{
		cache: &LocalLeaderboardCache{
			allList: []*Leaderboard{
				{Id: "ending-tournament", Duration: 3600, StartTime: tournamentEnd - 7200, EndTime: tournamentEnd},
				{Id: "hourly-leaderboard", ResetScheduleStr: "0 * * * *", ResetSchedule: hourly},
			},
		},
	}

	liveExpiry := hourly.Next(time.Unix(tournamentEnd, 0).UTC()).UTC().Unix()

	_, expiryTs, _, expiryIds := ls.computeNext(time.Unix(tournamentEnd-1, 0).UTC())
	assert.Equal(t, tournamentEnd, expiryTs)
	assert.Equal(t, []string{"ending-tournament"}, expiryIds)

	_, expiryTs, _, expiryIds = ls.computeNext(time.Unix(tournamentEnd, 0).UTC())
	assert.Equal(t, liveExpiry, expiryTs)
	assert.Equal(t, []string{"hourly-leaderboard"}, expiryIds)
}

func TestP28_S2_EndedTournamentHidesSuccessorExpiry(t *testing.T) {
	const day1End int64 = 1_700_042_400
	const day2End int64 = day1End + 86400

	ls := &LocalLeaderboardScheduler{
		cache: &LocalLeaderboardCache{
			allList: []*Leaderboard{
				{Id: "day-1", Duration: 86400, StartTime: day1End - 86400, EndTime: day1End},
				{Id: "day-2", Duration: 86400, StartTime: day1End, EndTime: day2End},
			},
		},
	}

	endActiveTs, expiryTs, endActiveIds, expiryIds := ls.computeNext(time.Unix(day1End, 0).UTC())

	assert.Equal(t, day2End, endActiveTs)
	assert.Equal(t, []string{"day-2"}, endActiveIds)
	assert.Equal(t, day2End, expiryTs)
	assert.Equal(t, []string{"day-2"}, expiryIds)
}

// ---------------------------------------------------------------------------
// S3 · 对应补丁 04-update-mu-serialize
//
// 旧架构：Update() 自己扫描全部榜再提交，锁只包住末尾的替换块 ⇒
// 「U1 扫描 → U2 扫描并提交 → U1 用旧快照提交」会让陈旧的目标时刻与 ID 集合
// 覆盖掉新的。
//
// 本用例并发调用 Update()，MUST 在 `-race` 下运行才有意义。
// ---------------------------------------------------------------------------
func TestP28_S3_ConcurrentUpdateIsRaceFree(t *testing.T) {
	now := time.Now().UTC()
	boards := make([]*Leaderboard, 0, 32)
	for i := 0; i < 32; i++ {
		boards = append(boards, &Leaderboard{
			Id:        "s3-board-" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Duration:  3600,
			StartTime: now.Add(time.Duration(-i) * time.Minute).Unix(),
		})
	}

	ls, cancel := p28NewScheduler(boards)
	defer cancel()

	go ls.scheduleLoop()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ls.Update()
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("S3: 并发 Update() 卡死 ⇒ P28 判断有误，补丁 04 仍然需要")
	}

	// 循环仍然活着：再发一次信号不应阻塞。
	ls.Update()
}
