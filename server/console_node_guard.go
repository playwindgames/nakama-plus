// Copyright 2026 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import "github.com/doublemo/nakama-plus/v3/console"

// L4 守卫：下面三个字段是我方 fork 相对上游 console API 的增量，号位取自 900+ 的
// fork 专用号段（leader=900 / Leaderboard.node=901 / StatusList.services=902）。
//
// 若上游合并了同名字段、或有人误删、或重新生成时号位没落对，此处编译失败，
// 提示重新评估这一层是否还需要存在。
//
// 见 docs/superpowers/specs/2026-08-27-nakama-340-port-a-design.md 的 D12。
var _ = func() struct{} {
	var l console.Leaderboard
	_ = l.Node

	var s console.StatusList_Status
	_ = s.Leader

	var sl console.StatusList
	_ = sl.Services

	return struct{}{}
}()
