// Copyright 2019 The Nakama Authors
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

package console

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
)

//go:embed ui/dist/*
var embedFS embed.FS
var UIFS = &uiFS{}

type uiFS struct {
	Nt bool
}

func (fs *uiFS) Open(name string) (fs.File, error) {
	// 🔴 取上游 3.40 的实现，不取 doublemo 的 prod / prod-nt 分支。
	//
	// doublemo 的 console/ui/dist 分成 prod/ 与 prod-nt/ 两个子目录，按 Nt 选一个；
	// 上游 3.40 的 dist 是扁平的（直接 index.html）。我方**不接手 doublemo 那棵
	// v3.29 血统的 UI 树**（总览 §2），用的是上游的扁平 dist ——
	// 照搬 doublemo 的分支会去找不存在的 ui/dist/prod-nt/，
	// 表现是启动即 fatal「Console dashboard registration failed」。
	//
	// ⚠️ 这个组合是 L3 干净套用带进来的：编译通过、单元测试全绿、只有真起进程才暴露。
	// Nt 字段保留（上游也保留了它，只是不使用），main.go 仍会设它。
	return embedFS.Open(path.Join("ui", "dist", name))
}

var UI = http.FileServer(http.FS(UIFS))
