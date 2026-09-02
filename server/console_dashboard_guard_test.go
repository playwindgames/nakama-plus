package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/doublemo/nakama-plus/v3/console"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// 钉住台账 F8：index.html 里的 {{nt}} 占位符必须被替换。
//
// 前端判据是 (window.CONSOLE_CONFIG?.nt) === "true"，字面量 "{{nt}}" 会判定为
// false ⇒ Segment 追踪开启，console 用户身份与页面浏览发往 api.segment.io。
//
// 🔴 该缺陷编译通过、其他单测全绿，只有真起进程用浏览器看网络面板才暴露——
// 同一形态在这个函数里已发生三次（console/ui.go 的 prod-nt 分支、F8、F12）。
func TestDashboardIndexInjectsNt(t *testing.T) {
	orig := console.UIFS.Nt
	defer func() { console.UIFS.Nt = orig }()
	console.UIFS.Nt = true

	router := mux.NewRouter()
	if err := registerDashboardHandlers(zap.NewNop(), router); err != nil {
		t.Fatalf("registerDashboardHandlers 失败: %v", err)
	}

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	if rr.Code != 200 {
		t.Fatalf("首页应返回 200，实际 %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, "{{nt}}") {
		t.Fatal("index.html 仍含未替换的 {{nt}} —— 前端遥测开关失效，见台账 F8")
	}
	if !strings.Contains(body, `"true"`) {
		t.Fatal(`nt 未被注入为 "true"，见台账 F8 与 L3-claims-check.md`)
	}
}
