// check-lock-order —— 把 design.md D3 的「人工枚举 12 个 Update() 调用点」变成可执行门禁。
//
// # 不变量
//
//	没有任何代码路径在**持锁**时调用 LeaderboardScheduler.Update()
//
// 为什么这条成立就够：Update() 自己的锁序是 updateMu → (ls 内嵌 Mutex | cache 的
// RWMutex)。只要 updateMu 永远是最外层，ABBA 反转就不可能构成。反过来，任何
// 「持着 Update() 内部会取的锁 → 调 Update()」的路径都是死锁候选。
// 本工具因此对**所有**持锁状态报警，而不去区分「这把锁是不是 cache 的」——
// 在 server 包里正确答案就是零，放宽只会给假阴留门。
//
// # 为什么是 go/ast 而不是正则
//
// 词法判据「Update() 位于 Lock() 与 Unlock() 之间」在这份代码上会同时错两头：
//
//   - defer l.Unlock()：Unlock() 在**文本上**位于 Update() 之前 ⇒ 判成没持锁。
//     🔴 假阴，而且方向最坏 —— 真正的持锁区反而被漏掉。
//   - wk.Submit(func(){ …Update() })：闭包词法上在持锁函数体内，实际在别的
//     goroutine 上跑 ⇒ 判成持锁。🔴 假阳，peer_binary_log.go 三处都是这个形状。
//
// 两者都是语法结构问题，AST 里是现成的节点。
//
// # 它看不见什么（务必连同结果一起读）
//
// 见 reportBlindSpots()，每次运行都会打印。
//
// # 附带的第二项检查：main.go 的启动顺序
//
// design.md D14 决定不把 ls.started 改成 atomic.Bool，理由是当前没有 race ——
// 而「当前没有」依赖 main.go 里三个调用的先后：
//
//	NewRuntime(...)              ← InitModule 建榜会调 Update()
//	leaderboardScheduler.Start() ← 这里裸写 ls.started = true
//	NewLocalPeer(...)            ← 内含 worker.New(128)，peer 复制会调 Update()
//
// Start() 必须夹在中间。上游哪天把它挪到 NewLocalPeer 之后，就是真实的数据竞争，
// 而**没有任何别的东西会发现**：锁序检查管的是锁序不是可见性；-race 只在两个
// goroutine 真的并发碰到时才报，而这个写只发生在启动一瞬间。
// 原本的缓解是「同步上游时人工复查」（tasks 5.6）—— 单点、且是人。这里把它机械化。
//
// 用法：
//
//	go run check-lock-order.go [-v] [-expect N] [-main <main.go>] <dir|file>...
//	go run check-lock-order.go -main /path/to/nakama-plus/main.go /path/to/nakama-plus/server
//
// 退出码：0 = 通过；1 = 发现违规 / 自检失败；2 = 用法或解析错误。
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// 被追踪的调度器类型。Update() 的接收者要能追溯到它们之一，才算「调度器的 Update」。
var schedulerTypeNames = map[string]bool{
	"LeaderboardScheduler":      true, // 接口
	"LocalLeaderboardScheduler": true, // 实现
}

type lockRec struct {
	expr     string // 接收者表达式，如 "l" / "ls" / "s.mu"
	kind     string // Lock | RLock
	pos      token.Position
	deferred bool // 由 defer 释放 ⇒ 持有到**函数**结束，而不是块结束
}

type callSite struct {
	pos       token.Position
	recv      string    // Update() 的接收者表达式
	held      []lockRec // 该点持有的锁
	inClosure bool      // 位于未立即调用的函数字面量内
	closure   token.Position
	viaDefer  bool // defer x.Update()
	viaGo     bool // go x.Update()
}

type checker struct {
	fset *token.FileSet
	root string

	// 名字 → 是否绑定到调度器类型。由字段声明 / 形参 / 接收者 / 显式 var 收集。
	schedulerIdents map[string]bool
	// 名字 → 它被声明过的类型名集合。用来给「x.M()」猜接收者类型。
	// 同名不同类型会同时进集合 ⇒ 命中算低置信，不进门禁。
	identTypes map[string]map[string]bool

	sites              []callSite // 已确认是调度器的 Update() 调用点
	unclassified       []callSite // 名字对不上的 .Update() —— 不判失败，但必须打印
	asyncClosure       []closureNote
	deferredUnlockSeen int

	// 有限跨函数：函数键（「类型.方法」或「函数名」）→ 它内部是否直接调 Update()
	fnDirectUpdate map[string]bool
	// 同上，但只按方法裸名索引 —— 接收者类型解析不出来时的兜底（低置信）
	fnDirectUpdateByName map[string]bool

	// 全量调用边：函数键 → 它调用的候选被调方键。用于跨函数**多层**可达性。
	// 与 underLockCalls 分开：这里记的是「谁调了谁」，不管当时持不持锁。
	callEdges map[string]map[string]bool
	// 接口名 → 方法名集合；具体类型名 → 方法名集合。用来做结构化的「谁实现了谁」。
	ifaceMethods map[string]map[string]bool
	typeMethods  map[string]map[string]bool
	// 结构体名 → map 字段名 → **值**类型名。用来解析 `v := x.f[k]`。
	structFieldMapVal map[string]map[string]string
	// 结构体名 → 字段名 → 字段类型名。用来把 `x.f.M()` 里的 `f` 解析准。
	// 🔴 没有它就只能按标识符裸名查全局表，而 `cache` 这种名字在 164 个文件里
	//    被声明成过好几种类型 ⇒ 4 个假阳（实测：rankCache.cache 其实是 *skiplist.SkipList）。
	structFields map[string]map[string]string
	// 接口名 → 实现它的具体类型名（由上面两张表结构匹配算出）
	ifaceImpls map[string][]string

	// 能（直接或间接）到达调度器 Update() 的函数键 → 到 Update 的一条示例路径
	reaches map[string][]string

	// 持锁期间发起的所有调用。可达集要等全部文件扫完才算得出，
	// 所以这里只记录，解析放到 report()。
	underLockCalls []underLockCall

	transitiveHigh []transitiveHit // 接收者类型已解析且命中 ⇒ 进门禁
	transitiveLow  []transitiveHit // 裸名命中、类型未解析 ⇒ 只提示

	mainOrder *mainOrderResult
}

// mainOrderResult 见文件顶部「附带的第二项检查」。
type mainOrderResult struct {
	path     string
	skipped  bool     // 没找到 main.go ⇒ 跳过（会显式打印，不静默）
	problems []string // 非空 = 失败
	lines    []string // 给人看的三行位置
}

type underLockCall struct {
	pos        token.Position
	method     string   // 方法名或函数名
	recvExpr   string   // 接收者表达式原文（普通函数为空）
	candidates []string // 接收者可能的类型名；空 = 解析不出来
	plainFunc  bool     // 形如 f()，非方法调用
}

type closureNote struct {
	pos          token.Position
	hasUpdate    bool // 闭包体内直接含调度器 Update()
	underLockPos token.Position
}

type transitiveHit struct {
	pos    token.Position
	callee string
	why    string
	path   []string // callee → … → Update()，供人工复核
}

func main() {
	verbose := flag.Bool("v", false, "打印每个调用点的明细")
	expect := flag.Int("expect", 0, "期望的调用点总数；不符即失败（0 = 不检查）")
	withTests := flag.Bool("tests", true, "把 _test.go 一并纳入扫描")
	mainPath := flag.String("main", "", "main.go 路径；检查 Start() 是否仍夹在 NewRuntime 与 NewLocalPeer 之间。留空则从被扫描目录的上一级自动找")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: go run check-lock-order.go [-v] [-expect N] [-tests=false] [-main <main.go>] <dir|file>...\n")
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	c := &checker{
		fset:                 token.NewFileSet(),
		schedulerIdents:      map[string]bool{},
		identTypes:           map[string]map[string]bool{},
		fnDirectUpdate:       map[string]bool{},
		fnDirectUpdateByName: map[string]bool{},
		callEdges:            map[string]map[string]bool{},
		ifaceMethods:         map[string]map[string]bool{},
		typeMethods:          map[string]map[string]bool{},
		structFields:         map[string]map[string]string{},
		structFieldMapVal:    map[string]map[string]string{},
		ifaceImpls:           map[string][]string{},
		reaches:              map[string][]string{},
	}

	files, root, err := collectFiles(flag.Args(), *withTests)
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(2)
	}
	c.root = root
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "错误: 没有找到任何 .go 文件")
		os.Exit(2)
	}

	parsed := make([]*ast.File, 0, len(files))
	for _, f := range files {
		af, err := parser.ParseFile(c.fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			fmt.Fprintln(os.Stderr, "解析失败:", err)
			os.Exit(2)
		}
		parsed = append(parsed, af)
	}

	// 第 1 遍：标识符 → 调度器类型；接口与具体类型的方法集。
	for _, af := range parsed {
		c.collectSchedulerIdents(af)
	}
	// 结构匹配算出「谁实现了哪个接口」——第 2 遍解析被调方时要用。
	c.computeIfaceImpls()
	// 第 2 遍：锁状态机 + 调用点 + 全量调用边。
	for _, af := range parsed {
		c.scanFile(af)
	}
	// 反向传播：谁能（间接）到达 Update()。
	c.computeReachability()

	c.mainOrder = c.checkMainOrder(*mainPath)

	os.Exit(c.report(*verbose, *expect, len(files)))
}

func collectFiles(args []string, withTests bool) ([]string, string, error) {
	var out []string
	root := ""
	for _, a := range args {
		st, err := os.Stat(a)
		if err != nil {
			return nil, "", err
		}
		if !st.IsDir() {
			out = append(out, a)
			if root == "" {
				root = filepath.Dir(a)
			}
			continue
		}
		if root == "" {
			root = a
		}
		err = filepath.WalkDir(a, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "vendor" || name == "node_modules" || strings.HasPrefix(name, ".") && name != "." {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") {
				return nil
			}
			if !withTests && strings.HasSuffix(p, "_test.go") {
				return nil
			}
			out = append(out, p)
			return nil
		})
		if err != nil {
			return nil, "", err
		}
	}
	sort.Strings(out)
	return out, root, nil
}

// ---------------------------------------------------------------------------
// 第 1 遍：哪些标识符是调度器
// ---------------------------------------------------------------------------

func isSchedulerType(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.Ident:
		return schedulerTypeNames[t.Name]
	case *ast.StarExpr:
		return isSchedulerType(t.X)
	case *ast.SelectorExpr: // server.LeaderboardScheduler
		return schedulerTypeNames[t.Sel.Name]
	}
	return false
}

// typeName 取一个类型表达式的裸名：*T → T，pkg.T → T，其余（切片 / map / 函数）→ ""。
func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return typeName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	// 泛型实例化：MapOf[string, X] / Foo[T] —— 取基础类型名。
	// 不认的话 matchmaker 的 revCache *MapOf[...] 就解析不出来，只能落到裸名兜底。
	case *ast.IndexExpr:
		return typeName(t.X)
	case *ast.IndexListExpr:
		return typeName(t.X)
	}
	return ""
}

func (c *checker) noteIdentType(name string, e ast.Expr) {
	tn := typeName(e)
	if tn == "" || name == "" || name == "_" {
		return
	}
	if c.identTypes[name] == nil {
		c.identTypes[name] = map[string]bool{}
	}
	c.identTypes[name][tn] = true
}

func (c *checker) collectSchedulerIdents(af *ast.File) {
	noteFields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, f := range fl.List {
			for _, n := range f.Names {
				c.noteIdentType(n.Name, f.Type)
			}
			if !isSchedulerType(f.Type) {
				continue
			}
			for _, n := range f.Names {
				c.schedulerIdents[n.Name] = true
			}
		}
	}

	// 接口声明的方法集
	for _, d := range af.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok || it.Methods == nil {
				continue
			}
			set := c.ifaceMethods[ts.Name.Name]
			if set == nil {
				set = map[string]bool{}
				c.ifaceMethods[ts.Name.Name] = set
			}
			for _, m := range it.Methods.List {
				for _, nm := range m.Names {
					set[nm.Name] = true
				}
			}
		}
	}
	// 结构体字段表
	for _, d := range af.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok || st.Fields == nil {
				continue
			}
			fields := c.structFields[ts.Name.Name]
			if fields == nil {
				fields = map[string]string{}
				c.structFields[ts.Name.Name] = fields
			}
			for _, f := range st.Fields.List {
				if mt, ok := f.Type.(*ast.MapType); ok {
					if vt := typeName(mt.Value); vt != "" {
						if c.structFieldMapVal[ts.Name.Name] == nil {
							c.structFieldMapVal[ts.Name.Name] = map[string]string{}
						}
						for _, nm := range f.Names {
							c.structFieldMapVal[ts.Name.Name][nm.Name] = vt
						}
					}
					continue
				}
				tn := typeName(f.Type)
				if tn == "" {
					continue
				}
				if len(f.Names) == 0 { // 内嵌字段：字段名就是类型名
					fields[tn] = tn
					continue
				}
				for _, nm := range f.Names {
					fields[nm.Name] = tn
				}
			}
		}
	}
	// 具体类型的方法集（用于结构匹配「谁实现了这个接口」）
	for _, d := range af.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv == nil || len(fd.Recv.List) == 0 {
			continue
		}
		tn := typeName(fd.Recv.List[0].Type)
		if tn == "" {
			continue
		}
		if c.typeMethods[tn] == nil {
			c.typeMethods[tn] = map[string]bool{}
		}
		c.typeMethods[tn][fd.Name.Name] = true
	}

	ast.Inspect(af, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.StructType: // 结构体字段：n.leaderboardScheduler
			noteFields(v.Fields)
		case *ast.FuncDecl: // 形参 / 返回值 / 方法接收者
			noteFields(v.Recv)
			if v.Type != nil {
				noteFields(v.Type.Params)
				noteFields(v.Type.Results)
			}
		case *ast.FuncType:
			noteFields(v.Params)
			noteFields(v.Results)
		case *ast.ValueSpec: // var scheduler LeaderboardScheduler
			if v.Type == nil {
				return true
			}
			for _, n := range v.Names {
				c.noteIdentType(n.Name, v.Type)
			}
			if isSchedulerType(v.Type) {
				for _, n := range v.Names {
					c.schedulerIdents[n.Name] = true
				}
			}
		}
		return true
	})
}

// isSchedulerRecv 判断 X.Update() 里的 X 是不是调度器。
// 取选择器链的**最后一个**标识符：n.leaderboardScheduler → leaderboardScheduler。
func (c *checker) isSchedulerRecv(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.Ident:
		return c.schedulerIdents[t.Name]
	case *ast.SelectorExpr:
		return c.schedulerIdents[t.Sel.Name]
	case *ast.ParenExpr:
		return c.isSchedulerRecv(t.X)
	}
	return false
}

// ---------------------------------------------------------------------------
// 第 2 遍：锁状态机
// ---------------------------------------------------------------------------

type frame struct {
	// 函数内的 标识符 → 类型名。**局部作用域优先于全局裸名表** ——
	// 🔴 全局表在 164 个文件里会把同名不同类型混在一起（`cache` 曾解析出 3 个候选），
	//    而函数内的形参 / 接收者 / 局部声明是无歧义的。
	locals map[string]string

	held []lockRec
	// 当前所处的闭包信息（未立即调用的 FuncLit）
	inClosure bool
	closure   token.Position
	fnKey     string
}

func (c *checker) scanFile(af *ast.File) {
	for _, d := range af.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		f := &frame{fnKey: funcKey(fd), locals: map[string]string{}}
		noteParams := func(fl *ast.FieldList) {
			if fl == nil {
				return
			}
			for _, fld := range fl.List {
				tn := typeName(fld.Type)
				if tn == "" {
					continue
				}
				for _, nm := range fld.Names {
					f.locals[nm.Name] = tn
				}
			}
		}
		noteParams(fd.Recv)
		if fd.Type != nil {
			noteParams(fd.Type.Params)
			noteParams(fd.Type.Results)
		}
		c.walkBlock(fd.Body.List, f)
		// 函数结束：defer 释放的锁在此归零（不需要显式处理，frame 随之丢弃）
	}
}

// funcKey：方法 → "接收者类型.方法名"，普通函数 → "函数名"。
func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		if tn := typeName(fd.Recv.List[0].Type); tn != "" {
			return tn + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

// walkBlock 处理一个语句块；块结束时弹出本块内取得、且**不是** defer 释放的锁。
func (c *checker) walkBlock(list []ast.Stmt, f *frame) {
	base := len(f.held)
	for _, s := range list {
		c.walkStmt(s, f)
	}
	// 块退出：保留 deferred 的（它们活到函数末尾），丢弃其余本块新增的
	if len(f.held) > base {
		kept := f.held[:base]
		for _, l := range f.held[base:] {
			if l.deferred {
				kept = append(kept, l)
			}
		}
		f.held = kept
	}
}

func (c *checker) walkStmt(s ast.Stmt, f *frame) {
	switch v := s.(type) {
	case nil:
		return

	case *ast.ExprStmt:
		if call, ok := v.X.(*ast.CallExpr); ok {
			if c.handleLockCall(call, f, false) {
				return
			}
			c.handleCall(call, f, false, false)
			return
		}
		c.scanExpr(v.X, f)

	case *ast.DeferStmt:
		// defer x.Unlock() ⇒ 把匹配的锁标记为「持有到函数结束」
		if c.handleLockCall(v.Call, f, true) {
			return
		}
		// defer x.Update() ⇒ 在函数返回时执行，此时 defer 的 Unlock 可能尚未跑（LIFO）
		c.handleCall(v.Call, f, true, false)

	case *ast.GoStmt:
		// 新 goroutine ⇒ 既不继承锁，也不构成同步调用边
		if lit, ok := v.Call.Fun.(*ast.FuncLit); ok {
			c.walkFuncLit(lit, f, false)
			for _, a := range v.Call.Args {
				c.scanExpr(a, f)
			}
			return
		}
		saved := f.held
		f.held = nil
		c.handleCall(v.Call, f, false, true)
		f.held = saved

	case *ast.BlockStmt:
		c.walkBlock(v.List, f)

	case *ast.IfStmt:
		c.walkStmt(v.Init, f)
		c.scanExpr(v.Cond, f)
		c.walkBlock(v.Body.List, f)
		c.walkStmt(v.Else, f)

	case *ast.ForStmt:
		base := len(f.held)
		c.walkStmt(v.Init, f)
		c.scanExpr(v.Cond, f)
		c.walkStmt(v.Post, f)
		c.walkBlock(v.Body.List, f)
		f.held = truncKeepDeferred(f.held, base)

	case *ast.RangeStmt:
		c.scanExpr(v.X, f)
		c.walkBlock(v.Body.List, f)

	case *ast.SwitchStmt:
		base := len(f.held)
		c.walkStmt(v.Init, f)
		c.scanExpr(v.Tag, f)
		for _, cs := range v.Body.List {
			if cc, ok := cs.(*ast.CaseClause); ok {
				for _, e := range cc.List {
					c.scanExpr(e, f)
				}
				c.walkBlock(cc.Body, f)
			}
		}
		f.held = truncKeepDeferred(f.held, base)

	case *ast.TypeSwitchStmt:
		base := len(f.held)
		c.walkStmt(v.Init, f)
		c.walkStmt(v.Assign, f)
		for _, cs := range v.Body.List {
			if cc, ok := cs.(*ast.CaseClause); ok {
				c.walkBlock(cc.Body, f)
			}
		}
		f.held = truncKeepDeferred(f.held, base)

	case *ast.SelectStmt:
		for _, cs := range v.Body.List {
			if cc, ok := cs.(*ast.CommClause); ok {
				c.walkStmt(cc.Comm, f)
				c.walkBlock(cc.Body, f)
			}
		}

	case *ast.LabeledStmt:
		c.walkStmt(v.Stmt, f)

	case *ast.AssignStmt:
		if v.Tok == token.DEFINE {
			c.noteLocalsFromAssign(v, f)
		}
		for _, e := range v.Rhs {
			c.scanExpr(e, f)
		}
		for _, e := range v.Lhs {
			c.scanExpr(e, f)
		}

	case *ast.ReturnStmt:
		for _, e := range v.Results {
			c.scanExpr(e, f)
		}

	case *ast.DeclStmt:
		if gd, ok := v.Decl.(*ast.GenDecl); ok && gd.Tok == token.VAR {
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || vs.Type == nil {
					continue
				}
				if tn := typeName(vs.Type); tn != "" {
					for _, nm := range vs.Names {
						f.locals[nm.Name] = tn
					}
				}
			}
		}
		ast.Inspect(v, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok {
				if _, isLit := e.(*ast.FuncLit); isLit {
					c.scanExpr(e, f)
					return false
				}
			}
			if call, ok := n.(*ast.CallExpr); ok {
				c.handleCall(call, f, false, false)
			}
			return true
		})

	default:
		// 其余语句里可能藏着 CallExpr / FuncLit
		ast.Inspect(s, func(n ast.Node) bool {
			switch e := n.(type) {
			case *ast.FuncLit:
				c.walkFuncLit(e, f, false)
				return false
			case *ast.CallExpr:
				c.handleCall(e, f, false, false)
				return false // handleCall 会自行下钻实参
			}
			return true
		})
	}
}

func truncKeepDeferred(held []lockRec, base int) []lockRec {
	if len(held) <= base {
		return held
	}
	kept := held[:base]
	for _, l := range held[base:] {
		if l.deferred {
			kept = append(kept, l)
		}
	}
	return kept
}

// handleLockCall 处理 Lock/RLock/Unlock/RUnlock，返回是否已消费该调用。
func (c *checker) handleLockCall(call *ast.CallExpr, f *frame, isDefer bool) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	recv := types.ExprString(sel.X)
	switch sel.Sel.Name {
	case "Lock", "RLock":
		if isDefer {
			// defer x.Lock() 极罕见且语义诡异；记为普通获取，位置用 defer 处
			return true
		}
		f.held = append(f.held, lockRec{
			expr: recv, kind: sel.Sel.Name, pos: c.fset.Position(call.Pos()),
		})
		return true
	case "Unlock", "RUnlock":
		if isDefer {
			c.deferredUnlockSeen++
			// 🔴 关键分支：defer 的 Unlock **不弹栈**，而是把对应的锁标记为
			// 「持有到函数结束」。词法版正是在这里判反的。
			for i := len(f.held) - 1; i >= 0; i-- {
				if f.held[i].expr == recv {
					f.held[i].deferred = true
					break
				}
			}
			return true
		}
		for i := len(f.held) - 1; i >= 0; i-- {
			if f.held[i].expr == recv {
				f.held = append(f.held[:i], f.held[i+1:]...)
				break
			}
		}
		return true
	}
	return false
}

// handleCall 处理一次函数调用：可能是 Update()、可能带 FuncLit 实参。
func (c *checker) handleCall(call *ast.CallExpr, f *frame, viaDefer, viaGo bool) {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Update" && len(call.Args) == 0 {
		site := callSite{
			pos:       c.fset.Position(call.Pos()),
			recv:      types.ExprString(sel.X),
			held:      append([]lockRec(nil), f.held...),
			inClosure: f.inClosure,
			closure:   f.closure,
			viaDefer:  viaDefer,
			viaGo:     viaGo,
		}
		if c.isSchedulerRecv(sel.X) {
			c.sites = append(c.sites, site)
			if f.fnKey != "" {
				c.fnDirectUpdate[f.fnKey] = true
				if i := strings.LastIndex(f.fnKey, "."); i >= 0 {
					c.fnDirectUpdateByName[f.fnKey[i+1:]] = true
				} else {
					c.fnDirectUpdateByName[f.fnKey] = true
				}
			}
		} else {
			c.unclassified = append(c.unclassified, site)
		}
	}

	// 记调用边（不管持不持锁）—— 跨函数多层可达性要用
	var uc *underLockCall
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		uc = &underLockCall{
			pos: c.fset.Position(call.Pos()), method: fn.Name, plainFunc: true,
		}
	case *ast.SelectorExpr:
		uc = &underLockCall{
			pos:        c.fset.Position(call.Pos()),
			method:     fn.Sel.Name,
			recvExpr:   types.ExprString(fn.X),
			candidates: c.recvTypeCandidates(fn.X, f),
		}
	}
	if uc != nil && f.fnKey != "" {
		if c.callEdges[f.fnKey] == nil {
			c.callEdges[f.fnKey] = map[string]bool{}
		}
		for _, k := range c.calleeKeys(*uc) {
			c.callEdges[f.fnKey][k] = true
		}
	}
	// 持锁时发起的调用单独留一份，report() 里做可达性判定
	if uc != nil && len(f.held) > 0 {
		c.underLockCalls = append(c.underLockCalls, *uc)
	}

	// 立即调用的字面量 func(){...}() 继承锁；作为实参传出去的不继承
	if lit, ok := call.Fun.(*ast.FuncLit); ok {
		c.walkFuncLit(lit, f, true)
	}
	for _, a := range call.Args {
		c.scanExpr(a, f)
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		c.scanExpr(sel.X, f)
	}
}

// calleeKeys 把一次调用翻译成候选的「函数键」。
//   - 普通函数 f()          → ["f"]
//   - 方法 x.M()，x 类型已解析为 T → ["T.M"]；T 若是接口，再展开成每个实现 "Impl.M"
//   - 解析不出来            → 空（交给裸名兜底，那一档只提示不进门禁）
func (c *checker) calleeKeys(uc underLockCall) []string {
	if uc.plainFunc {
		return []string{uc.method}
	}
	var out []string
	for _, t := range uc.candidates {
		out = append(out, t+"."+uc.method)
		for _, impl := range c.ifaceImpls[t] {
			out = append(out, impl+"."+uc.method)
		}
	}
	return out
}

// computeIfaceImpls 用**结构匹配**算「谁实现了这个接口」：
// 具体类型的方法集覆盖接口的方法集即算实现。没有类型信息，这是能做到的最好近似。
//
// ⚠️ 方法数很少的接口（如只有一个 Update()）会匹配到一大票无关类型 ⇒ 假阳。
// 因此只对 **≥3 个方法** 的接口做展开，小接口留给裸名兜底那一档。
// 本仓库关心的两个接口分别是 5 个（LeaderboardScheduler）与 11 个（LeaderboardCache）方法。
func (c *checker) computeIfaceImpls() {
	const minMethods = 3
	for iface, want := range c.ifaceMethods {
		if len(want) < minMethods {
			continue
		}
		for tn, have := range c.typeMethods {
			if tn == iface {
				continue
			}
			ok := true
			for m := range want {
				if !have[m] {
					ok = false
					break
				}
			}
			if ok {
				c.ifaceImpls[iface] = append(c.ifaceImpls[iface], tn)
			}
		}
		sort.Strings(c.ifaceImpls[iface])
	}
}

// computeReachability 从「直接调 Update() 的函数」出发反向传播，
// 算出所有能到达 Update() 的函数键，并为每个键留一条示例路径。
// 用工作表 + visited，环不会让它转不出来。
func (c *checker) computeReachability() {
	type item struct {
		key  string
		path []string
	}
	// 反向边：被调方 → 调用方
	rev := map[string][]string{}
	for caller, callees := range c.callEdges {
		for callee := range callees {
			rev[callee] = append(rev[callee], caller)
		}
	}

	queue := make([]item, 0, len(c.fnDirectUpdate))
	for k := range c.fnDirectUpdate {
		c.reaches[k] = []string{k + " → Update()"}
		queue = append(queue, item{k, c.reaches[k]})
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i].key < queue[j].key })

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, caller := range rev[cur.key] {
			if _, seen := c.reaches[caller]; seen {
				continue
			}
			p := append([]string{caller + " →"}, cur.path...)
			c.reaches[caller] = p
			queue = append(queue, item{caller, p})
		}
	}
}

// noteLocalsFromAssign 处理 `x := …`，把能推出来的类型记进函数局部作用域。
// 只认三种最常见、且推断确定的写法；推不出来就不记（宁可解析不出，也不要猜错）。
func (c *checker) noteLocalsFromAssign(a *ast.AssignStmt, f *frame) {
	for i, lhs := range a.Lhs {
		id, ok := lhs.(*ast.Ident)
		if !ok || id.Name == "_" || i >= len(a.Rhs) && len(a.Rhs) != 1 {
			continue
		}
		var rhs ast.Expr
		if len(a.Rhs) == len(a.Lhs) {
			rhs = a.Rhs[i]
		} else if len(a.Rhs) == 1 {
			rhs = a.Rhs[0] // v, ok := m[k] / v, err := f()
		}
		if tn := c.inferType(rhs, f); tn != "" {
			f.locals[id.Name] = tn
		}
	}
}

// inferType 只处理确定性高的几种右值。
func (c *checker) inferType(e ast.Expr, f *frame) string {
	switch t := e.(type) {
	case *ast.UnaryExpr: // &T{…}
		if t.Op == token.AND {
			return c.inferType(t.X, f)
		}
	case *ast.CompositeLit: // T{…}
		return typeName(t.Type)
	case *ast.IndexExpr: // v := x.m[k] —— 取 map 字段的值类型
		if sel, ok := t.X.(*ast.SelectorExpr); ok {
			for _, base := range c.recvTypeCandidates(sel.X, f) {
				if vt, ok := c.structFieldMapVal[base][sel.Sel.Name]; ok {
					return vt
				}
			}
		}
	}
	return ""
}

// recvTypeCandidates 猜 x.M() 里 x 的类型名。取选择器链最后一个标识符，
// 查它在本包里被声明过的类型。查不到返回 nil —— 那就是「解析不出来」。
func (c *checker) recvTypeCandidates(e ast.Expr, f *frame) []string {
	switch t := e.(type) {
	case *ast.ParenExpr:
		return c.recvTypeCandidates(t.X, f)

	case *ast.Ident:
		// 局部作用域优先：函数内的形参 / 接收者 / 局部声明是无歧义的。
		if f != nil {
			if tn, ok := f.locals[t.Name]; ok {
				return []string{tn}
			}
		}
		return setToSorted(c.identTypes[t.Name])

	case *ast.SelectorExpr:
		// 🔴 先按**字段**解析：拿到 X 的类型，再在那个结构体里查字段名。
		// 这比「按裸名查全局标识符表」精确得多 —— rankCache.cache 只有这样
		// 才能解析成 SkipList 而不是把所有叫 cache 的类型都算上。
		var out []string
		seen := map[string]bool{}
		for _, base := range c.recvTypeCandidates(t.X, f) {
			if ft, ok := c.structFields[base][t.Sel.Name]; ok && !seen[ft] {
				seen[ft] = true
				out = append(out, ft)
			}
		}
		if len(out) > 0 {
			sort.Strings(out)
			return out
		}
		// 字段表查不到（跨包结构体、内嵌提升等）⇒ 退回裸名，精度较低
		return setToSorted(c.identTypes[t.Sel.Name])
	}
	return nil
}

func setToSorted(set map[string]bool) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// scanExpr 在表达式里找 FuncLit 与嵌套调用。
func (c *checker) scanExpr(e ast.Expr, f *frame) {
	if e == nil {
		return
	}
	switch v := e.(type) {
	case *ast.FuncLit:
		c.walkFuncLit(v, f, false)
	case *ast.CallExpr:
		c.handleCall(v, f, false, false)
	default:
		ast.Inspect(e, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				c.walkFuncLit(x, f, false)
				return false
			case *ast.CallExpr:
				c.handleCall(x, f, false, false)
				return false
			}
			return true
		})
	}
}

// walkFuncLit 走进函数字面量。
// inherit=true  ⇒ 立即调用（func(){}()），锁继承。
// inherit=false ⇒ 作为值传出（wk.Submit(func(){}) / go func(){}()），
//
//	锁**不**继承 —— 但这条假设本身要报出来，见 reportBlindSpots。
func (c *checker) walkFuncLit(lit *ast.FuncLit, parent *frame, inherit bool) {
	nf := &frame{fnKey: parent.fnKey, locals: map[string]string{}}
	if !inherit {
		// 🔴 异步闭包（交给 worker 池 / time.AfterFunc / go 语句）里的调用，
		// **不是**外层函数的同步调用边。用合成键把它们隔开，否则会造出
		// 「Update → queueEndActiveElapse → Update」这种其实经由定时器的假路径。
		nf.fnKey = fmt.Sprintf("%s$func@%d", parent.fnKey, c.fset.Position(lit.Pos()).Line)
	}
	for k, v := range parent.locals { // 闭包捕获外层变量
		nf.locals[k] = v
	}
	if lit.Type != nil && lit.Type.Params != nil {
		for _, fld := range lit.Type.Params.List {
			if tn := typeName(fld.Type); tn != "" {
				for _, nm := range fld.Names {
					nf.locals[nm.Name] = tn
				}
			}
		}
	}
	if inherit {
		nf.held = append([]lockRec(nil), parent.held...)
		nf.inClosure = parent.inClosure
		nf.closure = parent.closure
		c.walkBlock(lit.Body.List, nf)
		return
	}

	pos := c.fset.Position(lit.Pos())
	nf.inClosure = true
	nf.closure = pos
	before := len(c.sites)
	c.walkBlock(lit.Body.List, nf)
	if len(parent.held) > 0 {
		note := closureNote{pos: pos, hasUpdate: len(c.sites) > before}
		if n := len(parent.held); n > 0 {
			note.underLockPos = parent.held[n-1].pos
		}
		c.asyncClosure = append(c.asyncClosure, note)
	}
}

// ---------------------------------------------------------------------------
// main.go 启动顺序（见文件顶部说明）
// ---------------------------------------------------------------------------

// 要按顺序出现的三个调用。改这张表时**必须**同步改 design.md D14 与 tasks 5.6。
var mainOrderSteps = []struct {
	label  string
	method string
	why    string
}{
	{"NewRuntime", "NewRuntime", "InitModule 建榜 → Update()，与 started 的写同 goroutine 且在其之前"},
	{"Start", "Start", "这里裸写 ls.started = true"},
	{"NewLocalPeer", "NewLocalPeer", "内含 worker.New(128)，peer 复制会在新 goroutine 里调 Update()"},
}

func (c *checker) checkMainOrder(explicit string) *mainOrderResult {
	path := explicit
	if path == "" {
		// 自动找：被扫描目录（通常是 <root>/server）的上一级
		if abs, err := filepath.Abs(c.root); err == nil {
			cand := filepath.Join(filepath.Dir(abs), "main.go")
			if st, err := os.Stat(cand); err == nil && !st.IsDir() {
				path = cand
			}
		}
	}
	res := &mainOrderResult{path: path}
	if path == "" {
		res.skipped = true
		return res
	}

	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		res.problems = append(res.problems, "解析失败："+err.Error())
		return res
	}
	var mainFn *ast.FuncDecl
	for _, d := range af.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == "main" {
			mainFn = fd
			break
		}
	}
	if mainFn == nil || mainFn.Body == nil {
		res.problems = append(res.problems, "找不到 func main() —— 文件结构变了，请人工复查")
		return res
	}

	found := make([]token.Position, len(mainOrderSteps))
	ok := make([]bool, len(mainOrderSteps))
	ast.Inspect(mainFn.Body, func(n ast.Node) bool {
		call, isCall := n.(*ast.CallExpr)
		if !isCall {
			return true
		}
		sel, isSel := call.Fun.(*ast.SelectorExpr)
		if !isSel {
			return true
		}
		for i, step := range mainOrderSteps {
			if ok[i] || sel.Sel.Name != step.method {
				continue
			}
			// Start() 要认准接收者是调度器，否则会撞上别的 .Start()
			if step.method == "Start" {
				recv := strings.ToLower(types.ExprString(sel.X))
				if !strings.Contains(recv, "leaderboardscheduler") {
					continue
				}
			}
			found[i] = fset.Position(call.Pos())
			ok[i] = true
		}
		return true
	})

	for i, step := range mainOrderSteps {
		if !ok[i] {
			res.problems = append(res.problems,
				fmt.Sprintf("在 func main() 里找不到 %s(...) —— 结构变了，请人工复查启动顺序", step.label))
			continue
		}
		res.lines = append(res.lines,
			fmt.Sprintf("%-14s %s:%d   （%s）", step.label, filepath.Base(path), found[i].Line, step.why))
	}
	if len(res.problems) > 0 {
		return res
	}
	for i := 1; i < len(found); i++ {
		if found[i].Offset <= found[i-1].Offset {
			res.problems = append(res.problems, fmt.Sprintf(
				"顺序不对：%s（第 %d 行）应当在 %s（第 %d 行）**之后**",
				mainOrderSteps[i].label, found[i].Line, mainOrderSteps[i-1].label, found[i-1].Line))
		}
	}
	return res
}

// ---------------------------------------------------------------------------
// 报告
// ---------------------------------------------------------------------------

func (c *checker) rel(p token.Position) string {
	r, err := filepath.Rel(c.root, p.Filename)
	if err != nil {
		r = p.Filename
	}
	return fmt.Sprintf("%s:%d", r, p.Line)
}

func (c *checker) report(verbose bool, expect, nFiles int) int {
	sort.Slice(c.sites, func(i, j int) bool { return c.sites[i].pos.String() < c.sites[j].pos.String() })

	var violations []callSite
	for _, s := range c.sites {
		if len(s.held) > 0 {
			violations = append(violations, s)
		}
	}

	fmt.Printf("check-lock-order —— 扫描 %d 个 .go 文件\n", nFiles)
	fmt.Printf("不变量：没有任何代码路径在持锁时调用 LeaderboardScheduler.Update()\n\n")

	fmt.Printf("调度器 Update() 调用点：%d 个\n", len(c.sites))
	if verbose || len(violations) > 0 {
		for _, s := range c.sites {
			mark := "  ok  "
			if len(s.held) > 0 {
				mark = "  🔴  "
			}
			extra := ""
			switch {
			case s.viaGo:
				extra = "  [go —— 新 goroutine，不继承锁]"
			case s.viaDefer:
				extra = "  [defer —— 函数返回时执行，与 defer Unlock 的 LIFO 次序有关]"
			case s.inClosure:
				extra = fmt.Sprintf("  [闭包 @ %s —— 假定异步，锁未继承]", c.rel(s.closure))
			}
			fmt.Printf("%s%-46s %s()%s\n", mark, c.rel(s.pos), s.recv+".Update", extra)
			for _, l := range s.held {
				kind := l.kind
				if l.deferred {
					kind += " + defer Unlock"
				}
				fmt.Printf("        └─ 持有 %s.%s  （取自 %s）\n", l.expr, kind, c.rel(l.pos))
			}
		}
	}

	if len(c.unclassified) > 0 {
		fmt.Printf("\n未归类的 .Update() 调用点：%d 个（接收者不像调度器；列出以免静默漏掉）\n", len(c.unclassified))
		for _, s := range c.unclassified {
			fmt.Printf("  --  %-46s %s()\n", c.rel(s.pos), s.recv+".Update")
		}
	}

	c.resolveTransitive()
	if len(c.transitiveHigh) > 0 {
		fmt.Printf("\n🔴 跨函数（多层，被调方已解析）：持锁时调用了能到达 Update() 的函数：%d 处\n", len(c.transitiveHigh))
		for _, t := range c.transitiveHigh {
			fmt.Printf("  %s  → %s   %s\n", c.rel(t.pos), t.callee, t.why)
			fmt.Printf("      路径：%s\n", strings.Join(t.path, " "))
		}
	}
	if len(c.transitiveLow) > 0 {
		fmt.Printf("\n⚠️  跨函数（低置信）：%d 处 —— 方法**裸名**撞上了能到达 Update() 的方法，\n", len(c.transitiveLow))
		fmt.Printf("    但接收者类型解析不出来 ⇒ 多半是同名方法串味。**不进门禁**，用 -v 看清单。\n")
		if verbose {
			for _, t := range c.transitiveLow {
				fmt.Printf("      %s  → %s.%s   %s\n", c.rel(t.pos), t.callee, t.why, "（需人工确认）")
			}
		}
	}

	c.reportMainOrder()
	c.reportBlindSpots()

	fmt.Println()
	fail := false
	if len(c.sites) == 0 {
		fmt.Println("🔴 自检失败：一个调度器 Update() 调用点都没找到。")
		fmt.Println("   检查器找不到东西和代码没问题，在退出码上长得一样 —— 这里当作失败。")
		fmt.Println("   多半是类型被改名了（见 schedulerTypeNames）或路径传错了。")
		fail = true
	}
	if expect > 0 && len(c.sites) != expect {
		fmt.Printf("🔴 自检失败：期望 %d 个调用点，实际 %d 个。\n", expect, len(c.sites))
		fmt.Printf("   新增调用点是正常的 —— 确认它不持锁后，把 -expect 改成 %d 并同步 design.md D3。\n", len(c.sites))
		fail = true
	}
	if len(violations) > 0 {
		fmt.Printf("🔴 违规：%d 处在持锁时调用了 Update()。\n", len(violations))
		fmt.Println("   后果是 ABBA 死锁（design.md D3），比丢回调更难查。")
		fail = true
	}
	if len(c.transitiveHigh) > 0 {
		fmt.Println("🔴 违规：存在跨函数的持锁调用链（详见上面的路径）。")
		fail = true
	}
	if c.mainOrder != nil && len(c.mainOrder.problems) > 0 {
		fmt.Println("🔴 违规：main.go 的启动顺序不再保证 ls.started 的可见性（design.md D14）。")
		fmt.Println("   ⇒ 把 ls.started 改成 atomic.Bool（D14 的选项 A，四行），或查清顺序为何变了。")
		fail = true
	}
	if fail {
		return 1
	}
	fmt.Printf("✅ 通过：%d 个调用点，无一在持锁时调用。\n", len(c.sites))
	return 0
}

// resolveTransitive 把「持锁期间的调用」与「能（间接）到达 Update() 的函数」对起来。
//   - 被调方键解析得出且在可达集里 ⇒ 高置信，进门禁，并打印整条路径
//   - 只有裸名命中（类型解析不出）⇒ 低置信，只提示
//
// 低置信这一档存在的理由：`batch.Delete()`（bluge 索引）与
// `(*LocalLeaderboardCache).Delete()` 裸名相同。按裸名判就是 7 个假阳，
// 一个天天误报的门禁等于没有门禁。
func (c *checker) resolveTransitive() {
	// 同一个调用点若已按「直接持锁调 Update()」报过，就别再报一遍跨函数版。
	direct := map[string]bool{}
	for _, s := range c.sites {
		if len(s.held) > 0 {
			direct[s.pos.String()] = true
		}
	}
	for _, uc := range c.underLockCalls {
		if direct[uc.pos.String()] {
			continue
		}
		// 🔴 接收者解析出**多个**候选类型 = 名字串味没消干净，不能进门禁。
		// 实测教训：rankCache.cache 曾被解析成 LeaderboardCache/LocalLeaderboardRankCache/SkipList
		// 三选一，随便命中一个就报红 ⇒ 4 个假阳。
		ambiguous := !uc.plainFunc && len(uc.candidates) > 1

		matched := false
		for _, key := range c.calleeKeys(uc) {
			path, ok := c.reaches[key]
			if !ok {
				continue
			}
			if ambiguous {
				c.transitiveLow = append(c.transitiveLow, transitiveHit{
					pos: uc.pos, callee: uc.recvExpr,
					why: uc.method + " —— 接收者有 " + strconv.Itoa(len(uc.candidates)) +
						" 个候选类型（" + strings.Join(uc.candidates, "/") + "），无法确定是哪个",
				})
				matched = true
				break
			}
			why := "（包级函数）"
			if !uc.plainFunc {
				why = "（接收者 " + uc.recvExpr + " 解析为 " + strings.Join(uc.candidates, "/") + "）"
			}
			if len(path) > 1 {
				// 层数 = 链上函数个数（被调方本身算第 1 层，Update() 不算）。
				why += fmt.Sprintf("  ⚠️ 经 %d 层调用才到 Update()", len(path))
			}
			c.transitiveHigh = append(c.transitiveHigh, transitiveHit{
				pos: uc.pos, callee: key, why: why, path: path,
			})
			matched = true
			break // 一个调用点报一次就够，路径已足以定位
		}
		if matched || uc.plainFunc {
			continue
		}
		// 🔴 接收者已**无歧义**地解析出来、而它到不了 Update() ⇒ 这一处是干净的，
		// 不再退回裸名兜底。退回去只会造噪音：`batch.Delete` 是 bluge 的索引批量，
		// 与 (*LocalLeaderboardCache).Delete 只是重名。
		if len(uc.candidates) == 1 {
			continue
		}
		if c.fnDirectUpdateByName[uc.method] || c.reachesByName(uc.method) {
			why := "接收者类型未解析"
			if len(uc.candidates) > 0 {
				why = "接收者解析为 " + strings.Join(uc.candidates, "/") + "，其上无此方法"
			}
			c.transitiveLow = append(c.transitiveLow, transitiveHit{
				pos: uc.pos, callee: uc.recvExpr, why: uc.method + " —— " + why,
			})
		}
	}
	sort.Slice(c.transitiveHigh, func(i, j int) bool { return c.transitiveHigh[i].pos.String() < c.transitiveHigh[j].pos.String() })
	sort.Slice(c.transitiveLow, func(i, j int) bool { return c.transitiveLow[i].pos.String() < c.transitiveLow[j].pos.String() })
}

// reachesByName：有没有**任何**类型的同名方法能到达 Update()。只用于低置信那一档。
func (c *checker) reachesByName(method string) bool {
	for k := range c.reaches {
		if i := strings.LastIndex(k, "."); i >= 0 && k[i+1:] == method {
			return true
		}
	}
	return false
}

func (c *checker) reportMainOrder() {
	m := c.mainOrder
	if m == nil {
		return
	}
	fmt.Println("\n──────  main.go 启动顺序（ls.started 的可见性，design.md D14）  ──────")
	if m.skipped {
		fmt.Println("⏭  未检查 —— 没找到 main.go。用 -main <路径> 指定。")
		fmt.Println("   ⚠️ 这不是「通过」。门禁里必须显式传 -main，否则这一项等于没跑。")
		return
	}
	for _, l := range m.lines {
		fmt.Println("  " + l)
	}
	if len(m.problems) == 0 {
		fmt.Println("✅ Start() 仍夹在 NewRuntime 与 NewLocalPeer 之间 ⇒ D14 选 B 的前提成立。")
		return
	}
	for _, p := range m.problems {
		fmt.Println("🔴 " + p)
	}
}

func (c *checker) reportBlindSpots() {
	fmt.Println("\n────────────────  它看不见什么（每次运行都打印，别只看退出码）  ────────────────")
	fmt.Printf("1. 跨函数已做**任意层**（可达集 %d 个函数键，含接口结构匹配展开）。\n", len(c.reaches))
	fmt.Println("   仍看不见：函数值 / 回调参数（fn := obj.Method; fn() 这种间接调用）、")
	fmt.Println("   以及**跨包**的调用方与实现（只分析被扫描到的这些文件）。")
	fmt.Println("2. 类型解析是按「标识符声明处的类型名」做的，不是真正的类型检查 ——")
	fmt.Printf("   同名不同类型会串味（已降级为低置信）；接口靠**结构匹配**展开（%d 个接口有实现），\n", len(c.ifaceImpls))
	fmt.Println("   方法数 <3 的小接口**不展开**（会匹配到一大票无关类型），那部分是假阴。")
	risky := make([]closureNote, 0)
	for _, n := range c.asyncClosure {
		if n.hasUpdate {
			risky = append(risky, n)
		}
	}
	switch {
	case len(risky) > 0:
		fmt.Printf("3. 🔴 持锁函数体内的函数字面量**且体内含 Update()**：%d 处，假定异步执行、锁未继承。\n", len(risky))
		fmt.Println("   若其中任何一处其实是同步回调（形如 WithLock(func(){…})），这条假设就错了 ⇒ 假阴。")
		for _, n := range risky {
			fmt.Println("     " + c.rel(n.pos) + "  ← 必须人工确认它不是同步回调")
		}
	case len(c.asyncClosure) > 0:
		fmt.Printf("3. 持锁函数体内有 %d 处函数字面量，但**均不含 Update() 调用** ⇒ 本次无影响。\n", len(c.asyncClosure))
		fmt.Println("   （假定它们异步执行：既不继承锁，也不算外层函数的同步调用边。")
		fmt.Println("    哪天有人往里加 Update()，上面这行会变成红的。）")
	default:
		fmt.Println("3. 持锁函数体内没有函数字面量 ⇒ 本次不涉及「闭包是否同步执行」的假设。")
	}
	fmt.Println("4. 只看被扫描到的包。跨包、反射、通过接口值间接调用，都不在范围内。")
	fmt.Printf("5. 处理过的 defer Unlock：%d 处 —— 这一类词法检查会判反，本工具按「持有到函数结束」计。\n", c.deferredUnlockSeen)
}
