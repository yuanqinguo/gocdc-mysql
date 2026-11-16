package mysql

import (
	"database/sql"
	"strings"
	"testing"

	"gocdc/internal/model"
)

// diffDefs 决策矩阵（2026-11-09 全量对账改造）：全部差异类别必须产出可执行
// 语句 + 正确的白名单标记——approve 必炸陷阱（非白名单差异无语句可执行）的
// 回归防线。
func nd(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

func TestDiffDefs_DecisionMatrix(t *testing.T) {
	cases := []struct {
		name        string
		src, tgt    *tableDef
		wantKinds   []string // 语句种类（执行序）
		wantWhite   []bool   // 逐条白名单标记
		whitelisted bool     // 整体（auto 判据）
	}{
		{
			name:        "无差异",
			src:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint", Default: nd("")}}, PK: []string{"id"}},
			tgt:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint", Default: nd("")}}, PK: []string{"id"}},
			whitelisted: true,
		},
		{
			name: "加列（白名单）",
			src: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"},
			}, PK: []string{"id"}},
			tgt:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}},
			wantKinds:   []string{KindAddColumn},
			wantWhite:   []bool{true},
			whitelisted: true,
		},
		{
			name: "删列（白名单外，须生成语句）",
			src:  &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"},
			}, PK: []string{"id"}},
			wantKinds:   []string{KindDropColumn},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name: "列改名（同位置同类型 → CHANGE，白名单外）",
			src: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "user_name", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"},
			}, PK: []string{"id"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "name", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"},
			}, PK: []string{"id"}},
			wantKinds:   []string{KindRenameColumn},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name: "同名列类型变更（不配对改名，直接 MODIFY）",
			src: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "amount", Ordinal: 2, ColumnType: "decimal", RawType: "decimal(10,2)"},
			}, PK: []string{"id"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "amount", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(32)"},
			}, PK: []string{"id"}},
			// 同名 = 共有列类型变更（改名配对只作用于名字消失的目标独有列）
			wantKinds:   []string{KindModifyColumn},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name:        "类型变更（白名单外，须生成语句）",
			src:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}},
			tgt:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "int", RawType: "int"}}, PK: []string{"id"}},
			wantKinds:   []string{KindModifyColumn},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name: "varchar 扩容（白名单）",
			src: &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(128)"}}, PK: []string{"id"}},
			tgt: &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"}}, PK: []string{"id"}},
			wantKinds:   []string{KindModifySafe},
			wantWhite:   []bool{true},
			whitelisted: true,
		},
		{
			name: "varchar 缩容（白名单外）",
			src: &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(32)"}}, PK: []string{"id"}},
			tgt: &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "memo", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"}}, PK: []string{"id"}},
			wantKinds:   []string{KindModifyColumn},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name:        "同类型注释/默认值/可空变更（白名单）",
			src:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "int", RawType: "int", Nullable: true, Comment: "新注释", Default: nd("5")}}, PK: []string{"id"}},
			tgt:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "int", RawType: "int", Comment: "旧注释"}}, PK: []string{"id"}},
			wantKinds:   []string{KindModifySafe},
			wantWhite:   []bool{true},
			whitelisted: true,
		},
		{
			name: "主键变更（自增列随主键保留 → 单条原子 ALTER）",
			src: &tableDef{Cols: []ColDef{
				{Name: "oid", Ordinal: 1, ColumnType: "bigint", RawType: "bigint", AutoInc: true},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint"},
			}, PK: []string{"oid"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "oid", Ordinal: 1, ColumnType: "bigint", RawType: "bigint", AutoInc: true},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint"},
			}, PK: []string{"uid"}},
			// tgt 旧主键列 uid 非自增 → 无前置；新主键列 oid 双方都已自增 → 无后置；
			// DROP+ADD 合并单条（分开执行 DROP PRIMARY KEY 对自增列报 1075）
			wantKinds:   []string{KindPKChange},
			wantWhite:   []bool{false},
			whitelisted: false,
		},
		{
			name: "旧主键自增列退出主键（前置去自增 + 原子换键）",
			src: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint"},
			}, PK: []string{"uid"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint", AutoInc: true},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint"},
			}, PK: []string{"id"}},
			// tgt 旧主键 id 自增且退出新主键 → 先 MODIFY 去自增，再单条 DROP+ADD
			wantKinds:   []string{KindDropAutoInc, KindPKChange},
			wantWhite:   []bool{false, false},
			whitelisted: false,
		},
		{
			name: "新主键列源端自增目标未自增（原子换键 + 后置恢复）",
			src: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint", AutoInc: true},
			}, PK: []string{"uid"}},
			tgt: &tableDef{Cols: []ColDef{
				{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
				{Name: "uid", Ordinal: 2, ColumnType: "bigint", RawType: "bigint"},
			}, PK: []string{"id"}},
			// tgt 旧主键 id 非自增 → 无前置；新主键 uid 源端自增目标非自增 → 后置恢复
			wantKinds:   []string{KindPKChange, KindRestoreAutoInc},
			wantWhite:   []bool{false, false},
			whitelisted: false,
		},
		{
			name:        "表注释（白名单）",
			src:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}, Comment: "新表注释"},
			tgt:         &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}, Comment: "旧表注释"},
			wantKinds:   []string{KindTableComment},
			wantWhite:   []bool{true},
			whitelisted: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := diffDefs(tc.src, tc.tgt, "dstdb", "t1")
			if len(res.Stmts) != len(tc.wantKinds) {
				t.Fatalf("语句数 %d != 期望 %d；got=%+v", len(res.Stmts), len(tc.wantKinds), res.AllStmts())
			}
			for i, k := range tc.wantKinds {
				if res.Stmts[i].Kind != k {
					t.Errorf("语句[%d] kind=%s 期望 %s（SQL=%s）", i, res.Stmts[i].Kind, k, res.Stmts[i].SQL)
				}
				if res.Stmts[i].Whitelisted != tc.wantWhite[i] {
					t.Errorf("语句[%d]（%s）白名单标记=%v 期望 %v", i, res.Stmts[i].Kind, res.Stmts[i].Whitelisted, tc.wantWhite[i])
				}
				if strings.TrimSpace(res.Stmts[i].SQL) == "" || strings.TrimSpace(res.Stmts[i].Why) == "" {
					t.Errorf("语句[%d] SQL/Why 不得为空（审计与执行依赖）", i)
				}
			}
			if res.Whitelisted != tc.whitelisted {
				t.Errorf("整体白名单=%v 期望 %v（summary=%s）", res.Whitelisted, tc.whitelisted, res.Summary)
			}
			// 便捷访问器一致性
			if got := len(res.WhitelistedStmts()); got != countWhitelisted(res.Stmts) {
				t.Errorf("WhitelistedStmts 数 %d != countWhitelisted %d", got, countWhitelisted(res.Stmts))
			}
		})
	}
}

// 混合批次（用户场景：一次审批须覆盖全部累计变更）：加列+删列+改名+类型变更。
func TestDiffDefs_MultiChangeBatch(t *testing.T) {
	src := &tableDef{Cols: []ColDef{
		{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
		{Name: "buyer_name", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"}, // rename from user_name
		{Name: "amount", Ordinal: 3, ColumnType: "decimal", RawType: "decimal(12,2)"},   // type change
		{Name: "channel", Ordinal: 4, ColumnType: "int", RawType: "int"},                // new column
	}, PK: []string{"id"}}
	tgt := &tableDef{Cols: []ColDef{
		{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
		{Name: "user_name", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(64)"},
		{Name: "amount", Ordinal: 3, ColumnType: "int", RawType: "int"},
		{Name: "legacy", Ordinal: 4, ColumnType: "varchar", RawType: "varchar(16)"}, // dropped
	}, PK: []string{"id"}}

	res := diffDefs(src, tgt, "dstdb", "orders")
	if res.Whitelisted {
		t.Fatal("混合批次必须判非白名单（auto 转 pause）")
	}
	if len(res.AllStmts()) < 4 {
		t.Fatalf("须覆盖全部四类变更，got %d 条: %v", len(res.AllStmts()), res.AllStmts())
	}
	var hasRename, hasDrop, hasType, hasAdd bool
	for _, s := range res.Stmts {
		switch s.Kind {
		case KindRenameColumn:
			hasRename = strings.Contains(s.SQL, "CHANGE COLUMN `user_name` `buyer_name`")
		case KindDropColumn:
			hasDrop = strings.Contains(s.SQL, "DROP COLUMN `legacy`")
		case KindModifyColumn:
			hasType = strings.Contains(s.SQL, "MODIFY COLUMN") && strings.Contains(s.SQL, "`amount`")
		case KindAddColumn:
			hasAdd = strings.Contains(s.SQL, "ADD COLUMN") && strings.Contains(s.SQL, "`channel`")
		}
	}
	if !hasRename || !hasDrop || !hasType || !hasAdd {
		t.Fatalf("四类变更覆盖不全 rename=%v drop=%v type=%v add=%v; stmts=%+v",
			hasRename, hasDrop, hasType, hasAdd, res.Stmts)
	}
}

// 同位置同 DATA_TYPE 即配对改名（既定启发式）：varchar(16)→varchar(32) 改名
// 一条 CHANGE 覆盖（宽度差异随源定义采纳）。
func TestDiffDefs_RenamePairsSameFamilyWidth(t *testing.T) {
	src := &tableDef{Cols: []ColDef{
		{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
		{Name: "memo2", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(32)"},
	}, PK: []string{"id"}}
	tgt := &tableDef{Cols: []ColDef{
		{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"},
		{Name: "memo1", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(16)"},
	}, PK: []string{"id"}}
	res := diffDefs(src, tgt, "dstdb", "t1")
	if len(res.Stmts) != 1 || res.Stmts[0].Kind != KindRenameColumn {
		t.Fatalf("同位置同 DATA_TYPE 须配对为改名: %+v", res.Stmts)
	}
	if !strings.Contains(res.Stmts[0].SQL, "CHANGE COLUMN `memo1` `memo2` varchar(32)") {
		t.Fatalf("CHANGE 语句须采纳源定义（含新宽度）: %s", res.Stmts[0].SQL)
	}
}

// 目标表缺失 → CREATE TABLE 白名单（结构含列序/主键/注释）。
func TestDiffDefs_CreateTable(t *testing.T) {
	src := &tableDef{Cols: []ColDef{
		{Name: "b", Ordinal: 2, ColumnType: "varchar", RawType: "varchar(8)"},
		{Name: "a", Ordinal: 1, ColumnType: "int", RawType: "int", AutoInc: true},
	}, PK: []string{"a"}, Comment: "注"}
	res := diffDefs(src, &tableDef{}, "dstdb", "t_new")
	if !res.CreateTable || !res.Whitelisted || len(res.Stmts) != 1 || res.Stmts[0].Kind != KindCreateTable {
		t.Fatalf("CREATE TABLE 场景判定错误: %+v", res)
	}
	// 列序保序（a 在 b 前——旧实现按列名排序近似，违反源列序）
	sqlText := res.Stmts[0].SQL
	if strings.Index(sqlText, "`a` int") > strings.Index(sqlText, "`b` varchar") {
		t.Fatalf("CREATE 语句须保持源列序: %s", sqlText)
	}
	if !strings.Contains(sqlText, "PRIMARY KEY (`a`)") || !strings.Contains(sqlText, "AUTO_INCREMENT") {
		t.Fatalf("CREATE 语句须含主键与自增: %s", sqlText)
	}
}

// TestColDefSQL_DefaultForms 列默认值生成（2026-11-16 review 二轮）：
// datetime/timestamp 字面量默认值必须加引号（information_schema 返回裸字面量，
// 原样拼接生成非法 SQL）；CURRENT_TIMESTAMP 函数形态保持不加引号。
func TestColDefSQL_DefaultForms(t *testing.T) {
	cases := []struct {
		name string
		c    ColDef
		want string
	}{
		{"datetime 字面量默认", ColDef{Name: "created", ColumnType: "datetime", RawType: "datetime", Default: nd("2020-01-01 00:00:00")}, "DEFAULT '2020-01-01 00:00:00'"},
		{"datetime 函数默认", ColDef{Name: "created", ColumnType: "datetime", RawType: "datetime", Default: nd("CURRENT_TIMESTAMP")}, "DEFAULT CURRENT_TIMESTAMP"},
		{"timestamp fsp 函数默认", ColDef{Name: "ts", ColumnType: "timestamp", RawType: "timestamp(6)", Default: nd("CURRENT_TIMESTAMP(6)")}, "DEFAULT CURRENT_TIMESTAMP(6)"},
		{"int 默认加引号", ColDef{Name: "n", ColumnType: "int", RawType: "int", Default: nd("0")}, "DEFAULT '0'"},
		{"varchar 默认加引号", ColDef{Name: "s", ColumnType: "varchar", RawType: "varchar(8)", Default: nd("x")}, "DEFAULT 'x'"},
	}
	for _, tc := range cases {
		got := colDefSQL(tc.c)
		if !strings.Contains(got, tc.want) {
			t.Fatalf("%s: colDefSQL=%q 缺 %q", tc.name, got, tc.want)
		}
	}
}

// 唯一索引对账（2026-11-10 重启拒启陷阱修复）：源端 ADD UNIQUE INDEX 必须生成
// 补建语句（非白名单）；目标多余唯一索引生成 DROP INDEX。
func TestDiffDefs_UniqueIndex(t *testing.T) {
	base := func() *tableDef {
		return &tableDef{Cols: []ColDef{{Name: "id", Ordinal: 1, ColumnType: "bigint", RawType: "bigint"}}, PK: []string{"id"}}
	}
	src := base()
	src.Unique = []model.UniqueIndex{{Name: "uq_code", Parts: []model.UniqueIndexPart{{Column: "code"}}}}
	tgt := base()

	res := diffDefs(src, tgt, "dstdb", "t1")
	if res.Whitelisted {
		t.Fatal("索引差异应判非白名单（挂起审批）")
	}
	found := false
	for _, s := range res.Stmts {
		if s.Kind == KindAddUniqueIndex && strings.Contains(s.SQL, "ADD UNIQUE INDEX `uq_code` (`code`)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("未生成补建唯一索引语句: %+v", res.Stmts)
	}

	// 反向：目标多余索引 → DROP INDEX
	res2 := diffDefs(base(), src, "dstdb", "t1")
	dropped := false
	for _, s := range res2.Stmts {
		if s.Kind == KindDropIndex && strings.Contains(s.SQL, "DROP INDEX `uq_code`") {
			dropped = true
		}
	}
	if !dropped {
		t.Fatalf("未生成删多余索引语句: %+v", res2.Stmts)
	}

	// 两端一致 → 无语句
	res3 := diffDefs(src, src, "dstdb", "t1")
	if len(res3.Stmts) != 0 {
		t.Fatalf("索引一致不应有语句: %+v", res3.Stmts)
	}
}
