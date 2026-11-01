package verify

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql" // 驱动注册：sql.Open 惰性句柄（不建连）
)

// TestQuoteIdent 反引号转义（表/列名来自命令行与请求体——防误拼/防注入底线）。
func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("orders"); got != "`orders`" {
		t.Fatalf("普通标识符: %s", got)
	}
	if got := quoteIdent("we`ird"); got != "`we``ird`" {
		t.Fatalf("内嵌反引号应翻倍: %s", got)
	}
}

// TestCompareTable_ParamValidation 必填校验（nil DB / 缺 schema/table/pk）。
// sql.Open 是惰性的——句柄有效但不建连，校验在触库前失败。
func TestCompareTable_ParamValidation(t *testing.T) {
	h, _ := sql.Open("mysql", "root@tcp(127.0.0.1:1)/x")
	if _, err := CompareTable(context.Background(), &Options{}); err == nil ||
		!strings.Contains(err.Error(), "SrcDB") {
		t.Fatalf("nil DB 应报错: %v", err)
	}
	if _, err := CompareTable(context.Background(),
		&Options{SrcDB: h, TgtDB: h}); err == nil ||
		!strings.Contains(err.Error(), "Schema/Table/PK") {
		t.Fatalf("缺参数应报错: %v", err)
	}
}
