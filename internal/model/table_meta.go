package model

import "time"

// 规范化列类型名（Column.Type 的封闭词汇表）。
// 由 schema 包从 information_schema / go-mysql schema 归一化产出，
// transform（04 §7）与消息构建（05 §2.1）按此分支。
const (
	TypeTinyint   = "tinyint"
	TypeSmallint  = "smallint"
	TypeMediumint = "mediumint"
	TypeInt       = "int"
	TypeBigint    = "bigint"

	TypeDecimal = "decimal" // decimal | numeric
	TypeFloat   = "float"
	TypeDouble  = "double" // double | real

	TypeBit  = "bit"
	TypeEnum = "enum"
	TypeSet  = "set"

	TypeDate      = "date"
	TypeDatetime  = "datetime"
	TypeTimestamp = "timestamp"
	TypeTime      = "time"
	TypeYear      = "year"

	TypeChar      = "char"
	TypeVarchar   = "varchar"
	TypeBinary    = "binary"
	TypeVarbinary = "varbinary"
	TypeJSON      = "json"

	TypeTinytext   = "tinytext"
	TypeText       = "text"
	TypeMediumtext = "mediumtext"
	TypeLongtext   = "longtext"
	TypeTinyblob   = "tinyblob"
	TypeBlob       = "blob"
	TypeMediumblob = "mediumblob"
	TypeLongblob   = "longblob"
)

// Column 表列元数据（有序；TableMeta.Columns 的元素）。
//
// 字段为设计文档的权威集合外加实施必需的 Unsigned/Precision/Scale/SetValues
// （列名清单为「至少包含」语义，扩展已在 docs/PROGRESS.md 登记）。
type Column struct {
	Name    string
	Type    string // 规范化类型名（上方常量）
	RawType string // 原始类型文本，如 "decimal(10,2)"、"int unsigned"

	Unsigned   bool     // 整型无符号（04 §7：unsigned → uint64）
	Precision  int      // decimal/bit/varchar 等的精度（bit 为位数）
	Scale      int      // decimal 标度
	IsPK       bool     // 参与主键
	EnumValues []string // enum 取值（有序，索引 1..n）
	SetValues  []string // set 取值（有序，位掩码 1<<i）
	Charset    string   // 字符列字符集（utf8mb4 等）
	Comment    string   // 列注释（DDL 白名单同步用，08 §3）
}

// UniqueIndexPart 唯一索引的一列。PrefixLength=0 表示完整列索引。
type UniqueIndexPart struct {
	Column       string
	PrefixLength int
	Expression   bool // 函数索引暂不支持等价性校验，启动时 fail-fast
}

// UniqueIndex 源/目标唯一索引元数据。索引名不参与语义比较，Parts 顺序参与。
type UniqueIndex struct {
	Name  string
	Parts []UniqueIndexPart
}

// TableMeta 表结构快照（SchemaManager 缓存，不可变、整体替换）。
// transform 在转换时拿到的是版本化快照，DDL 刷新后为新实例（08 文档）。
type TableMeta struct {
	Version             uint64
	Schema              string
	Table               string
	Columns             []Column      // 有序，与 binlog RowsEvent 列序一致
	PKColumns           []string      // 主键列名，有序；无主键表为空（由规则 order_by_column 兜底）
	UniqueIndexes       []UniqueIndex // 主键 + 全部唯一索引，列序与 information_schema 一致
	UniqueIndexesLoaded bool          // 区分「已查询且无索引」与旧/手工构造的元数据
	Comment             string
	LoadedAt            time.Time
}

// ColumnByName 返回列名字对应的列与下标（大小写敏感，与 binlog 列名一致）。
func (m *TableMeta) ColumnByName(name string) (*Column, int) {
	for i := range m.Columns {
		if m.Columns[i].Name == name {
			return &m.Columns[i], i
		}
	}
	return nil, -1
}
