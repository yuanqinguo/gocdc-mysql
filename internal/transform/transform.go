package transform

import (
	"sync"
	"time"

	"gocdc/internal/logging"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
)

// on_convert_error 策略（04 §7）。
const (
	StrategyNull      = "null"       // 置 NULL + 计数（默认）
	StrategyZero      = "zero"       // 按类别零值 + 计数
	StrategyFailEvent = "fail_event" // 整事件失败（不可重试 → PAUSED + 告警）
)

// decimal_as 形态（04 §7）。
const (
	DecimalAsString = "string" // 无损（默认）
	DecimalAsFloat  = "float"  // 兼容旧消费方，有精度风险
)

// zero_datetime_as 形态（04 §7）。
const (
	ZeroDatetimeNull   = "null"   // 置 NULL + 计数（默认）
	ZeroDatetimeString = "string" // 原样字符串透传
)

// Options 转换器选项（来自 config.Pipeline + source.timezone，装配层映射）。
type Options struct {
	Timezone       *time.Location   // source.timezone：DATETIME/TIMESTAMP 解释时区；nil = UTC
	OnConvertError string           // Strategy*；空 = null
	DecimalAs      string           // Decimal*；空 = string
	ZeroDatetimeAs string           // ZeroDatetime*；空 = null
	Metrics        *metrics.Metrics // 可 nil（单测）
}

// Transformer 值转换与事件构建（全量/增量共用同一实例，04 §7 补充规则）。
// 并发安全：Row 可被多 worker 同时调用。
type Transformer struct {
	opts Options

	mu      sync.Mutex
	maskers map[model.MaskSpec]masker // 脱敏规格 → 算法实例缓存
}

// New 构建转换器（选项缺省值在此统一填平）。
func New(opts Options) *Transformer {
	if opts.Timezone == nil {
		opts.Timezone = time.UTC
	}
	if opts.OnConvertError == "" {
		opts.OnConvertError = StrategyNull
	}
	if opts.DecimalAs == "" {
		opts.DecimalAs = DecimalAsString
	}
	if opts.ZeroDatetimeAs == "" {
		opts.ZeroDatetimeAs = ZeroDatetimeNull
	}
	return &Transformer{opts: opts, maskers: map[model.MaskSpec]masker{}}
}

func (t *Transformer) loc() *time.Location { return t.opts.Timezone }

// RowInput 单行事件的转换输入。
//
// Before/After 是原始列值切片，下标与 Meta.Columns 一一对应：
// 增量来自 go-mysql RowsEvent.Rows() 的一行，全量来自文本协议扫描行。
type RowInput struct {
	Meta *model.TableMeta
	Rule *model.Rule // 可 nil：无规则时全列透传、不脱敏

	Before []any // 前像原始值（UPDATE/DELETE；INSERT 为 nil）
	After  []any // 后像原始值（INSERT/UPDATE；DELETE 为 nil）

	Type      model.EventType
	Phase     string // 空 = model.PhaseBinlog
	EventID   string
	Ts        uint64
	XID       uint64
	SourcePos model.Position
}

// Row 转换一行并构建 model.Event（05 §2.1）。
//
// 处理链：值转换 → exclude/mask → Event（10 §2 单点实现，增量全量自动一致）。
// 策略 fail_event 下任一列转换失败即返回错误（*ConvertError，且 IsFailEvent()=true）。
func (t *Transformer) Row(in RowInput) (*model.Event, error) {
	meta := in.Meta
	if meta == nil {
		return nil, &ConvertError{Reason: ReasonUnsupported, Strategy: t.opts.OnConvertError,
			Err: errNilMeta}
	}
	switch in.Type {
	case model.EventInsert:
		if in.After == nil {
			return nil, errImage("INSERT", "after")
		}
	case model.EventDelete:
		if in.Before == nil {
			return nil, errImage("DELETE", "before")
		}
	case model.EventUpdate:
		if in.Before == nil || in.After == nil {
			return nil, errImage("UPDATE", "before+after")
		}
	default:
		return nil, errImage(string(in.Type), "行事件类型")
	}

	// 列名 → 脱敏规格（主键列豁免：红线 10 §4，配置校验拦截在先，此处防御）
	rule := in.Rule
	masks := map[string]model.MaskSpec{}
	if rule != nil {
		for _, c := range meta.Columns {
			if c.IsPK {
				continue
			}
			if spec, ok := rule.MaskColumns[c.Name]; ok {
				masks[c.Name] = spec
			}
		}
	}

	var (
		before, after map[string]any
		err           error
	)
	if in.Before != nil {
		if before, err = t.image(meta, rule, masks, in.Before); err != nil {
			return nil, err
		}
	}
	if in.After != nil {
		if after, err = t.image(meta, rule, masks, in.After); err != nil {
			return nil, err
		}
	}

	// 寻址主键：真实主键优先；无主键表退回规则 order_by_column（05 §5.1 伪主键）
	pkCols := meta.PKColumns
	if len(pkCols) == 0 && rule != nil && rule.OrderByColumn != "" {
		pkCols = []string{rule.OrderByColumn}
	}
	src := after
	if src == nil {
		src = before // DELETE：从前像寻址
	}
	pk, err := t.pkMap(meta, pkCols, src)
	if err != nil {
		return nil, err
	}

	phase := in.Phase
	if phase == "" {
		phase = model.PhaseBinlog
	}
	return &model.Event{
		EventID:       in.EventID,
		Type:          in.Type,
		Phase:         phase,
		Database:      meta.Schema,
		Table:         meta.Table,
		Ts:            in.Ts,
		SourcePos:     in.SourcePos,
		XID:           in.XID,
		Pk:            pk,
		Before:        before,
		After:         after,
		Columns:       t.columnMeta(meta, rule),
		SchemaVersion: meta.Version,
	}, nil
}

// image 转换一幅像（before/after）为列名→已脱敏值 map；
// exclude 列物理移除（10 §2），主键列永不被 exclude（防御性保留）。
func (t *Transformer) image(meta *model.TableMeta, rule *model.Rule,
	masks map[string]model.MaskSpec, raw []any) (map[string]any, error) {
	out := make(map[string]any, len(meta.Columns))
	if raw != nil && len(raw) > len(meta.Columns) {
		// 行像比 schema 新（重放中结构漂移竞态）：多余列忽略 + 计数告警——
		// 列集收敛由 DDL barrier 的 schemaMgr.Refresh 负责，此处不阻塞管线。
		t.countError(meta, "<extra>", t.opts.OnConvertError)
	}
	for i := range meta.Columns {
		col := &meta.Columns[i]
		if raw != nil && i >= len(raw) {
			// binlog 行列数与 schema 不符——两类场景：
			// ① 位点回拨重放窗口：历史行像（DDL 前形态）× 内存中已刷新的新
			//    schema（时间机器问题——DDL 早已 Settled，结构无法回退）；
			// ② 漂移竞态。处置走 on_convert_error 三策略：null/zero 降级单列
			//    并计数告警；fail_event 策略保持上抛（红线=绝不静默——计数
			//    告警必发生）。无条件 fatal 会让回拨重放在第一个已放行的
			//    ADD COLUMN 处死锁、只能 restock 解。
			v, ferr := t.fail(meta, col, category(col), ReasonColumnCount,
				errColumnCount(len(raw), len(meta.Columns)))
			if ferr != nil {
				return nil, ferr
			}
			if isExcluded(rule, col.Name) {
				continue
			}
			out[col.Name] = v
			continue
		}
		if isExcluded(rule, col.Name) {
			continue
		}
		v, err := t.convertValue(meta, col, raw[i])
		if err != nil {
			return nil, err
		}
		if spec, ok := masks[col.Name]; ok {
			m, err := t.maskerFor(spec)
			if err != nil {
				return nil, &ConvertError{
					Schema: meta.Schema, Table: meta.Table, Column: col.Name,
					Category: category(col), Reason: ReasonUnsupported,
					Strategy: t.opts.OnConvertError, Err: err,
				}
			}
			v = m.Apply(v)
		}
		out[col.Name] = v
	}
	return out, nil
}

// pkMap 提取主键列→值（脱敏前的转换值，禁脱敏禁排除，05 §2.1/10 §4）。
func (t *Transformer) pkMap(meta *model.TableMeta, pkCols []string, src map[string]any) (map[string]any, error) {
	if len(pkCols) == 0 {
		return nil, &ConvertError{
			Schema: meta.Schema, Table: meta.Table,
			Reason: ReasonUnsupported, Strategy: t.opts.OnConvertError,
			Err: errNoPK,
		}
	}
	pk := make(map[string]any, len(pkCols))
	for _, name := range pkCols {
		col, _ := meta.ColumnByName(name)
		if col == nil {
			return nil, &ConvertError{
				Schema: meta.Schema, Table: meta.Table, Column: name,
				Reason: ReasonUnsupported, Strategy: t.opts.OnConvertError,
				Err: errPKColumn,
			}
		}
		v, ok := src[name]
		if !ok || v == nil {
			// 主键值缺失/NULL：无法寻址，必须上抛（DELETE 无前像主键即数据异常）
			return nil, &ConvertError{
				Schema: meta.Schema, Table: meta.Table, Column: name,
				Category: category(col), Reason: ReasonUnsupported,
				Strategy: t.opts.OnConvertError, Err: errPKNil,
			}
		}
		pk[name] = v
	}
	return pk, nil
}

// columnMeta 构建列元信息 map（消息 columns 字段；exclude 列不出现，
// mask 列标注算法，decimal 标注标度与 as_string）。
func (t *Transformer) columnMeta(meta *model.TableMeta, rule *model.Rule) map[string]model.ColumnMeta {
	out := make(map[string]model.ColumnMeta, len(meta.Columns))
	for i := range meta.Columns {
		col := &meta.Columns[i]
		if isExcluded(rule, col.Name) {
			continue
		}
		cm := model.ColumnMeta{Type: col.Type, Precision: col.Precision, Scale: col.Scale}
		if col.Type == model.TypeDecimal && t.opts.DecimalAs == DecimalAsString {
			cm.AsString = true
		}
		if rule != nil && !col.IsPK { // 主键永不脱敏（红线）；防御性双保险
			if spec, ok := rule.MaskColumns[col.Name]; ok {
				cm.Masked = spec.Type
			}
		}
		out[col.Name] = cm
	}
	return out
}

// maskerFor 取（带缓存的）脱敏算法实例。MaskSpec 为可比结构，可作 map key。
func (t *Transformer) maskerFor(spec model.MaskSpec) (masker, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if m, ok := t.maskers[spec]; ok {
		return m, nil
	}
	m, err := buildMasker(spec)
	if err != nil {
		return nil, err
	}
	t.maskers[spec] = m
	return m, nil
}

// countError 转换异常计数（11 §1 gocdc_convert_errors_total{db,table,column,action}）。
func (t *Transformer) countError(meta *model.TableMeta, column, action string) {
	if t.opts.Metrics == nil {
		return
	}
	t.opts.Metrics.ConvertErrors.WithLabelValues(meta.Schema, meta.Table, column, action).Inc()
}

func isExcluded(rule *model.Rule, col string) bool {
	if rule == nil {
		return false
	}
	for _, c := range rule.ExcludeColumns {
		if c == col {
			return true
		}
	}
	return false
}

// Redactor 日志脱敏视图（实现 logging.RowRedactor，10 §5）：
// exclude 列整列删除、mask 列替换为脱敏值；输入行必须是已转换值。
//
// rules 为编译后的规则表，key = "db.table" 小写（装配层注入）。
type Redactor struct {
	rules map[string]*model.Rule
	t     *Transformer
}

// 编译期断言：Redactor 满足日志层接口。
var _ logging.RowRedactor = (*Redactor)(nil)

func NewRedactor(rules map[string]*model.Rule) *Redactor {
	return &Redactor{rules: rules, t: New(Options{})}
}

func (r *Redactor) Redact(schema, table string, row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	rule := r.rules[schema+"."+table]
	if rule == nil {
		return row
	}
	out := make(map[string]any, len(row))
	for col, v := range row {
		if isExcluded(rule, col) {
			continue
		}
		if spec, ok := rule.MaskColumns[col]; ok {
			m, err := r.t.maskerFor(spec)
			if err != nil {
				// 算法不可用：宁可整列不打（漏列可察觉），不可漏脱敏（10 §5）
				continue
			}
			v = m.Apply(v)
		}
		out[col] = v
	}
	return out
}
