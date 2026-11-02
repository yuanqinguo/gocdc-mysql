import { Select } from 'antd'
import { useSourceScope } from './sourceScope'
import { useStatus } from '@/shared/api/queries'
import { classifySource, type SourceCardSeverity } from '@/features/dashboard/sourceCards'
import { Chip } from '@/shared/components/presentation'

const ALL = '__all__'

const SEVERITY_TONE: Record<SourceCardSeverity, 'ok' | 'warn' | 'danger'> = {
  failed: 'danger',
  paused: 'danger',
  ddlWaiting: 'warn',
  catchingUp: 'warn',
  ok: 'ok',
}

/**
 * 源作用域选择器（多源部署常驻，聚合/单源视图通用）：下拉一步互切——
 * 选项带异常状态点（沿用总控台源卡口径）；「全部数据源」即聚合视图。
 * 不做搜索过滤：源数量量级是十来个，平铺可见即可。
 * 单源部署不渲染。
 */
export function SourceScopePicker() {
  const { currentSid, setSid, sourceCount } = useSourceScope()
  const { data: status } = useStatus()
  if (sourceCount < 2) return null
  return (
    <div className="source-picker">
      <span className="source-picker-label">数据源</span>
      <Select
        size="small"
        variant="borderless"
        aria-label="切换数据源"
        className="source-picker-select"
        popupMatchSelectWidth={false}
        value={currentSid ?? ALL}
        onChange={(value) => setSid(value === ALL ? null : value)}
        options={[
          { value: ALL, label: <Chip tone="muted">全部数据源</Chip> },
          ...(status?.sources ?? []).map((source) => ({
            value: source.id,
            label: <Chip tone={SEVERITY_TONE[classifySource(source)]} dot>{source.id}</Chip>,
          })),
        ]}
      />
    </div>
  )
}
