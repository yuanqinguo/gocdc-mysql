import { DatabaseOutlined } from '@ant-design/icons'
import { Segmented } from 'antd'
import { useSourceScope } from './sourceScope'
import { useStatus } from '@/shared/api/queries'

const ALL = '__all__'

/**
 * 源上下文切换条（多源部署常驻）：「全部源 | 各源 id」一键切换，替代藏在顶栏的
 * 下拉框——进入单源视图后回聚合、切其他源都是一步可见操作。单源部署不渲染。
 */
export function SourceSwitcher({ block = false }: { block?: boolean }) {
  const { currentSid, setSid, sourceCount } = useSourceScope()
  const { data: status } = useStatus()
  const sources = status?.sources ?? []
  if (sourceCount < 2) return null
  return (
    <div className={block ? 'source-scope is-block' : 'source-scope'}>
      <span className="source-scope-label"><DatabaseOutlined /> 数据源</span>
      <Segmented
        aria-label="切换数据源"
        value={currentSid ?? ALL}
        onChange={(value) => setSid(value === ALL ? null : String(value))}
        options={[{ value: ALL, label: '全部源' }, ...sources.map((source) => ({ value: source.id, label: source.id }))]}
      />
    </div>
  )
}
