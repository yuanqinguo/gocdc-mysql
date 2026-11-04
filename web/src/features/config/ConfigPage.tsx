import { useState, type ReactNode } from 'react'
import { App as AntApp, Button, Segmented } from 'antd'
import { CodeOutlined, CopyOutlined, DatabaseOutlined, SafetyCertificateOutlined } from '@ant-design/icons'
import { useConfig, useDDLEventsAll, useEffectiveRules, useSources, useStatus } from '@/shared/api/queries'
import type { DDLRecord } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip } from '@/shared/components/presentation'
import { RuleExplorer } from '@/features/rules/RuleExplorer'
import { RulesManage } from './RulesManage'

type ConfigTab = 'config' | 'rules'

export function ConfigPage() {
  const [tab, setTab] = useState<ConfigTab>('config')
  const { message } = AntApp.useApp()
  const config = useConfig()
  const [copied, setCopied] = useState(false)
  const content = config.data === undefined ? '' : JSON.stringify(config.data, null, 2)

  async function copyConfig() {
    try {
      await navigator.clipboard.writeText(content)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1600)
    } catch {
      setCopied(false)
      void message.warning('浏览器未允许访问剪贴板，请在下方配置内容中手动选择复制。')
    }
  }

  return <>
    <PageHeader
      code="05"
      eyebrow="系统 / 运行配置 · 集群级"
      title="生效配置"
      description="集群全量配置文档（含全部 sources[]）。「规则视图（按库）」页签按源提供源库/表的在线增删改（校验+活体预检+CAS 保存，应用重启后生效）；其余配置须在外部配置中心完成并重启服务。"
    />
    <Callout tone="info" title="查看与编辑范围">
      「生效配置」页签仅查看（敏感字段由服务端打码，请勿复制到公开渠道）；「规则视图（按库）」页签可直接增删改各源的同步白名单，保存前服务端自动校验与活体预检，保存后需应用重启生效。
    </Callout>
    <section className="panel config-panel">
      <div className="config-toolbar">
        <Segmented
          value={tab}
          onChange={(value) => setTab(value as ConfigTab)}
          options={[
            { label: '生效配置', value: 'config', icon: <SafetyCertificateOutlined /> },
            { label: '规则视图（按库）', value: 'rules', icon: <CodeOutlined /> },
          ]}
        />
        <div className="config-toolbar-actions">
          {tab === 'config' && <Button icon={<CopyOutlined />} onClick={() => void copyConfig()} disabled={!content}>{copied ? '已复制' : '复制 JSON'}</Button>}
        </div>
      </div>
      {tab === 'config' && (
        config.error
          ? <div className="config-state"><ErrorState error={config.error} onRetry={() => void config.refetch()} /></div>
          : config.isPending
            ? <div className="config-state"><LoadingState rows={10} /></div>
            : (
              <div className="code-shell">
                <div className="code-toolbar">
                  <span className="code-file"><span className="code-prompt">›_</span> effective-config.json <Chip tone="muted">已打码</Chip></span>
                  <span>{content ? `${content.split('\n').length} 行` : '暂无数据'}</span>
                </div>
                <JsonCode content={content} />
              </div>
            )
      )}
      {tab === 'rules' && <ConfigRulesTab />}
    </section>
    <div className="note-line">
      <SafetyCertificateOutlined />
      <span><b>服务端脱敏</b> · 配置页面展示由 Go API 返回的数据。登录凭据仅保存在本机 Cookie 中，不写入服务端。</span>
    </div>
  </>
}

const JSON_TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g

/** 规则视图（按库）：配置是全量 v2——按 sources[] 分节，每源一个在线编辑视图
 * （RulesManage，sid 作用域）；本地文件配置形态下回退只读生效视图。 */
function ConfigRulesTab() {
  const status = useStatus()
  const sources = useSources()
  const ddlAll = useDDLEventsAll()
  const ids = status.data?.sources.map((source) => source.id) ?? []
  const ddlRecords = ddlAll.data ?? []
  // 该源 targets 已启用的下游：新增源库时 sinks 开关的默认值与提示
  const sinksBySid = new Map((sources.data ?? []).map((source) => [source.id, source.sinks]))
  if (status.error) return <div className="config-state"><ErrorState error={status.error} onRetry={() => void status.refetch()} /></div>
  if (status.isPending) return <div className="config-state"><LoadingState /></div>
  return (
    <div className="config-rules">
      {ids.map((sid) => (
        <section className="config-source-group" key={sid}>
          <header className="config-source-head">
            <span className="entity"><span className="entity-icon"><DatabaseOutlined /></span><span><b>{sid}</b></span></span>
            <Chip tone="muted">在线编辑 · 保存后需应用重启生效</Chip>
          </header>
          <RulesManage
            sid={sid}
            pendingDDL={ddlRecords.filter((record) => record.source === sid).map((record) => ({ schema: record.schema, table: record.table }))}
            enabledSinks={sinksBySid.get(sid) ?? []}
            readOnlyView={<SourceEffectiveRules sid={sid} ddlRecords={ddlRecords} />}
          />
        </section>
      ))}
    </div>
  )
}

/** file 配置形态下的只读回退：该源编译后生效规则（原「规则视图（按库）」形态）。 */
function SourceEffectiveRules({ sid, ddlRecords }: { sid: string; ddlRecords: DDLRecord[] }) {
  const rules = useEffectiveRules(sid)
  if (rules.isPending) return <LoadingState />
  if (rules.error) return <ErrorState error={rules.error} onRetry={() => void rules.refetch()} />
  return (
    <RuleExplorer
      rules={rules.data ?? []}
      pendingDDL={ddlRecords.filter((record) => record.source === sid).map((record) => ({ schema: record.schema, table: record.table }))}
      schemaSearchPlaceholder={`在源 ${sid} 中搜索源库…`}
    />
  )
}

function JsonCode({ content }: { content: string }) {
  if (!content) return <div className="code-empty">暂无配置内容</div>
  const lines = content.split('\n')
  return (
    <div className="code-view" role="region" aria-label="生效配置 JSON">
      {lines.map((line, index) => (
        <div className="code-line" key={index}>
          <span className="code-gutter" aria-hidden="true">{index + 1}</span>
          <code className="code-source">{highlightJson(line)}</code>
        </div>
      ))}
    </div>
  )
}

function highlightJson(line: string): ReactNode {
  if (!line) return '\u00a0'
  const parts: ReactNode[] = []
  let last = 0
  let match: RegExpExecArray | null
  JSON_TOKEN.lastIndex = 0
  while ((match = JSON_TOKEN.exec(line)) !== null) {
    if (match.index > last) parts.push(line.slice(last, match.index))
    if (match[1]) {
      const isKey = Boolean(match[2])
      parts.push(<span key={`${match.index}-s`} className={isKey ? 'tok-key' : 'tok-string'}>{match[1]}</span>)
      if (match[2]) parts.push(<span key={`${match.index}-c`} className="tok-punct">{match[2]}</span>)
    } else if (match[3]) {
      parts.push(<span key={`${match.index}-b`} className="tok-bool">{match[3]}</span>)
    } else if (match[4]) {
      parts.push(<span key={`${match.index}-n`} className="tok-number">{match[4]}</span>)
    }
    last = JSON_TOKEN.lastIndex
  }
  if (last < line.length) parts.push(line.slice(last))
  return parts
}
