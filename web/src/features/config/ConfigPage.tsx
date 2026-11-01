import { useState, type ReactNode } from 'react'
import { App as AntApp, Button, Segmented } from 'antd'
import { CodeOutlined, CopyOutlined, SafetyCertificateOutlined } from '@ant-design/icons'
import { useConfig, useDDLEvents, useEffectiveRules } from '@/shared/api/queries'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip } from '@/shared/components/presentation'
import { RuleExplorer } from '@/features/rules/RuleExplorer'

type ConfigTab = 'config' | 'rules'

export function ConfigPage() {
  const [tab, setTab] = useState<ConfigTab>('config')
  const { message } = AntApp.useApp()
  const config = useConfig()
  const rules = useEffectiveRules()
  const ddlEvents = useDDLEvents()
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
      eyebrow="系统 / 运行配置"
      title="生效配置"
      description="查看当前生效配置与编译后的规则视图（组形态白名单）。修改须在外部配置中心完成并重启服务。"
    />
    <Callout tone="info" title="只读视图">
      配置仅可查看，不在管理台编辑。敏感字段由服务端打码；请勿将页面内容复制到公开渠道。
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
      {tab === 'rules' && (
        rules.error
          ? <div className="config-state"><ErrorState error={rules.error} onRetry={() => void rules.refetch()} /></div>
          : rules.isPending
            ? <div className="config-state"><LoadingState /></div>
            : <div className="config-rules"><RuleExplorer rules={rules.data ?? []} pendingDDL={ddlEvents.data ?? []} schemaSearchPlaceholder="搜索源库…" /></div>
      )}
    </section>
    <div className="note-line">
      <SafetyCertificateOutlined />
      <span><b>服务端脱敏</b> · 配置页面展示由 Go API 返回的数据。登录凭据仅保存在本机 Cookie 中，不写入服务端。</span>
    </div>
  </>
}

const JSON_TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g

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
