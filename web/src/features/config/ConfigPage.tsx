import { useState, type ReactNode } from 'react'
import { App as AntApp, Button } from 'antd'
import { CopyOutlined, SafetyCertificateOutlined } from '@ant-design/icons'
import { useConfig } from '@/shared/api/queries'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip } from '@/shared/components/presentation'

/**
 * 生效配置页（2026-11-12 收敛）：只读展示集群全量配置文档（含全部 sources[]，
 * 敏感字段服务端打码）。规则在线编辑收敛到「复制规则」页（编辑规则抽屉，唯一
 * 入口）——本页原「规则视图（按库）」页签与该页完全重合，已移除（用户决策）。
 */
export function ConfigPage() {
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
      description="集群全量配置文档（含全部 sources[]，敏感字段服务端打码）。规则（同步白名单）的在线增删改在「复制规则」页完成（校验+活体预检+CAS 保存，应用重启后生效）；其余配置须在外部配置中心完成并重启服务。"
    />
    <Callout tone="info" title="查看范围">
      本页仅查看（敏感字段由服务端打码，请勿复制到公开渠道）。规则编辑入口：「复制规则」页 → 「编辑规则」抽屉，保存后需应用重启生效。
    </Callout>
    <section className="panel config-panel">
      <div className="config-toolbar">
        <span className="panel-kicker">CONFIG DOCUMENT · /api/v1/config</span>
        <div className="config-toolbar-actions">
          <Button icon={<CopyOutlined />} onClick={() => void copyConfig()} disabled={!content}>{copied ? '已复制' : '复制 JSON'}</Button>
        </div>
      </div>
      {config.error
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
