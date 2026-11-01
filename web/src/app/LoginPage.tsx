import { useState } from 'react'
import { Button, Form, Input } from 'antd'
import { ApiError } from '@/shared/api/client'
import { ApiClient } from '@/shared/api/client'
import { BrandMark } from '@/shared/components/BrandMark'
import { Callout } from '@/shared/components/Callout'
import { useAuth } from './auth'

export function LoginPage() {
  const { login, invalidate } = useAuth()
  const [form] = Form.useForm<{ username: string; password: string }>()
  const [checking, setChecking] = useState(false)
  const [error, setError] = useState('')

  async function submit(values: { username: string; password: string }) {
    setChecking(true)
    setError('')
    const candidate = new ApiClient(values, invalidate)
    try {
      await candidate.getStatus()
      login(values)
    } catch (cause) {
      setError(cause instanceof ApiError && cause.status === 401 ? '用户名或密码不正确，请重试。' : cause instanceof Error ? cause.message : '连接服务失败。')
    } finally {
      setChecking(false)
    }
  }

  async function continueWithoutCredentials() {
    setChecking(true)
    setError('')
    const candidate = new ApiClient(null, invalidate)
    try {
      await candidate.getStatus()
      login(null)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法连接服务。若服务已启用 HTTP Basic，请输入账号密码。')
    } finally {
      setChecking(false)
    }
  }

  return (
    <main className="login-screen">
      <section className="login-visual">
        <div className="login-brand">
          <BrandMark />
          <span className="brand-copy"><strong>GOCDC</strong><small>MYSQL CDC 控制台</small></span>
        </div>
        <div className="login-statement">
          <span className="panel-kicker">MYSQL CDC 数据同步</span>
          <h1>让每一次数据变更<br />都有迹可循。</h1>
          <p>RAFT 一致性 · BINLOG 流式订阅 · 多下游分发</p>
        </div>
        <div className="login-flow" aria-hidden="true">
          <div className="login-flow-node">
            <span>来源</span>
            <b>MySQL</b>
            <small>binlog 读取</small>
          </div>
          <div className="login-flow-line"><i /></div>
          <div className="login-flow-node is-core">
            <span>管线</span>
            <b>GOCDC</b>
            <small>实时分发</small>
          </div>
          <div className="login-flow-line"><i /></div>
          <div className="login-flow-node">
            <span>下游</span>
            <b>MySQL · Kafka · MQ</b>
            <small>扇出写入</small>
          </div>
        </div>
        <div className="login-visual-foot">
          <span>RAFT 选举</span><span>BINLOG 订阅</span><span>ETCD 存储</span><span>多下游</span>
        </div>
      </section>

      <section className="login-form-side">
        <div className="login-panel">
          <div className="login-panel-head">
            <span className="panel-kicker">安全接入</span>
            <h2>欢迎回来</h2>
            <p>使用 Web 管理凭据连接到 gocdc 服务。</p>
          </div>
          {error && <Callout tone="danger" className="login-error">{error}</Callout>}
          <Form form={form} layout="vertical" onFinish={submit} requiredMark={false} className="login-form">
            <Form.Item name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }]}>
              <Input autoComplete="username" size="large" placeholder="admin" />
            </Form.Item>
            <Form.Item name="password" label="密码" rules={[{ required: true, message: '请输入密码' }]}>
              <Input.Password autoComplete="current-password" size="large" placeholder="输入 Web 密码" />
            </Form.Item>
            <Button type="primary" htmlType="submit" size="large" block loading={checking}>安全连接</Button>
          </Form>
          <div className="login-actions">
            <Button type="link" block onClick={continueWithoutCredentials} loading={checking}>此服务已显式关闭鉴权？无凭据继续</Button>
          </div>
          <p className="login-footnote">凭据将保存在本机 Cookie 中（7 天后自动过期），「断开连接」即清除。</p>
        </div>
        <div className="login-footer">gocdc · MySQL CDC 控制台</div>
      </section>
    </main>
  )
}
