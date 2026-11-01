import { useStatus } from '@/shared/api/queries'
import { useAuth } from './auth'
import { LoginPage } from './LoginPage'
import { LoadingState } from '@/shared/components/PageState'
import { ApiError } from '@/shared/api/client'
import { Button, Result } from 'antd'
import { useEffect, type ReactNode } from 'react'

export function AuthGate({ children }: { children: ReactNode }) {
  const { session, invalidate } = useAuth()
  if (!session) return <LoginPage />
  return <SessionGate key={`${session.credentials?.username ?? 'anonymous'}`} onInvalid={invalidate}>{children}</SessionGate>
}

function SessionGate({ children, onInvalid }: { children: ReactNode; onInvalid: () => void }) {
  const { error, isPending, refetch } = useStatus()
  useEffect(() => {
    if (error instanceof ApiError && error.status === 401) onInvalid()
  }, [error, onInvalid])
  if (error instanceof ApiError && error.status === 401) return null
  if (isPending) return <div className="gate-loading"><LoadingState rows={2} /></div>
  if (error) {
    return (
      <div className="gate-error">
        <Result status="error" title="无法连接 gocdc" subTitle={error instanceof Error ? error.message : '请确认服务正在运行。'} extra={<Button type="primary" onClick={() => void refetch()}>重试连接</Button>} />
      </div>
    )
  }
  return <>{children}</>
}
