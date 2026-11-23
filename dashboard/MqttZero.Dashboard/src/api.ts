export interface OverviewDto {
  state: string
  connectedClients: number
}

export interface ClientDto {
  clientId: string
  remoteEndPoint: string | null
  isSecure: boolean
  isWebSocket: boolean
  subscriptionCount: number
}

export interface PublishRequest {
  topic: string
  payload: string
  qualityOfServiceLevel: 0 | 1
  retain: boolean
}

export async function getOverview(): Promise<OverviewDto> {
  return request<OverviewDto>('overview')
}

export async function getClients(): Promise<ClientDto[]> {
  return request<ClientDto[]>('clients')
}

export async function publishMessage(message: PublishRequest): Promise<void> {
  const tokenResponse = await fetch('/mqttzero/api/v1/antiforgery', {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' }
  })

  if (!tokenResponse.ok) {
    throw new Error(`无法获取安全令牌：${tokenResponse.status}`)
  }

  const token = (await tokenResponse.json()) as { token: string }
  const response = await fetch('/mqttzero/api/v1/publish', {
    method: 'POST',
    credentials: 'same-origin',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      RequestVerificationToken: token.token
    },
    body: JSON.stringify(message)
  })

  if (!response.ok) {
    throw new Error(`发布失败：${response.status}`)
  }
}

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`/mqttzero/api/v1/${path}`, {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' }
  })

  if (!response.ok) {
    throw new Error(`请求失败：${response.status}`)
  }

  return response.json() as Promise<T>
}
