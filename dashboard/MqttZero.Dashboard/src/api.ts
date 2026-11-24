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

export interface PageDto<T> {
  items: T[]
  page: number
  pageSize: number
  total: number
}

export interface PublishRequest {
  topic: string
  payload: string
  qualityOfServiceLevel: 0 | 1 | 2
  retain: boolean
}

export async function getOverview(): Promise<OverviewDto> {
  return request<OverviewDto>('overview')
}

export async function getClientPage(page: number, pageSize?: number): Promise<PageDto<ClientDto>> {
  const query = new URLSearchParams({ page: page.toString() })
  if (pageSize !== undefined) {
    query.set('pageSize', pageSize.toString())
  }
  return request<PageDto<ClientDto>>(`clients?${query}`)
}

export async function publishMessage(message: PublishRequest): Promise<void> {
  const tokenResponse = await fetch(getApiPath('antiforgery'), {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' }
  })

  if (!tokenResponse.ok) {
    throw new Error(`无法获取安全令牌：${tokenResponse.status}`)
  }

  const token = (await tokenResponse.json()) as { token: string }
  const response = await fetch(getApiPath('publish'), {
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
  const response = await fetch(getApiPath(path), {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' }
  })

  if (!response.ok) {
    throw new Error(`请求失败：${response.status}`)
  }

  return response.json() as Promise<T>
}

export function getApiPath(path: string): string {
  return `api/v1/${path}`
}
