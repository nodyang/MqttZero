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

export async function getOverview(): Promise<OverviewDto> {
  return request<OverviewDto>('overview')
}

export async function getClients(): Promise<ClientDto[]> {
  return request<ClientDto[]>('clients')
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
