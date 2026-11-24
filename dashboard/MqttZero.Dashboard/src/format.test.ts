import { describe, expect, it } from 'vitest'
import { formatBrokerState } from './format'
import { getApiPath } from './api'

describe('formatBrokerState', () => {
  it('maps running state to Chinese text', () => {
    expect(formatBrokerState('Running')).toBe('运行中')
  })

  it('maps unknown state to not started', () => {
    expect(formatBrokerState('Created')).toBe('未启动')
  })
})

describe('getApiPath', () => {
  it('builds a relative API path for custom dashboard prefixes', () => {
    expect(getApiPath('overview')).toBe('api/v1/overview')
  })
})
