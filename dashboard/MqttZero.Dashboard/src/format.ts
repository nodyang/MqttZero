export function formatBrokerState(state: string): string {
  switch (state) {
    case 'Running':
      return '运行中'
    case 'Starting':
      return '启动中'
    case 'Stopping':
      return '停止中'
    case 'Stopped':
      return '已停止'
    default:
      return '未启动'
  }
}
