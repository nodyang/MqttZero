<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { getClients, getOverview, type ClientDto, type OverviewDto } from './api'

const activePage = ref('overview')
const overview = ref<OverviewDto>({ state: 'Created', connectedClients: 0 })
const clients = ref<ClientDto[]>([])
const error = ref('')
let timer: number | undefined

const title = computed(() => {
  switch (activePage.value) {
    case 'clients':
      return '客户端'
    case 'publish':
      return '发布消息'
    case 'events':
      return '实时事件'
    default:
      return '运行概览'
  }
})

async function refresh() {
  try {
    error.value = ''
    overview.value = await getOverview()
    clients.value = await getClients()
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : '加载失败'
  }
}

onMounted(async () => {
  await refresh()
  timer = window.setInterval(refresh, 3000)
})

onUnmounted(() => {
  if (timer !== undefined) {
    window.clearInterval(timer)
  }
})
</script>

<template>
  <el-container class="shell">
    <el-aside width="220px" class="sidebar">
      <div class="brand">MQTTZero</div>
      <el-menu :default-active="activePage" @select="activePage = $event">
        <el-menu-item index="overview">运行概览</el-menu-item>
        <el-menu-item index="clients">客户端</el-menu-item>
        <el-menu-item index="publish">发布消息</el-menu-item>
        <el-menu-item index="events">实时事件</el-menu-item>
      </el-menu>
    </el-aside>

    <el-container>
      <el-header class="header">
        <strong>{{ title }}</strong>
        <span class="state">Broker：{{ overview.state }}</span>
      </el-header>

      <el-main>
        <el-alert v-if="error" :title="error" type="error" show-icon class="alert" />

        <section v-if="activePage === 'overview'" class="cards">
          <el-card>
            <div class="metric-label">在线客户端</div>
            <div class="metric-value">{{ overview.connectedClients }}</div>
          </el-card>
          <el-card>
            <div class="metric-label">运行状态</div>
            <div class="metric-value small">{{ overview.state }}</div>
          </el-card>
          <el-card>
            <div class="metric-label">零分配目标</div>
            <div class="metric-value small">核心热路径</div>
          </el-card>
        </section>

        <el-card v-if="activePage === 'clients'">
          <el-table :data="clients" empty-text="当前没有在线客户端">
            <el-table-column prop="clientId" label="Client ID" min-width="180" />
            <el-table-column prop="remoteEndPoint" label="远端地址" min-width="180" />
            <el-table-column label="TLS" width="90">
              <template #default="{ row }">{{ row.isSecure ? '是' : '否' }}</template>
            </el-table-column>
            <el-table-column label="WebSocket" width="110">
              <template #default="{ row }">{{ row.isWebSocket ? '是' : '否' }}</template>
            </el-table-column>
            <el-table-column prop="subscriptionCount" label="订阅数" width="100" />
          </el-table>
        </el-card>

        <el-card v-if="activePage === 'publish'">
          <el-empty description="Publish API 已预留，下一步接入操作权限和 Antiforgery。" />
        </el-card>

        <el-card v-if="activePage === 'events'">
          <el-empty description="SignalR 实时事件服务已预留。" />
        </el-card>
      </el-main>
    </el-container>
  </el-container>
</template>
