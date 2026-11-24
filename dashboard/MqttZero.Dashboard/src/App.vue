<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import {
  getClientPage,
  getOverview,
  publishMessage,
  type ClientDto,
  type OverviewDto,
  type PublishRequest
} from './api'
import { formatBrokerState } from './format'

const activePage = ref('overview')
const overview = ref<OverviewDto>({
  state: 'Created',
  activeConnections: 0,
  connectedClients: 0
})
const clients = ref<ClientDto[]>([])
const clientPage = ref(1)
const clientPageSize = ref(50)
const clientTotal = ref(0)
const error = ref('')
const publishing = ref(false)
const publishForm = ref<PublishRequest>({
  topic: 'devices/demo',
  payload: '{"value":1}',
  qualityOfServiceLevel: 0,
  retain: false
})
let timer: number | undefined

const title = computed(() => {
  switch (activePage.value) {
    case 'clients':
      return '客户端'
    case 'publish':
      return '发布消息'
    default:
      return '运行概览'
  }
})

async function refresh() {
  try {
    error.value = ''
    overview.value = await getOverview()
    const page = await getClientPage(clientPage.value, clientPageSize.value)
    clients.value = page.items
    clientTotal.value = page.total
    clientPage.value = page.page
    clientPageSize.value = page.pageSize
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : '加载失败'
  }
}

async function changeClientPage(page: number, pageSize: number) {
  clientPage.value = page
  clientPageSize.value = pageSize
  await refresh()
}

async function publish() {
  try {
    publishing.value = true
    await publishMessage(publishForm.value)
    ElMessage.success('消息已发送')
  } catch (reason) {
    ElMessage.error(reason instanceof Error ? reason.message : '发布失败')
  } finally {
    publishing.value = false
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
      </el-menu>
    </el-aside>

    <el-container>
      <el-header class="header">
        <strong>{{ title }}</strong>
        <span class="state">Broker：{{ formatBrokerState(overview.state) }}</span>
      </el-header>

      <el-main>
        <el-alert v-if="error" :title="error" type="error" show-icon class="alert" />

        <section v-if="activePage === 'overview'" class="cards">
          <el-card>
            <div class="metric-label">在线客户端</div>
            <div class="metric-value">{{ overview.connectedClients }}</div>
          </el-card>
          <el-card>
            <div class="metric-label">活跃连接</div>
            <div class="metric-value">{{ overview.activeConnections }}</div>
          </el-card>
          <el-card>
            <div class="metric-label">运行状态</div>
            <div class="metric-value small">{{ formatBrokerState(overview.state) }}</div>
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
          <el-pagination
            :current-page="clientPage"
            :page-size="clientPageSize"
            :total="clientTotal"
            :page-sizes="[20, 50, 100, 200]"
            layout="total, sizes, prev, pager, next"
            @current-change="(page: number) => changeClientPage(page, clientPageSize)"
            @size-change="(size: number) => changeClientPage(1, size)"
          />
        </el-card>

        <el-card v-if="activePage === 'publish'">
          <el-form :model="publishForm" label-width="100px" class="publish-form">
            <el-form-item label="Topic">
              <el-input v-model="publishForm.topic" />
            </el-form-item>
            <el-form-item label="QoS">
              <el-radio-group v-model="publishForm.qualityOfServiceLevel">
                <el-radio :value="0">QoS 0</el-radio>
                <el-radio :value="1">QoS 1</el-radio>
                <el-radio :value="2">QoS 2</el-radio>
              </el-radio-group>
            </el-form-item>
            <el-form-item label="保留消息">
              <el-switch v-model="publishForm.retain" />
            </el-form-item>
            <el-form-item label="Payload">
              <el-input
                v-model="publishForm.payload"
                type="textarea"
                :rows="8"
                placeholder="UTF-8 文本"
              />
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="publishing" @click="publish">发布</el-button>
            </el-form-item>
          </el-form>
        </el-card>

      </el-main>
    </el-container>
  </el-container>
</template>
