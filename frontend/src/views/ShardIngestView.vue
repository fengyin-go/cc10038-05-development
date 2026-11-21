<template>
  <section class="page shard-page" data-module="shard_ingest">
    <header class="page-head">
      <div>
        <h2>分片批量交付</h2>
        <p class="page-desc">
          多选分片一次提交，片号关联原始行号区间与原始行；同文摘重送幂等复用，异文摘冲突留痕，解析只认清单授权行。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/shard_delivery">查看分片交付清单</RouterLink>
      </div>
    </header>

    <form class="analysis-form" @submit.prevent="loadBatch">
      <label>
        <span>采集批次编号</span>
        <input v-model="batchNo" data-testid="batch-no" placeholder="如 INGE-SHARD-01" required />
      </label>
      <button class="btn primary" data-testid="load-batch" type="submit">加载整批视图</button>
      <label>
        <span>数据源（首次自动建批次时使用）</span>
        <input v-model="sourceId" placeholder="app-logs" />
      </label>
      <label>
        <span>采集时间窗口</span>
        <input v-model="timeWindow" placeholder="2026-11-21" />
      </label>
    </form>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">已到片号</span>
        <strong class="stat-value">{{ view ? view.received_shards.join('、') || '—' : '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待补片号</span>
        <strong class="stat-value" :class="{ 'error-text': view && view.missing_shards.length }">
          {{ view ? view.missing_shards.join('、') || '无缺口' : '—' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待补行区间</span>
        <strong class="stat-value">
          {{ view && view.missing_ranges.length ? formatRanges(view.missing_ranges) : '无' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">内容冲突片</span>
        <strong class="stat-value" :class="{ 'error-text': view && view.conflicts.length }">
          {{ view ? view.conflicts.length : 0 }}
        </strong>
      </article>
    </div>

    <!-- 多选分片批量交付表单 -->
    <div class="shard-editor card-block">
      <div class="block-head">
        <h3>分片交付（可一次提交多片，允许乱序到达）</h3>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="add-shard" @click="addShardRow">增加一片</button>
          <button class="btn primary" type="button" data-testid="submit-shards" :disabled="submitting" @click="submitShards">
            {{ submitting ? '提交中…' : `一次提交选中的 ${selectedCount} 片` }}
          </button>
        </div>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th style="width: 42px">选择</th>
            <th style="width: 110px">片号</th>
            <th style="width: 150px">起始原始行号（可空=接末尾）</th>
            <th>原始正文（每行一条）</th>
            <th style="width: 60px">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, idx) in shardRows" :key="idx">
            <td><input v-model="row.selected" type="checkbox" :data-testid="`pick-${idx}`" /></td>
            <td><input v-model="row.shard_no" :data-testid="`shard-no-${idx}`" placeholder="如 3" /></td>
            <td><input v-model="row.start_line" :data-testid="`start-line-${idx}`" placeholder="自动" /></td>
            <td>
              <textarea
                v-model="row.linesText"
                :data-testid="`lines-${idx}`"
                rows="3"
                class="shard-lines"
                placeholder="2026-11-21T09:00:00|INFO|service boot&#10;每行一条原始日志"
              ></textarea>
            </td>
            <td><button class="link" type="button" @click="shardRows.splice(idx, 1)">移除</button></td>
          </tr>
          <tr v-if="!shardRows.length">
            <td colspan="5" class="empty-state">还没有分片，点「增加一片」开始批量交付</td>
          </tr>
        </tbody>
      </table>
      <p v-if="message" class="page-foot" :class="{ 'error-text': !lastOk }">{{ message }}</p>
    </div>

    <template v-if="view">
      <!-- 整批行状态网格：待补行区间在整批记录里看得出来 -->
      <div class="card-block">
        <div class="block-head"><h3>整批原始行状态（{{ view.max_line_no }} 行）</h3></div>
        <div class="line-grid">
          <span
            v-for="cell in view.line_grid"
            :key="cell.line_no"
            class="line-cell"
            :class="`cell-${cell.status}`"
            :title="cellTitle(cell)"
          >
            <i>{{ cell.line_no }}</i>
            <em>{{ cell.shard_no ? `片${cell.shard_no}` : '缺' }}</em>
          </span>
        </div>
        <p class="legend">
          <span class="dot cell-delivered"></span>已交付可读
          <span class="dot cell-missing"></span>待补缺口
          <span class="dot cell-conflicted"></span>内容冲突待裁决
        </p>
      </div>

      <!-- 分片交付清单 -->
      <div class="card-block">
        <div class="block-head"><h3>分片交付清单</h3></div>
        <table class="data-table">
          <thead>
            <tr>
              <th>片号</th><th>原始行号区间</th><th>行数</th><th>当前授权版本</th>
              <th>正文摘要</th><th>接入次数</th><th>清单状态</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="m in view.manifests" :key="String(m.manifest_id)" :data-status="String(m.status)">
              <td>片{{ m['片号'] }}</td>
              <td>{{ m['起始行号'] }} - {{ m['截止行号'] }}</td>
              <td>{{ m['行计数'] }}</td>
              <td>{{ m['当前版本'] }}</td>
              <td class="mono">{{ m['当前正文摘要'] }}</td>
              <td>{{ m['接入次数'] }}</td>
              <td :class="{ 'error-text': m.status === '内容冲突' }">{{ m.status }}</td>
              <td class="row-actions">
                <button
                  v-if="m.status === '内容冲突'"
                  class="link"
                  type="button"
                  :data-testid="`diff-${m['片号']}`"
                  @click="openConflict(String(m['片号']))"
                >查看差异/裁决</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 授权解析与行号对照 -->
      <div class="card-block">
        <div class="block-head">
          <h3>授权解析与行号对照</h3>
          <div class="page-actions">
            <button class="btn primary" type="button" data-testid="parse-authorized" @click="runParse">
              按清单授权解析（补交续做）
            </button>
            <button class="btn" type="button" data-testid="refresh-map" @click="loadLineMap">刷新行号对照</button>
          </div>
        </div>
        <p v-if="parseSummary" class="page-foot">{{ parseSummary }}</p>
        <table class="data-table">
          <thead>
            <tr>
              <th>原始行号</th><th>片号</th><th>行版本</th><th>授权可读</th>
              <th>解析状态</th><th>成品行号</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in lineMap" :key="row.raw_id" :data-status="row.解析状态">
              <td>{{ row['原始行号'] }}</td>
              <td>片{{ row['片号'] }}</td>
              <td>{{ row['行版本'] }}</td>
              <td>{{ row['授权可读'] ? '是' : '否（未裁决版本）' }}</td>
              <td :class="{ 'error-text': row.解析状态 === '解析失败' }">{{ row.解析状态 }}</td>
              <td class="mono">{{ row['成品行号'] || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <!-- 冲突差异与裁决弹窗 -->
    <div v-if="conflict" class="modal-backdrop" data-testid="conflict-modal">
      <div class="modal">
        <h3>片{{ conflict['片号'] }} 两次接入差异（当前授权 {{ conflict['当前授权版本'] }}）</h3>
        <table class="data-table">
          <thead><tr><th>原始行号</th><th>旧版正文</th><th>新版正文</th></tr></thead>
          <tbody>
            <tr v-for="d in conflict.line_diff || []" :key="d.line_no">
              <td>{{ d.line_no }}</td>
              <td class="mono">{{ d.old_body ?? '—' }}</td>
              <td class="mono">{{ d.new_body ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="block-head" style="margin-top: 12px">
          <span>裁决授权版本：</span>
          <div class="page-actions">
            <button
              v-for="v in conflict.versions"
              :key="v.version"
              class="btn"
              type="button"
              :data-testid="`adopt-${v.version}`"
              @click="adopt(v.version)"
            >
              采用 {{ v.version }}
            </button>
          </div>
        </div>
        <p v-if="message" class="page-foot" :class="{ 'error-text': !lastOk }">{{ message }}</p>
        <div class="page-actions" style="margin-top: 12px">
          <button class="btn ghost" type="button" @click="conflict = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import {
  deliverShards,
  fetchBatchView,
  fetchConflictDiff,
  fetchLineMap,
  parseAuthorized,
  resolveConflict,
} from '@/api/shard-api'
import type { BatchView, ConflictDetail, LineMapRow } from '@/api/shard-api'

type ShardFormRow = {
  selected: boolean
  shard_no: string
  start_line: string
  linesText: string
}

const batchNo = ref('INGE-SHARD-01')
const sourceId = ref('app-logs')
const timeWindow = ref('2026-11-21')
const view = ref<BatchView | null>(null)
const lineMap = ref<LineMapRow[]>([])
const conflict = ref<ConflictDetail | null>(null)
const shardRows = ref<ShardFormRow[]>([
  { selected: true, shard_no: '', start_line: '', linesText: '' },
])
const submitting = ref(false)
const message = ref('')
const lastOk = ref(true)
const parseSummary = ref('')

const selectedCount = computed(
  () => shardRows.value.filter((row) => row.selected && row.shard_no.trim() && row.linesText.trim()).length,
)

function addShardRow() {
  shardRows.value.push({ selected: true, shard_no: '', start_line: '', linesText: '' })
}

function formatRanges(ranges: [number, number][]) {
  return ranges.map(([lo, hi]) => (lo === hi ? `${lo}` : `${lo}-${hi}`)).join('、')
}

function cellTitle(cell: BatchView['line_grid'][number]) {
  if (cell.status === 'missing') return `原始行 ${cell.line_no}：待补缺口`
  return `原始行 ${cell.line_no}：片${cell.shard_no} ${cell.version}（${cell.status === 'conflicted' ? '冲突' : '已交付'}）`
}

async function submitShards() {
  message.value = ''
  const picked = shardRows.value.filter(
    (row) => row.selected && row.shard_no.trim() && row.linesText.trim(),
  )
  if (!picked.length) {
    lastOk.value = false
    message.value = '请至少勾选并填写一个分片'
    return
  }
  submitting.value = true
  try {
    const shards = picked.map((row) => ({
      shard_no: row.shard_no.trim(),
      start_line: row.start_line.trim() ? Number(row.start_line) : null,
      lines: row.linesText.split('\n').map((line) => line).filter((line) => line.length > 0),
    }))
    const result = await deliverShards(batchNo.value.trim(), shards, sourceId.value, timeWindow.value)
    lastOk.value = result.ok
    if (result.ok) {
      const results = result.receipts.map((r) => `片${r.shard_no}=${r.result}(行${r.start_line}-${r.end_line})`)
      message.value = `${result.message}；${results.join('，')}`
      await refreshAll()
    } else {
      message.value = result.message
    }
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '分片交付失败'
  } finally {
    submitting.value = false
  }
}

async function loadBatch() {
  message.value = ''
  try {
    view.value = await fetchBatchView(batchNo.value.trim())
    if (!view.value.ok) {
      lastOk.value = false
      message.value = view.value.message ?? '整批视图读取失败'
      return
    }
    lastOk.value = true
    await loadLineMap()
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '整批视图读取失败'
  }
}

async function loadLineMap() {
  const payload = await fetchLineMap(batchNo.value.trim())
  lineMap.value = payload.mapping
}

async function refreshAll() {
  await loadBatch()
}

async function runParse() {
  try {
    const result = await parseAuthorized(batchNo.value.trim())
    lastOk.value = result.ok
    parseSummary.value =
      `新解析 ${result.parsed_count} 行，失败 ${result.failure_count} 行，已解析跳过 ${result.skipped_already_parsed} 行；` +
      `成品行号：${result.line_mapping.map((m) => `${m['原始行号']}→${m['成品行号']}`).join('，') || '无新增'}`
    message.value = result.message
    await loadLineMap()
    await loadBatch()
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '授权解析失败'
  }
}

async function openConflict(shardNo: string) {
  const payload = await fetchConflictDiff(batchNo.value.trim(), shardNo)
  if (payload.ok) conflict.value = payload.conflict
}

async function adopt(version: string) {
  if (!conflict.value) return
  try {
    const result = await resolveConflict(conflict.value.manifest_id, version)
    lastOk.value = result.ok
    message.value = result.message
    if (result.ok) {
      conflict.value = null
      await refreshAll()
    }
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '冲突裁决失败'
  }
}
</script>

<style scoped>
.card-block {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
  margin-bottom: 14px;
}
.block-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
  gap: 10px;
  flex-wrap: wrap;
}
.block-head h3 {
  margin: 0;
  font-size: 15px;
}
.shard-lines {
  width: 100%;
  box-sizing: border-box;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px;
  font-family: monospace;
  font-size: 12px;
}
.line-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.line-cell {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 52px;
  border-radius: 6px;
  padding: 4px 0;
  border: 1px solid var(--border);
  font-style: normal;
}
.line-cell i {
  font-size: 12px;
  font-weight: 600;
}
.line-cell em {
  font-size: 11px;
  font-style: normal;
  color: var(--muted);
}
.cell-delivered {
  background: #dcfce7;
  border-color: #86efac;
}
.cell-missing {
  background: #f3f4f6;
  border-style: dashed;
  color: #6b7280;
}
.cell-conflicted {
  background: #fee2e2;
  border-color: #fca5a5;
}
.legend {
  margin: 10px 0 0;
  font-size: 12px;
  color: var(--muted);
  display: flex;
  gap: 14px;
  align-items: center;
}
.dot {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 4px;
}
.mono {
  font-family: monospace;
  font-size: 12px;
}
</style>
