/** 分片接入专用接口：批量交付、整批视图、授权解析、行号映射与冲突裁决。 */
import { request } from './client'

export type ShardInput = {
  shard_no: string
  lines: string[]
  start_line?: number | null
}

export type DeliverReceipt = {
  shard_no: string
  result: string
  digest: string
  start_line: number
  end_line: number
  raw_ids: number[]
  reused?: boolean
  conflict?: boolean
  new_version?: string
  authorized_version?: string
  manifest_id: number
}

export type LineCell = {
  line_no: number
  status: 'delivered' | 'missing' | 'conflicted'
  shard_no?: string
  version?: string
  digest?: string
}

export type ConflictDetail = {
  manifest_id: number
  批次编号: string
  片号: string
  当前授权版本: string
  versions: {
    version: string
    digest: string
    preview: string
    start_line: number
    end_line: number
    raw_ids: number[]
    received_at: string
  }[]
  line_diff?: {
    line_no: number
    old_version: string
    old_body: string | null
    new_version: string
    new_body: string | null
    changed: boolean
  }[]
}

export type BatchView = {
  ok: boolean
  message?: string
  batch_no: string
  received_shards: string[]
  missing_shards: string[]
  missing_ranges: [number, number][]
  max_line_no: number
  line_grid: LineCell[]
  manifests: Record<string, string | number | boolean | number[]>[]
  conflicts: ConflictDetail[]
}

export type LineMapRow = {
  raw_id: number
  原始行号: number
  片号: string
  行版本: string
  授权可读: boolean
  解析状态: string
  成品行号: string
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

export function deliverShards(
  batchNo: string,
  shards: ShardInput[],
  sourceId = '',
  timeWindow = '',
) {
  return postJson<{ ok: boolean; message: string; receipts: DeliverReceipt[] }>(
    '/api/shard-ingest/deliver',
    { batch_no: batchNo, source_id: sourceId || null, time_window: timeWindow, shards },
  )
}

export function fetchBatchView(batchNo: string) {
  return getJson<BatchView>(`/api/shard-ingest/batch/${encodeURIComponent(batchNo)}/view`)
}

export function fetchAuthorizedLines(batchNo: string) {
  return getJson<{ ok: boolean; count: number; lines: Record<string, string | number>[] }>(
    `/api/shard-ingest/batch/${encodeURIComponent(batchNo)}/authorized-lines`,
  )
}

export function parseAuthorized(batchNo: string) {
  return postJson<{
    ok: boolean
    message: string
    parsed_count: number
    failure_count: number
    skipped_already_parsed: number
    line_mapping: { 原始行号: number; 成品行号: string; 片号: string; 行版本: string }[]
  }>('/api/shard-ingest/parse-authorized', { batch_no: batchNo })
}

export function fetchLineMap(batchNo: string) {
  return getJson<{ ok: boolean; mapping: LineMapRow[] }>(
    `/api/shard-ingest/batch/${encodeURIComponent(batchNo)}/line-map`,
  )
}

export function fetchConflictDiff(batchNo: string, shardNo: string) {
  return getJson<{ ok: boolean; message?: string; conflict: ConflictDetail }>(
    `/api/shard-ingest/batch/${encodeURIComponent(batchNo)}/shards/${encodeURIComponent(shardNo)}/conflict-diff`,
  )
}

export function resolveConflict(manifestId: number, adoptVersion: string) {
  return postJson<{ ok: boolean; message: string }>('/api/shard-ingest/conflicts/resolve', {
    manifest_id: manifestId,
    adopt_version: adoptVersion,
  })
}
