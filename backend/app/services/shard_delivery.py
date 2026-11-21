"""分片交付领域：采集入口的分片批量接收、交付清单与缺口/冲突管理。

核心约束（对应题面）：
- 一次请求可携带多个分片，分片允许乱序到达（第三片先到、第二片还空着）；
- 每个片号关联整批连续的原始行号区间与原始正文行，汇总成分片交付清单；
- 字段解析只认清单当前授权版本覆盖的行区间，未补齐的缺口行不可读；
- 已解析分段结果回写 raw_records 待处理清单，补交后只处理新增的待解析行；
- 同片号同文摘重送走幂等复用，保留已得到的行引用与成品，不盖历史；
- 同片号不同文摘记为内容冲突，两版正文与逐行差异都可追溯，等待人工裁决。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 清单状态：片到达后默认已交付；同片号撞不同正文升级为内容冲突。
MANIFEST_STATUSES = ["待补齐", "已交付", "内容冲突"]
# 接入结果同时作为 shard_ingest_log 的 status。
INGEST_RESULTS = ["首次接收", "幂等复用", "内容冲突"]


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def digest_of(lines: list[str]) -> str:
    """对片内整段正文计算稳定摘要，用于判断重送是否同文。"""
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()[:16]


def line_digest(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]


class ShardDeliveryService(BaseModuleService):
    """分片交付清单的通用资源服务：列表/筛选/导出复用基类骨架。"""

    MODULE = "shard_delivery"
    ENTITY = "分片交付清单"
    OPERATION_TERM = "交付"
    LIST_FIELDS = [
        "批次编号", "片号", "起始行号", "截止行号", "行计数",
        "接入次数", "当前版本", "当前正文摘要", "最近接入时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "片号", "起始行号", "截止行号"]
    STATUS_ORDER = MANIFEST_STATUSES
    ACTION_RULES = {"保留首版": "已交付", "标记缺口": "待补齐"}
    NEGATIVE_ACTIONS = []


class ShardIngestService:
    """分片接入编排：批量交付、缺口视图、授权解析与冲突裁决。"""

    # ---------- 基础查找 ----------
    def _find_manifest(self, batch_no: str, shard_no: str) -> dict[str, Any] | None:
        for row in store.rows("shard_delivery"):
            if row.get("批次编号") == batch_no and str(row.get("片号")) == str(shard_no):
                return row
        return None

    def _ensure_batch(self, batch_no: str, source_id: str | None, time_window: str) -> dict[str, Any]:
        """批次不存在时自动登记一条采集中批次，作为分片归属。"""
        for batch in store.rows("ingest_batch"):
            if batch.get("批次编号") == batch_no:
                return batch
        entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id or "shard-manual-source",
            "采集时间窗口": time_window or _now_iso(),
            "记录总数": "0",
            "批次状态": time_window or "分片采集中",
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "started_at": _now_iso(),
            "shard_mode": True,
        }
        store.rows("ingest_batch").append(entry)
        return entry

    # ---------- 阶段1：分片批量交付 ----------
    def deliver(
        self,
        batch_no: str,
        shards: list[dict[str, Any]],
        *,
        source_id: str | None = None,
        time_window: str = "",
    ) -> dict[str, Any]:
        if not batch_no or not str(batch_no).strip():
            return {"ok": False, "message": "批次编号不能为空"}
        if not shards:
            return {"ok": False, "message": "至少需要提交一个分片"}

        # 规范化入参：片号转字符串、正文行统一为字符串列表。
        normalized: list[dict[str, Any]] = []
        for shard in shards:
            shard_no = str(shard.get("shard_no", "")).strip()
            if not shard_no:
                return {"ok": False, "message": "存在缺少片号的分片，整单拒收"}
            raw_lines = shard.get("lines") or []
            lines = [str(line) for line in raw_lines]
            if not lines:
                return {"ok": False, "message": f"片 {shard_no} 正文为空，整单拒收"}
            start_line = shard.get("start_line")
            normalized.append({
                "shard_no": shard_no,
                "start_line": int(start_line) if start_line not in (None, "") else None,
                "lines": lines,
            })

        # 同一提交内片号不得重复。
        seen_shards: set[str] = set()
        for item in normalized:
            if item["shard_no"] in seen_shards:
                return {"ok": False, "message": f"同一提交里片号 {item['shard_no']} 重复，整单拒收"}
            seen_shards.add(item["shard_no"])

        # 推算每个片的行号区间：显式 start_line 优先，否则接整批当前最大行号之后。
        manifests = store.rows("shard_delivery")
        batch_max_end = max(
            (int(m.get("截止行号", 0)) for m in manifests if m.get("批次编号") == batch_no),
            default=0,
        )
        planned: list[dict[str, Any]] = []
        cursor = batch_max_end
        for item in normalized:
            start = item["start_line"] if item["start_line"] is not None else cursor + 1
            end = start + len(item["lines"]) - 1
            item["start_line"] = start
            item["end_line"] = end
            cursor = max(cursor, end)
            planned.append(item)

        # 区间重叠校验：提交内互查；与库内其他片号的清单互查。
        # 同片号重送不视为重叠——它走幂等复用/内容冲突的版本处理分支。
        incoming_shard_nos = {item["shard_no"] for item in planned}
        existing_ranges = [
            (int(m["起始行号"]), int(m["截止行号"]), str(m["片号"]))
            for m in manifests
            if m.get("批次编号") == batch_no and str(m["片号"]) not in incoming_shard_nos
        ]
        overlap_errors: list[str] = []
        for item in planned:
            for ex_start, ex_end, ex_shard in existing_ranges:
                if item["start_line"] <= ex_end and ex_start <= item["end_line"]:
                    overlap_errors.append(
                        f"片 {item['shard_no']} 行 {item['start_line']}-{item['end_line']} "
                        f"与已接收片 {ex_shard} 行 {ex_start}-{ex_end} 重叠"
                    )
            for other in planned:
                if other is item:
                    continue
                if item["start_line"] <= other["end_line"] and other["start_line"] <= item["end_line"]:
                    overlap_errors.append(
                        f"片 {item['shard_no']} 与片 {other['shard_no']} 行区间重叠"
                    )
        if overlap_errors:
            return {"ok": False, "message": "分片行号区间冲突，整单拒收：" + "；".join(overlap_errors),
                    "overlaps": overlap_errors}

        self._ensure_batch(batch_no, source_id, time_window)

        receipts: list[dict[str, Any]] = []
        for item in planned:
            receipts.append(self._ingest_one_shard(batch_no, item, source_id or "shard-manual-source"))

        accepted = [r for r in receipts if r["result"] != "拒收"]
        self._refresh_batch_summary(batch_no)
        return {
            "ok": True,
            "message": f"批次 {batch_no} 接收 {len(accepted)} 个分片",
            "batch_no": batch_no,
            "receipts": receipts,
        }

    def _ingest_one_shard(self, batch_no: str, item: dict[str, Any], source_id: str) -> dict[str, Any]:
        shard_no = item["shard_no"]
        start_line, end_line = item["start_line"], item["end_line"]
        lines = item["lines"]
        digest = digest_of(lines)
        manifest = self._find_manifest(batch_no, shard_no)

        # 情况A：片号首次出现 —— 建清单、落原始行、写接入留痕。
        if manifest is None:
            raw_ids = self._create_raw_rows(batch_no, shard_no, "v1", start_line, lines, None)
            log_id = self._write_ingest_log(
                batch_no, shard_no, 1, digest, start_line, end_line, lines, raw_ids, "首次接收",
            )
            manifest = {
                "id": _next_id("shard_delivery"),
                "批次编号": batch_no,
                "片号": shard_no,
                "起始行号": str(start_line),
                "截止行号": str(end_line),
                "行计数": str(len(lines)),
                "首次接入时间": _now_iso(),
                "最近接入时间": _now_iso(),
                "接入次数": "1",
                "当前版本": "v1",
                "当前正文摘要": digest,
                "正文预览": " | ".join(lines[:3]),
                "status": "已交付",
                "pending": False,
                "abnormal": False,
                "授权原始行ID": json.dumps(raw_ids),
                "版本历史": json.dumps([{
                    "version": "v1", "digest": digest, "ingest_log_id": log_id,
                    "raw_ids": raw_ids, "preview": " | ".join(lines[:3]),
                    "start_line": start_line, "end_line": end_line,
                    "received_at": _now_iso(),
                }], ensure_ascii=False),
            }
            store.rows("shard_delivery").append(manifest)
            return {"shard_no": shard_no, "result": "首次接收", "digest": digest,
                    "start_line": start_line, "end_line": end_line, "raw_ids": raw_ids,
                    "manifest_id": manifest["id"]}

        # 情况B：片号已存在，同文摘 —— 幂等复用，不重建行、不碰解析成品。
        if digest == manifest.get("当前正文摘要") or self._digest_in_history(manifest, digest):
            reused_ids = json.loads(manifest.get("授权原始行ID", "[]"))
            # 同文摘若发生在冲突待裁决期间，复用对应当前版本的行引用。
            version = self._version_of_digest(manifest, digest)
            if version and version != manifest.get("当前版本"):
                reused_ids = self._raw_ids_of_version(manifest, version)
            log_id = self._write_ingest_log(
                batch_no, shard_no, int(manifest.get("接入次数", 0)) + 1,
                digest, start_line, end_line, lines, reused_ids, "幂等复用",
            )
            manifest["接入次数"] = str(int(manifest.get("接入次数", 0)) + 1)
            manifest["最近接入时间"] = _now_iso()
            self._append_log_ref(manifest, version or manifest.get("当前版本"), log_id)
            return {"shard_no": shard_no, "result": "幂等复用", "digest": digest,
                    "start_line": start_line, "end_line": end_line,
                    "raw_ids": reused_ids, "reused": True, "manifest_id": manifest["id"]}

        # 情况C：同片号撞不同正文 —— 留新版本行但不授权，旧版行引用与成品全部保留。
        history = json.loads(manifest.get("版本历史", "[]"))
        new_version = f"v{len(history) + 1}"
        new_raw_ids = self._create_raw_rows(
            batch_no, shard_no, new_version, start_line, lines, None, authorized=False,
        )
        log_id = self._write_ingest_log(
            batch_no, shard_no, int(manifest.get("接入次数", 0)) + 1,
            digest, start_line, end_line, lines, new_raw_ids, "内容冲突",
        )
        history.append({
            "version": new_version, "digest": digest, "ingest_log_id": log_id,
            "raw_ids": new_raw_ids, "preview": " | ".join(lines[:3]),
            "start_line": start_line, "end_line": end_line,
            "received_at": _now_iso(),
        })
        manifest["版本历史"] = json.dumps(history, ensure_ascii=False)
        manifest["接入次数"] = str(int(manifest.get("接入次数", 0)) + 1)
        manifest["最近接入时间"] = _now_iso()
        manifest["status"] = "内容冲突"
        manifest["pending"] = True
        manifest["abnormal"] = True
        # 当前授权版本刻意保持不变，未裁决前字段解析仍读旧版正文。
        return {"shard_no": shard_no, "result": "内容冲突", "digest": digest,
                "new_version": new_version, "raw_ids": new_raw_ids,
                "authorized_version": manifest.get("当前版本"),
                "manifest_id": manifest["id"], "conflict": True}

    def _create_raw_rows(
        self,
        batch_no: str,
        shard_no: str,
        version: str,
        start_line: int,
        lines: list[str],
        ingest_log_id: int | None,
        *,
        authorized: bool = True,
    ) -> list[int]:
        raw_table = store.rows("raw_records")
        raw_ids: list[int] = []
        now = _now_iso()
        for offset, body in enumerate(lines):
            line_no = start_line + offset
            raw_id = _next_id("raw_records")
            raw_table.append({
                "id": raw_id,
                "批次编号": batch_no,
                "原始行号": str(line_no),
                "采集时间戳": now,
                "原始正文": body,
                "解析状态": "待解析",
                "status": "待解析",
                "pending": True,
                "abnormal": False,
                # —— 分片交付扩展字段 ——
                "片号": str(shard_no),
                "行版本": version,
                "正文摘要": line_digest(body),
                "授权可读": authorized,
                "接入记录ID": str(ingest_log_id) if ingest_log_id else "",
                "成品行号": "",
                "ingested_at": now,
            })
            raw_ids.append(raw_id)
        return raw_ids

    def _write_ingest_log(
        self,
        batch_no: str,
        shard_no: str,
        sequence: int,
        digest: str,
        start_line: int,
        end_line: int,
        lines: list[str],
        raw_ids: list[int],
        result: str,
    ) -> int:
        log_id = _next_id("shard_ingest_log")
        store.rows("shard_ingest_log").append({
            "id": log_id,
            "批次编号": batch_no,
            "片号": str(shard_no),
            "接入序号": str(sequence),
            "正文摘要": digest,
            "接入结果": result,
            "status": "冲突" if result == "内容冲突" else "成功",
            "pending": result == "内容冲突",
            "abnormal": result == "内容冲突",
            "起始行号": str(start_line),
            "截止行号": str(end_line),
            "行计数": str(len(lines)),
            "正文预览": " | ".join(lines[:3]),
            "关联行ID": json.dumps(raw_ids),
            "接入时间": _now_iso(),
        })
        return log_id

    # ---------- 阶段2：整批视图（缺口、乱序、冲突一目了然） ----------
    def batch_view(self, batch_no: str) -> dict[str, Any]:
        manifests = [m for m in store.rows("shard_delivery") if m.get("批次编号") == batch_no]
        if not manifests:
            return {"ok": False, "message": f"批次 {batch_no} 暂无分片交付清单"}

        received_shards = sorted({str(m["片号"]) for m in manifests},
                                 key=lambda x: int(x) if x.isdigit() else x)
        max_shard = max((int(s) for s in received_shards if s.isdigit()), default=0)
        missing_shards = [str(i) for i in range(1, max_shard + 1) if str(i) not in received_shards]

        # 已交付区间（只统计当前授权版本覆盖范围）。
        delivered_ranges = sorted(
            (int(m["起始行号"]), int(m["截止行号"])) for m in manifests
        )
        max_end = max((end for _, end in delivered_ranges), default=0)
        missing_ranges = self._complement_ranges(delivered_ranges, 1, max_end)

        # 行状态网格：整批 1..max_end 每个原始行号都能查到状态。
        line_grid: list[dict[str, Any]] = []
        for line_no in range(1, max_end + 1):
            cell: dict[str, Any] = {"line_no": line_no, "status": "missing"}
            for m in manifests:
                if int(m["起始行号"]) <= line_no <= int(m["截止行号"]):
                    cell.update({
                        "status": "conflicted" if m["status"] == "内容冲突" else "delivered",
                        "shard_no": str(m["片号"]),
                        "version": m.get("当前版本"),
                        "digest": m.get("当前正文摘要"),
                    })
                    break
            line_grid.append(cell)

        conflicts = [self._conflict_detail(m) for m in manifests if m["status"] == "内容冲突"]
        return {
            "ok": True,
            "batch_no": batch_no,
            "received_shards": received_shards,
            "missing_shards": missing_shards,
            "missing_ranges": missing_ranges,
            "max_line_no": max_end,
            "line_grid": line_grid,
            "manifests": [self._manifest_brief(m) for m in manifests],
            "conflicts": conflicts,
        }

    def authorized_lines(self, batch_no: str, shard_nos: list[str] | None = None) -> dict[str, Any]:
        """返回清单当前授权、实际获准解析的原始行（缺口行与未裁决新版本不在其中）。"""
        shard_set = {str(s) for s in shard_nos} if shard_nos else None
        rows = []
        for raw in store.rows("raw_records"):
            if raw.get("批次编号") != batch_no:
                continue
            if not raw.get("授权可读", False):
                continue
            if shard_set is not None and str(raw.get("片号")) not in shard_set:
                continue
            rows.append({
                "raw_id": raw["id"],
                "原始行号": int(raw["原始行号"]),
                "片号": str(raw.get("片号")),
                "行版本": raw.get("行版本"),
                "正文摘要": raw.get("正文摘要"),
                "原始正文": raw.get("原始正文"),
                "status": raw.get("status"),
                "成品行号": raw.get("成品行号", ""),
            })
        rows.sort(key=lambda r: r["原始行号"])
        return {"ok": True, "batch_no": batch_no, "count": len(rows), "lines": rows}

    # ---------- 阶段3：按清单授权解析，回写待处理清单 ----------
    def parse_authorized(self, batch_no: str, parser_rule_name: str | None = None) -> dict[str, Any]:
        view = self.batch_view(batch_no)
        if not view.get("ok"):
            return view

        rule = self._pick_parser_rule(parser_rule_name)
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        raw_table = store.rows("raw_records")
        # 只解析：授权可读、且仍处于待解析的行；已解析行直接跳过，支持补交续做。
        pending = [
            raw for raw in raw_table
            if raw.get("批次编号") == batch_no
            and raw.get("授权可读", False)
            and raw.get("status") == "待解析"
        ]
        pending.sort(key=lambda r: int(r["原始行号"]))
        already_parsed = self._count_parsed(batch_no)

        # 成品行号在批次内追加分配、永不重排，保证历史行引用不被覆盖。
        next_product_seq = self._next_product_seq(batch_no)
        parsed_entities = store.rows("parsed_entity")
        journal = store.rows("parse_journal")
        line_mapping: list[dict[str, Any]] = []
        success = 0
        failure = 0

        for raw in pending:
            try:
                fields = self._parse_body(raw.get("原始正文", ""), rule)
                product_no = f"P{next_product_seq:04d}"
                next_product_seq += 1
                entity = {
                    "id": _next_id("parsed_entity"),
                    "来源批次": batch_no,
                    "原始记录ID": str(raw["id"]),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "片号": str(raw.get("片号")),
                    "原始行号": str(raw.get("原始行号")),
                    "成品行号": product_no,
                    "行版本": raw.get("行版本"),
                    "正文摘要": raw.get("正文摘要"),
                    "parsed_at": _now_iso(),
                }
                parsed_entities.append(entity)
                # 回写采集待处理清单：状态与成品行号都落到原始行上。
                raw["status"] = "已解析"
                raw["解析状态"] = "已解析"
                raw["pending"] = False
                raw["成品行号"] = product_no
                line_mapping.append({
                    "raw_id": raw["id"],
                    "原始行号": int(raw["原始行号"]),
                    "成品行号": product_no,
                    "片号": str(raw.get("片号")),
                    "行版本": raw.get("行版本"),
                })
                success += 1
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure += 1

        journal.append({
            "id": _next_id("parse_journal"),
            "操作类型": "分片授权解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success),
            "执行结果": "成功" if failure == 0 else "部分成功",
            "status": "成功" if failure == 0 else "部分成功",
            "pending": failure > 0,
            "abnormal": failure > 0,
            "success_count": str(success),
            "failure_count": str(failure),
        })
        self._refresh_batch_summary(batch_no)
        return {
            "ok": True,
            "message": f"批次 {batch_no} 授权解析完成",
            "parsed_count": success,
            "failure_count": failure,
            "line_mapping": line_mapping,
            "skipped_already_parsed": already_parsed,
        }

    def line_number_map(self, batch_no: str) -> dict[str, Any]:
        """收齐前后对照：实际获准解析的原始行号与成品行号全量映射。"""
        mapping = []
        for raw in store.rows("raw_records"):
            if raw.get("批次编号") != batch_no:
                continue
            mapping.append({
                "raw_id": raw["id"],
                "原始行号": int(raw["原始行号"]),
                "片号": str(raw.get("片号", "")),
                "行版本": raw.get("行版本", ""),
                "授权可读": bool(raw.get("授权可读", False)),
                "解析状态": raw.get("status"),
                "成品行号": raw.get("成品行号", ""),
            })
        mapping.sort(key=lambda x: (x["原始行号"], x["行版本"]))
        return {"ok": True, "batch_no": batch_no, "mapping": mapping}

    # ---------- 阶段4：冲突裁决与差异追溯 ----------
    def conflict_diff(self, batch_no: str, shard_no: str) -> dict[str, Any]:
        manifest = self._find_manifest(batch_no, shard_no)
        if manifest is None:
            return {"ok": False, "message": f"批次 {batch_no} 片 {shard_no} 清单不存在"}
        if manifest.get("status") != "内容冲突":
            return {"ok": False, "message": "该片不存在内容冲突"}
        return {"ok": True, "conflict": self._conflict_detail(manifest, include_diff=True)}

    def resolve_conflict(self, manifest_id: int, adopt_version: str) -> dict[str, Any]:
        manifest = store.find("shard_delivery", manifest_id)
        if manifest is None:
            return {"ok": False, "message": f"清单 {manifest_id} 不存在"}
        history = json.loads(manifest.get("版本历史", "[]"))
        versions = {h["version"] for h in history}
        if adopt_version not in versions:
            return {"ok": False, "message": f"版本 {adopt_version} 不在该片版本历史中"}

        for version_info in history:
            authorized = version_info["version"] == adopt_version
            for raw_id in version_info["raw_ids"]:
                raw = store.find("raw_records", raw_id)
                if raw is None:
                    continue
                raw["授权可读"] = authorized
                raw["abnormal"] = not authorized
                # 切到新授权版本时，未解析过的新行重新进入待处理清单。
                if authorized and raw["status"] != "已解析":
                    raw["status"] = "待解析"
                    raw["解析状态"] = "待解析"
                    raw["pending"] = True
        manifest["当前版本"] = adopt_version
        manifest["当前正文摘要"] = next(h["digest"] for h in history if h["version"] == adopt_version)
        manifest["正文预览"] = next(h["preview"] for h in history if h["version"] == adopt_version)
        manifest["授权原始行ID"] = json.dumps(
            next(h["raw_ids"] for h in history if h["version"] == adopt_version))
        manifest["status"] = "已交付"
        manifest["pending"] = False
        manifest["abnormal"] = False
        self._refresh_batch_summary(manifest["批次编号"])
        return {"ok": True, "message": f"片 {manifest['片号']} 已裁决采用 {adopt_version}",
                "manifest": self._manifest_brief(manifest)}

    # ---------- 辅助 ----------
    def _pick_parser_rule(self, parser_rule_name: str | None) -> dict[str, Any] | None:
        parsers = store.rows("field_parser")
        if parser_rule_name:
            for rule in parsers:
                if rule.get("规则名称") == parser_rule_name:
                    return rule
        for rule in parsers:
            if rule.get("status") == "已发布":
                return rule
        return None

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        return {
            fn: (parts[i].strip() if i < len(parts) else "")
            for i, fn in enumerate(field_names) if fn.strip()
        }

    def _next_product_seq(self, batch_no: str) -> int:
        max_seq = 0
        for entity in store.rows("parsed_entity"):
            product_no = str(entity.get("成品行号", ""))
            if entity.get("来源批次") == batch_no and product_no.startswith("P"):
                try:
                    max_seq = max(max_seq, int(product_no[1:]))
                except ValueError:
                    continue
        return max_seq + 1

    def _count_parsed(self, batch_no: str) -> int:
        return sum(
            1 for raw in store.rows("raw_records")
            if raw.get("批次编号") == batch_no and raw.get("status") == "已解析"
        )

    def _refresh_batch_summary(self, batch_no: str) -> None:
        for batch in store.rows("ingest_batch"):
            if batch.get("批次编号") != batch_no:
                continue
            raw_count = sum(
                1 for raw in store.rows("raw_records")
                if raw.get("批次编号") == batch_no and raw.get("授权可读", False)
            )
            batch["记录总数"] = str(raw_count)
            has_conflict = any(
                m.get("status") == "内容冲突"
                for m in store.rows("shard_delivery") if m.get("批次编号") == batch_no
            )
            batch["abnormal"] = has_conflict
            view = self.batch_view(batch_no)
            if view.get("ok") and not view["missing_ranges"] and not has_conflict:
                batch["status"] = "已完成"
                batch["pending"] = False
            else:
                batch["status"] = "采集中"
                batch["pending"] = True

    @staticmethod
    def _complement_ranges(ranges: list[tuple[int, int]], lo: int, hi: int) -> list[list[int]]:
        missing: list[list[int]] = []
        cursor = lo
        for start, end in ranges:
            if start > cursor:
                missing.append([cursor, start - 1])
            cursor = max(cursor, end + 1)
        if cursor <= hi:
            missing.append([cursor, hi])
        return missing

    @staticmethod
    def _digest_in_history(manifest: dict[str, Any], digest: str) -> bool:
        return any(h.get("digest") == digest for h in json.loads(manifest.get("版本历史", "[]")))

    @staticmethod
    def _version_of_digest(manifest: dict[str, Any], digest: str) -> str | None:
        for h in json.loads(manifest.get("版本历史", "[]")):
            if h.get("digest") == digest:
                return h["version"]
        return None

    @staticmethod
    def _raw_ids_of_version(manifest: dict[str, Any], version: str) -> list[int]:
        for h in json.loads(manifest.get("版本历史", "[]")):
            if h.get("version") == version:
                return h["raw_ids"]
        return []

    @staticmethod
    def _append_log_ref(manifest: dict[str, Any], version: str | None, log_id: int) -> None:
        history = json.loads(manifest.get("版本历史", "[]"))
        for h in history:
            if version and h["version"] == version:
                h.setdefault("ingest_log_ids", []).append(log_id)
        manifest["版本历史"] = json.dumps(history, ensure_ascii=False)

    @staticmethod
    def _manifest_brief(m: dict[str, Any]) -> dict[str, Any]:
        return {
            "manifest_id": m["id"],
            "批次编号": m["批次编号"],
            "片号": str(m["片号"]),
            "起始行号": int(m["起始行号"]),
            "截止行号": int(m["截止行号"]),
            "行计数": int(m["行计数"]),
            "接入次数": int(m["接入次数"]),
            "当前版本": m.get("当前版本"),
            "当前正文摘要": m.get("当前正文摘要"),
            "status": m["status"],
            "abnormal": m.get("abnormal", False),
            "最近接入时间": m.get("最近接入时间"),
            "授权原始行ID": json.loads(m.get("授权原始行ID", "[]")),
        }

    def _conflict_detail(self, manifest: dict[str, Any], *, include_diff: bool = True) -> dict[str, Any]:
        history = json.loads(manifest.get("版本历史", "[]"))
        detail = {
            "manifest_id": manifest["id"],
            "批次编号": manifest["批次编号"],
            "片号": str(manifest["片号"]),
            "当前授权版本": manifest.get("当前版本"),
            "versions": [
                {
                    "version": h["version"],
                    "digest": h["digest"],
                    "preview": h["preview"],
                    "start_line": h["start_line"],
                    "end_line": h["end_line"],
                    "raw_ids": h["raw_ids"],
                    "received_at": h["received_at"],
                }
                for h in history
            ],
        }
        if include_diff and len(history) >= 2:
            detail["line_diff"] = self._build_line_diff(manifest, history)
        return detail

    def _build_line_diff(self, manifest: dict[str, Any], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按原始行号对齐两个版本，标出两次接入的正文差异。"""
        def body_map(version_info: dict[str, Any]) -> dict[int, str]:
            result = {}
            for raw_id in version_info["raw_ids"]:
                raw = store.find("raw_records", raw_id)
                if raw is not None:
                    result[int(raw["原始行号"])] = raw.get("原始正文", "")
            return result

        base = body_map(history[0])
        latest = body_map(history[-1])
        diffs: list[dict[str, Any]] = []
        for line_no in sorted(set(base) | set(latest)):
            old_body, new_body = base.get(line_no), latest.get(line_no)
            if old_body != new_body:
                diffs.append({
                    "line_no": line_no,
                    "old_version": history[0]["version"],
                    "old_body": old_body,
                    "new_version": history[-1]["version"],
                    "new_body": new_body,
                    "changed": old_body is not None and new_body is not None,
                })
        return diffs


shard_ingest = ShardIngestService()
