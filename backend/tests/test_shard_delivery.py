"""分片批量交付验收测试。

覆盖题面场景：
1. 给采集批次送一、三两片，第三片先抵达（乱序、片2缺口、行区间可见）；
2. 值班员按清单授权范围急用第一片正文（缺口行不可读）；
3. 第一片改动一行重送：同片号不同正文留冲突、两次接入差异可查、历史不被覆盖；
4. 同文摘断线重送：复用已有行引用，不重建行、不覆盖成品；
5. 收齐第二片前后，对照实际获准解析的原始行与成品行号；
6. 冲突裁决后新版本行可进入授权解析。
"""
from __future__ import annotations

import json

from app.services.shard_delivery import shard_ingest, digest_of
from app.store import store

BATCH = "INGE-SHARD-01"

SHARD1 = ["2026-11-21T09:00:00|INFO|service boot",
          "2026-11-21T09:01:00|INFO|config loaded"]
SHARD1_CHANGED = ["2026-11-21T09:00:00|INFO|service boot",
                  "2026-11-21T09:01:00|WARN|config reloaded"]
SHARD3 = ["2026-11-21T09:10:00|ERROR|db timeout",
          "2026-11-21T09:11:00|INFO|retry ok",
          "2026-11-21T09:12:00|WARN|queue lag"]
SHARD2 = ["2026-11-21T09:05:00|INFO|heartbeat",
          "2026-11-21T09:06:00|DEBUG|cache warm",
          "2026-11-21T09:07:00|INFO|sync done"]


def _deliver(shards):
    return shard_ingest.deliver(BATCH, shards, source_id="app-logs", time_window="2026-11-21")


def _manifest(shard_no):
    return shard_ingest._find_manifest(BATCH, shard_no)


class TestShardDeliveryBatch:
    """多选分片一次提交、片号关联行号区间与原始行。"""

    def test_multi_shard_one_submission(self):
        result = _deliver([
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
        ])
        assert result["ok"] is True
        receipts = {r["shard_no"]: r for r in result["receipts"]}
        assert set(receipts) == {"1", "3"}
        # 片号关联编号范围
        assert receipts["1"]["start_line"] == 1 and receipts["1"]["end_line"] == 2
        assert receipts["3"]["start_line"] == 6 and receipts["3"]["end_line"] == 8

    def test_manifest_rows_hold_original_lines(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        rows = [r for r in store.rows("raw_records") if r.get("批次编号") == BATCH]
        assert [r["原始正文"] for r in rows] == SHARD1
        assert [r["原始行号"] for r in rows] == ["1", "2"]
        assert all(r["片号"] == "1" and r["行版本"] == "v1" for r in rows)

    def test_empty_or_duplicate_shard_rejected(self):
        assert _deliver([{"shard_no": "1", "lines": []}])["ok"] is False
        dup = shard_ingest.deliver(BATCH, [
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
            {"shard_no": "1", "start_line": 9, "lines": SHARD3},
        ], source_id="s", time_window="t")
        assert dup["ok"] is False

    def test_overlap_ranges_rejected(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        overlap = _deliver([{"shard_no": "3", "start_line": 2, "lines": SHARD3}])
        assert overlap["ok"] is False
        assert "重叠" in overlap["message"]
        assert any("片 3" in detail and "片 1" in detail for detail in overlap["overlaps"])

    def test_auto_cursor_when_start_line_missing(self):
        result = _deliver([
            {"shard_no": "1", "lines": SHARD1},
            {"shard_no": "2", "lines": SHARD2},
        ])
        assert result["ok"] is True
        ranges = {r["shard_no"]: (r["start_line"], r["end_line"]) for r in result["receipts"]}
        assert ranges["1"] == (1, 2)
        assert ranges["2"] == (3, 5)


class TestOutOfOrderAndGaps:
    """第三片先到、第二片空着：缺口必须在整批记录里看得出来。"""

    def test_shard3_arrives_first_gap_visible(self):
        # 先送第三片（片3先抵达）
        first = _deliver([{"shard_no": "3", "start_line": 6, "lines": SHARD3}])
        assert first["ok"] is True
        # 再送第一片
        second = _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        assert second["ok"] is True

        view = shard_ingest.batch_view(BATCH)
        assert view["received_shards"] == ["1", "3"]
        assert view["missing_shards"] == ["2"]
        # 待补行区间 3-5 在整批网格里可见
        assert [3, 5] in view["missing_ranges"]
        statuses = {cell["line_no"]: cell["status"] for cell in view["line_grid"]}
        assert statuses[1] == "delivered"
        assert statuses[4] == "missing"
        assert statuses[7] == "delivered"

    def test_authorized_lines_exclude_gap(self):
        _deliver([
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
        ])
        authorized = shard_ingest.authorized_lines(BATCH)
        line_nos = [line["原始行号"] for line in authorized["lines"]]
        # 缺口行 3-5 不在获准解析范围
        assert line_nos == [1, 2, 6, 7, 8]
        assert authorized["count"] == 5


class TestConflictAndIdempotentRedelivery:
    """同片号撞不同正文可查差异；断线重送同文复用行引用不盖历史。"""

    def test_same_shard_changed_line_creates_conflict(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        before_ids = list(json.loads(_manifest("1")["授权原始行ID"]))

        conflict = _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        receipt = conflict["receipts"][0]
        assert receipt["result"] == "内容冲突"
        assert receipt["authorized_version"] == "v1"

        manifest = _manifest("1")
        assert manifest["status"] == "内容冲突"
        # 授权版本仍为 v1，历史行引用没有被覆盖
        assert manifest["当前版本"] == "v1"
        assert json.loads(manifest["授权原始行ID"]) == before_ids

    def test_conflict_diff_locates_changed_line(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        diff = shard_ingest.conflict_diff(BATCH, "1")
        assert diff["ok"] is True
        changed = [d for d in diff["conflict"]["line_diff"] if d["changed"]]
        assert len(changed) == 1
        assert changed[0]["line_no"] == 2
        assert changed[0]["old_body"] == SHARD1[1]
        assert changed[0]["new_body"] == SHARD1_CHANGED[1]

    def test_ingest_log_keeps_both_attempts(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        logs = [r for r in store.rows("shard_ingest_log")
                if r["批次编号"] == BATCH and r["片号"] == "1"]
        assert [r["接入结果"] for r in logs] == ["首次接收", "内容冲突"]
        assert logs[0]["正文摘要"] != logs[1]["正文摘要"]

    def test_identical_redelivery_is_idempotent(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        raw_before = [r["id"] for r in store.rows("raw_records") if r.get("批次编号") == BATCH]

        again = _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        assert again["receipts"][0]["result"] == "幂等复用"
        assert again["receipts"][0]["reused"] is True

        raw_after = [r["id"] for r in store.rows("raw_records") if r.get("批次编号") == BATCH]
        # 断线恢复重送原文：复用已有行引用，不新增原始行
        assert raw_after == raw_before
        manifest = _manifest("1")
        assert manifest["接入次数"] == "2"
        assert manifest["status"] == "已交付"

    def test_idempotent_resend_after_conflict_keeps_history(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        v1_ids = json.loads(_manifest("1")["授权原始行ID"])
        # 断线恢复后按原编号重送原文（v1 同文摘）
        replay = _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        assert replay["receipts"][0]["result"] == "幂等复用"
        assert replay["receipts"][0]["raw_ids"] == v1_ids
        assert _manifest("1")["status"] == "内容冲突"  # 冲突仍需人工裁决，不会被重送抹掉


class TestAuthorizedParsing:
    """字段解析按清单确定可读正文，分段结果回写采集待处理清单。"""

    def test_parse_only_authorized_lines_before_shard2(self):
        _deliver([
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
        ])
        result = shard_ingest.parse_authorized(BATCH)
        assert result["ok"] is True
        assert result["parsed_count"] == 5  # 片1两行 + 片3三行，缺口不解析
        mapping = {m["原始行号"]: m["成品行号"] for m in result["line_mapping"]}
        assert set(mapping) == {1, 2, 6, 7, 8}
        # 成品行号按解析顺序连续分配
        assert [mapping[k] for k in sorted(mapping)] == ["P0001", "P0002", "P0003", "P0004", "P0005"]

        raw = [r for r in store.rows("raw_records") if r.get("批次编号") == BATCH]
        assert all(r["status"] == "已解析" and r["成品行号"] for r in raw if r["授权可读"])

    def test_parse_result_written_back_to_pending_list(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        shard_ingest.parse_authorized(BATCH)
        rows = [r for r in store.rows("raw_records")
                if r.get("批次编号") == BATCH and r["片号"] == "1"]
        assert all(r["解析状态"] == "已解析" and r["pending"] is False for r in rows)
        # 标准化明细保留原始记录ID与成品行号
        entities = [e for e in store.rows("parsed_entity") if e.get("来源批次") == BATCH]
        assert len(entities) == 2
        assert all(e["成品行号"].startswith("P") for e in entities)

    def test_resume_after_supplementary_shard2(self):
        """补交第二片后只处理新增待解析行，成品行号追加不重排。"""
        _deliver([
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
        ])
        first_parse = shard_ingest.parse_authorized(BATCH)
        first_mapping = {m["原始行号"]: m["成品行号"] for m in first_parse["line_mapping"]}

        # 收齐第二片
        fill = _deliver([{"shard_no": "2", "start_line": 3, "lines": SHARD2}])
        assert fill["ok"] is True
        view = shard_ingest.batch_view(BATCH)
        assert view["missing_ranges"] == []
        assert view["missing_shards"] == []

        second_parse = shard_ingest.parse_authorized(BATCH)
        # 已解析的 5 行不重复处理
        assert second_parse["parsed_count"] == 3
        assert second_parse["skipped_already_parsed"] == 5
        new_mapping = {m["原始行号"]: m["成品行号"] for m in second_parse["line_mapping"]}
        assert set(new_mapping) == {3, 4, 5}
        # 新成品行号追加，历史成品行号不变
        assert list(new_mapping.values()) == ["P0006", "P0007", "P0008"]
        full = shard_ingest.line_number_map(BATCH)
        final = {m["原始行号"]: m["成品行号"] for m in full["mapping"]
                 if m["授权可读"] and m["成品行号"]}
        assert final == {1: "P0001", 2: "P0002", 3: "P0006", 4: "P0007",
                         5: "P0008", 6: "P0003", 7: "P0004", 8: "P0005"}

    def test_line_map_before_and_after_shard2(self):
        _deliver([
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
        ])
        shard_ingest.parse_authorized(BATCH)
        before = shard_ingest.line_number_map(BATCH)["mapping"]
        gap_rows = [m for m in before if 3 <= m["原始行号"] <= 5]
        assert gap_rows == []  # 片2未到，整批里根本没有这些行，只有缺口区间展示

        _deliver([{"shard_no": "2", "start_line": 3, "lines": SHARD2}])
        after = shard_ingest.line_number_map(BATCH)["mapping"]
        gap_after = [m for m in after if 3 <= m["原始行号"] <= 5]
        assert all(m["授权可读"] is True and m["解析状态"] == "待解析" for m in gap_after)

    def test_conflict_version_not_parsed_until_resolved(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        shard_ingest.parse_authorized(BATCH)
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        # 未裁决：新版本行不获准、不参与解析
        authorized = shard_ingest.authorized_lines(BATCH)
        v2_rows = [line for line in authorized["lines"] if line["行版本"] == "v2"]
        assert v2_rows == []

        manifest = _manifest("1")
        resolve = shard_ingest.resolve_conflict(manifest["id"], "v2")
        assert resolve["ok"] is True
        authorized2 = shard_ingest.authorized_lines(BATCH)
        assert all(line["行版本"] == "v2" for line in authorized2["lines"])

    def test_resolve_bad_version_rejected(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        manifest = _manifest("1")
        bad = shard_ingest.resolve_conflict(manifest["id"], "v9")
        assert bad["ok"] is False


class TestBatchLifecycle:
    """批次汇总与接入留痕的跨表一致性。"""

    def test_batch_auto_created_and_completed_when_filled(self):
        _deliver([
            {"shard_no": "1", "start_line": 1, "lines": SHARD1},
            {"shard_no": "3", "start_line": 6, "lines": SHARD3},
        ])
        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == BATCH)
        assert batch["status"] == "采集中"
        _deliver([{"shard_no": "2", "start_line": 3, "lines": SHARD2}])
        assert batch["status"] == "已完成"
        assert batch["记录总数"] == "8"

    def test_conflict_marks_batch_abnormal(self):
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1}])
        _deliver([{"shard_no": "1", "start_line": 1, "lines": SHARD1_CHANGED}])
        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == BATCH)
        assert batch["abnormal"] is True

    def test_digest_stable_for_same_content(self):
        assert digest_of(SHARD1) == digest_of(list(reversed(SHARD1[::-1])))
        assert digest_of(SHARD1) != digest_of(SHARD1_CHANGED)
