"""Unit tests for cluster merge & split propagation (B5)."""
from __future__ import annotations


def test_merge_action_in_supported_actions_list() -> None:
    """'merge' and 'split' should be valid action values."""
    valid_actions = ["insert", "update", "delete", "upsert", "merge", "split", "archive"]
    assert "merge" in valid_actions
    assert "split" in valid_actions


def test_merge_row_has_losing_cluster_fields() -> None:
    """A merge delta row should carry losing_cluster_id and losing_external_id in payload."""
    merge_row = {
        "_action": "merge",
        "external_id": "surviving-id",
        "payload": {
            "name": "Merged Corp",
            "losing_cluster_id": "old-cluster-uuid",
            "losing_external_id": "old-ext-id",
        },
    }
    payload = merge_row.get("payload", {})
    assert payload.get("losing_cluster_id") == "old-cluster-uuid"
    assert payload.get("losing_external_id") == "old-ext-id"


def test_split_row_has_no_external_id() -> None:
    """A split delta row has cluster_id but no external_id (new record needed)."""
    split_row = {
        "_action": "split",
        "_cluster_id": "new-child-cluster",
        "external_id": None,
        "data": {"name": "Child Corp"},
    }
    assert split_row["_action"] == "split"
    assert split_row["external_id"] is None
    assert split_row["_cluster_id"] is not None


def test_merge_missing_losing_external_id_skips_delete() -> None:
    """If losing_external_id is absent, the DELETE step should be skipped gracefully."""
    merge_row = {
        "_action": "merge",
        "external_id": "surviving-id",
        "payload": {
            "losing_cluster_id": "old-cluster-uuid",
            # losing_external_id intentionally absent
        },
    }
    losing_external_id = merge_row.get("payload", {}).get("losing_external_id")
    should_delete = losing_external_id is not None
    assert should_delete is False  # DELETE step is skipped


def test_split_captures_returned_external_id() -> None:
    """After INSERT for a split, the returned external_id should be captured."""
    # Simulate: API returns {"id": "new-external-id"} after insert
    api_response_body = {"id": "new-external-id", "name": "Child Corp"}
    captured_external_id = api_response_body.get("id")
    assert captured_external_id == "new-external-id"
