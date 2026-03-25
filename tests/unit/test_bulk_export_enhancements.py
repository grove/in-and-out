"""Unit tests for bulk export XML format and retry enhancements."""
from __future__ import annotations

import pytest


def test_bulk_export_xml_format_config():
    """BulkExportConfig should support xml format."""
    from inandout.config.ingestion import BulkExportConfig
    
    cfg = BulkExportConfig(
        submit_path="/export",
        status_path="/export/status",
        download_path="/export/download",
        result_format="xml",
        xml_record_tag="record",
    )
    assert cfg.result_format == "xml"
    assert cfg.xml_record_tag == "record"


def test_bulk_export_supports_all_formats():
    """BulkExportConfig should support jsonl, csv, json_array, xml."""
    from inandout.config.ingestion import BulkExportConfig
    
    for fmt in ["jsonl", "csv", "json_array", "xml"]:
        cfg = BulkExportConfig(
            submit_path="/export",
            status_path="/export/status",
            download_path="/export/download",
            result_format=fmt,
        )
        assert cfg.result_format == fmt


def test_bulk_export_xml_record_tag_optional():
    """xml_record_tag should be optional and default to None."""
    from inandout.config.ingestion import BulkExportConfig
    
    cfg = BulkExportConfig(
        submit_path="/export",
        status_path="/export/status",
        download_path="/export/download",
        result_format="xml",
    )
    assert cfg.xml_record_tag is None
