"""Bulk export support for ingestion (T1 #48 A5).

Handles the submit → poll → download lifecycle for APIs that provide
asynchronous bulk export jobs.
"""
from __future__ import annotations

import csv
import io
import uuid
from typing import Any, AsyncIterator

import anyio
import orjson
import structlog

from inandout.config._duration import parse_duration
from inandout.config.ingestion import BulkExportConfig

logger = structlog.get_logger(__name__)


class BulkExportFailed(Exception):
    """Raised when a bulk export job fails or exceeds max_wait."""

    def __init__(self, job_id: str, status: str) -> None:
        super().__init__(f"Bulk export job {job_id!r} failed with status {status!r}")
        self.job_id = job_id
        self.status = status


def _extract_nested(data: Any, path: str) -> Any:
    """Navigate a dot-notation path through a dict/list structure."""
    parts = path.split(".")
    current = data
    for part in parts:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if idx < len(current) else None
        else:
            return None
    return current


async def run_bulk_export(
    transport: Any,
    bulk_cfg: BulkExportConfig,
    run_id: uuid.UUID,
    pool: Any = None,
) -> AsyncIterator[dict[str, Any]]:
    """Submit a bulk export job, poll until complete, then stream records.

    Stores job_id in inout_ops_sync_checkpoint so a crash can resume polling.
    """
    log = logger.bind(run_id=str(run_id))
    poll_secs = parse_duration(bulk_cfg.poll_interval)
    max_wait_secs = parse_duration(bulk_cfg.max_wait)

    # Check for existing checkpoint (crash resume: re-poll from stored job_id)
    job_id: str | None = None
    if pool is not None:
        try:
            async with pool.connection() as conn:
                row = await (await conn.execute(
                    """
                    SELECT cursor_value FROM inout_ops_sync_checkpoint
                    WHERE run_id = %s
                    """,
                    [str(run_id)],
                )).fetchone()
            if row and row[0] and row[0].startswith("bulk_export_job:"):
                job_id = row[0][len("bulk_export_job:"):]
                log.info("bulk_export_resuming_from_job_id", job_id=job_id)
        except Exception:
            pass

    # Submit new job if not resuming
    if job_id is None:
        submit_resp = await transport._request(
            bulk_cfg.submit_method.upper(), bulk_cfg.submit_path
        )
        try:
            submit_body = orjson.loads(submit_resp.content) if submit_resp.content else {}
        except Exception:
            submit_body = {}
        job_id = str(_extract_nested(submit_body, bulk_cfg.job_id_field) or "")
        if not job_id:
            raise BulkExportFailed("", "no_job_id_in_submit_response")
        log.info("bulk_export_job_submitted", job_id=job_id)

        # Persist job_id to checkpoint for crash recovery
        if pool is not None:
            try:
                async with pool.connection() as conn:
                    await conn.execute(
                        """
                        INSERT INTO inout_ops_sync_checkpoint
                            (run_id, connector, datatype, page_number, cursor_value,
                             records_committed, checkpointed_at)
                        VALUES (%s, '', '', 0, %s, 0, NOW())
                        ON CONFLICT (run_id) DO UPDATE SET
                            cursor_value = EXCLUDED.cursor_value,
                            checkpointed_at = NOW()
                        """,
                        [str(run_id), f"bulk_export_job:{job_id}"],
                    )
                    await conn.commit()
            except Exception:
                pass

    # Poll until complete, failed, or max_wait exceeded
    elapsed = 0.0
    final_status = ""
    while elapsed < max_wait_secs:
        await anyio.sleep(poll_secs)
        elapsed += poll_secs

        status_url = f"{bulk_cfg.status_path}/{job_id}"
        status_resp = await transport._request("GET", status_url)
        try:
            status_body = orjson.loads(status_resp.content) if status_resp.content else {}
        except Exception:
            status_body = {}

        final_status = str(_extract_nested(status_body, bulk_cfg.status_field) or "")
        log.debug("bulk_export_poll", job_id=job_id, status=final_status, elapsed=elapsed)

        if final_status in bulk_cfg.complete_values:
            break
        if final_status in bulk_cfg.failed_values:
            raise BulkExportFailed(job_id, final_status)
    else:
        raise BulkExportFailed(job_id, f"max_wait_exceeded_after_{max_wait_secs}s")

    log.info("bulk_export_complete", job_id=job_id)

    # Download and stream records with retry logic
    download_url = f"{bulk_cfg.download_path}/{job_id}"
    download_resp = None
    last_error = None
    
    # Retry download up to 3 times (download might fail temporarily)
    for attempt in range(3):
        try:
            download_resp = await transport._request("GET", download_url)
            download_resp.raise_for_status()
            break
        except Exception as exc:
            last_error = exc
            log.warning("bulk_export_download_failed", attempt=attempt + 1, error=str(exc))
            if attempt < 2:  # Not last attempt
                await anyio.sleep(5.0 * (attempt + 1))  # Exponential backoff
    
    if download_resp is None:
        raise Exception(f"Failed to download bulk export after 3 attempts: {last_error}")
    
    content = download_resp.content or b""

    result_format = bulk_cfg.result_format

    if result_format == "jsonl":
        for line in content.decode("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield orjson.loads(line)
            except Exception:
                pass

    elif result_format == "json_array":
        try:
            data = orjson.loads(content)
        except Exception:
            return
        if bulk_cfg.record_selector:
            data = _extract_nested(data, bulk_cfg.record_selector)
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    yield record

    elif result_format == "csv":
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        for row_dict in reader:
            yield dict(row_dict)
    
    elif result_format == "xml":
        # XML format support for bulk exports
        try:
            import xml.etree.ElementTree as ET
            
            root = ET.fromstring(content.decode("utf-8"))
            
            # If record_selector is provided, navigate to the container
            if bulk_cfg.record_selector:
                parts = bulk_cfg.record_selector.split(".")
                current = root
                for part in parts:
                    found = current.find(part)
                    if found is not None:
                        current = found
                    else:
                        return
                root = current
            
            # Find all records by tag name
            record_tag = bulk_cfg.xml_record_tag or "item"
            for elem in root.findall(f".//{record_tag}"):
                # Convert XML element to dict
                record: dict[str, Any] = {}
                for child in elem:
                    # Simple conversion: tag name -> key, text -> value
                    tag = child.tag
                    if "}" in tag:  # Handle namespaces
                        tag = tag.split("}", 1)[1]
                    record[tag] = child.text or ""
                
                # Also include attributes
                for attr_name, attr_value in elem.attrib.items():
                    record[f"@{attr_name}"] = attr_value
                
                if record:
                    yield record
        except Exception as exc:
            log.warning("xml_parse_failed", error=str(exc))
