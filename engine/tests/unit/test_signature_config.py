"""Unit tests for SignatureConfig and SignatureAlgorithm."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.webhooks import SignatureAlgorithm, SignatureConfig


# --- SignatureAlgorithm enum ---

def test_algorithm_hmac_sha256_value():
    assert SignatureAlgorithm.hmac_sha256 == "hmac-sha256"


def test_algorithm_hmac_sha1_value():
    assert SignatureAlgorithm.hmac_sha1 == "hmac-sha1"


def test_algorithm_rsa_sha256_value():
    assert SignatureAlgorithm.rsa_sha256 == "rsa-sha256"


def test_algorithm_from_string_hmac_sha256():
    algo = SignatureAlgorithm("hmac-sha256")
    assert algo == SignatureAlgorithm.hmac_sha256


def test_algorithm_invalid_raises():
    with pytest.raises(ValueError):
        SignatureAlgorithm("md5")


# --- SignatureConfig model ---

def test_minimal_valid():
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Hub-Signature-256",
        credential_ref="MY_SECRET",
    )
    assert cfg.algorithm == SignatureAlgorithm.hmac_sha256
    assert cfg.header == "X-Hub-Signature-256"
    assert cfg.credential_ref == "MY_SECRET"


def test_version_default_none():
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Sig",
        credential_ref="KEY",
    )
    assert cfg.version is None


def test_version_set():
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Sig",
        credential_ref="KEY",
        version="v1",
    )
    assert cfg.version == "v1"


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Sig",
            credential_ref="KEY",
            unknown="bad",
        )


def test_missing_algorithm_raises():
    with pytest.raises(ValidationError):
        SignatureConfig(header="X-Sig", credential_ref="KEY")


def test_missing_header_raises():
    with pytest.raises(ValidationError):
        SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, credential_ref="KEY")


def test_missing_credential_ref_raises():
    with pytest.raises(ValidationError):
        SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, header="X-Sig")


def test_hmac_sha1_accepted():
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha1,
        header="X-Hub-Signature",
        credential_ref="KEY",
    )
    assert cfg.algorithm == SignatureAlgorithm.hmac_sha1


def test_round_trip_json():
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Sig",
        credential_ref="ref",
        version="v1",
    )
    loaded = SignatureConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.algorithm == cfg.algorithm
    assert loaded.version == "v1"
