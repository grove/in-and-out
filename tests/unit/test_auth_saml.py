"""Unit tests for SAML authentication configuration and handler."""
from __future__ import annotations

import pytest


def test_saml_config_required_fields():
    """SamlConfig should require essential SAML fields."""
    from inandout.config.auth import SamlConfig
    
    config = SamlConfig(
        idp_entity_id="https://idp.example.com",
        idp_sso_url="https://idp.example.com/sso",
        sp_entity_id="https://api.ourapp.com",
        assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
    )
    
    assert config.idp_entity_id == "https://idp.example.com"
    assert config.idp_sso_url == "https://idp.example.com/sso"
    assert config.sp_entity_id == "https://api.ourapp.com"
    assert config.assertion_consumer_service_url == "https://api.ourapp.com/saml/acs"


def test_saml_config_defaults():
    """SamlConfig should have sensible defaults."""
    from inandout.config.auth import SamlConfig
    
    config = SamlConfig(
        idp_entity_id="https://idp.example.com",
        idp_sso_url="https://idp.example.com/sso",
        sp_entity_id="https://api.ourapp.com",
        assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
    )
    
    assert config.name_id_format == "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
    assert config.token_attribute == "sessionToken"
    assert config.idp_x509_cert is None
    assert config.requested_authn_context is None


def test_saml_auth_discriminator():
    """SamlAuth should be recognized by type discriminator."""
    from inandout.config.auth import SamlAuth, SamlConfig
    
    auth = SamlAuth(
        type="saml",
        credential_ref="saml_credentials",
        saml=SamlConfig(
            idp_entity_id="https://idp.example.com",
            idp_sso_url="https://idp.example.com/sso",
            sp_entity_id="https://api.ourapp.com",
            assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
        ),
    )
    
    assert auth.type == "saml"
    assert auth.credential_ref == "saml_credentials"


def test_saml_authenticator_requires_library():
    """SamlAuthenticator should raise ImportError if library not available."""
    from inandout.auth.saml import HAS_SAML
    
    if not HAS_SAML:
        from inandout.auth.saml import SamlAuthenticator
        from inandout.config.auth import SamlConfig
        
        config = SamlConfig(
            idp_entity_id="https://idp.example.com",
            idp_sso_url="https://idp.example.com/sso",
            sp_entity_id="https://api.ourapp.com",
            assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
        )
        
        with pytest.raises(ImportError, match="python3-saml"):
            SamlAuthenticator(config, {})


def test_saml_config_with_certificate():
    """SamlConfig should support IDP certificate configuration."""
    from inandout.config.auth import SamlConfig
    
    cert = "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"
    
    config = SamlConfig(
        idp_entity_id="https://idp.example.com",
        idp_sso_url="https://idp.example.com/sso",
        sp_entity_id="https://api.ourapp.com",
        assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
        idp_x509_cert=cert,
    )
    
    assert config.idp_x509_cert == cert


def test_saml_config_with_token_injection():
    """SamlConfig should support custom token injection."""
    from inandout.config.auth import SamlConfig
    
    config = SamlConfig(
        idp_entity_id="https://idp.example.com",
        idp_sso_url="https://idp.example.com/sso",
        sp_entity_id="https://api.ourapp.com",
        assertion_consumer_service_url="https://api.ourapp.com/saml/acs",
        token_injection={
            "location": "header",
            "name": "X-API-Token",
            "format": "SAML {token}",
        },
    )
    
    assert config.token_injection["name"] == "X-API-Token"
    assert config.token_injection["format"] == "SAML {token}"


def test_saml_auth_config_validates():
    """Full SAML AuthConfig should validate correctly."""
    from inandout.config.auth import SamlAuth, SamlConfig
    import json
    
    auth_dict = {
        "type": "saml",
        "credential_ref": "my_saml_creds",
        "saml": {
            "idp_entity_id": "https://idp.example.com",
            "idp_sso_url": "https://idp.example.com/sso",
            "sp_entity_id": "https://api.ourapp.com",
            "assertion_consumer_service_url": "https://api.ourapp.com/saml/acs",
        },
    }
    
    auth = SamlAuth(**auth_dict)
    assert auth.type == "saml"
    assert auth.saml.idp_entity_id == "https://idp.example.com"
