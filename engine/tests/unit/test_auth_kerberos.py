"""Unit tests for Kerberos authentication configuration and handler."""
from __future__ import annotations

import pytest


def test_kerberos_config_required_fields():
    """KerberosConfig should require service principal."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
    )
    
    assert config.service == "HTTP@api.example.com"


def test_kerberos_config_defaults():
    """KerberosConfig should have sensible defaults."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
    )
    
    assert config.mutual_authentication == "REQUIRED"
    assert config.delegate is False
    assert config.force_preemptive is True
    assert config.keytab_path is None
    assert config.credential_cache is None


def test_kerberos_config_with_keytab():
    """KerberosConfig should support keytab file."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
        keytab_path="/etc/krb5.keytab",
    )
    
    assert config.keytab_path == "/etc/krb5.keytab"


def test_kerberos_config_mutual_authentication_options():
    """KerberosConfig should support all mutual authentication modes."""
    from inandout.config.auth import KerberosConfig
    
    for mode in ["REQUIRED", "OPTIONAL", "DISABLED"]:
        config = KerberosConfig(
            service="HTTP@api.example.com",
            mutual_authentication=mode,
        )
        assert config.mutual_authentication == mode


def test_kerberos_auth_discriminator():
    """KerberosAuth should be recognized by type discriminator."""
    from inandout.config.auth import KerberosAuth, KerberosConfig
    
    auth = KerberosAuth(
        type="kerberos",
        credential_ref="krb5_credentials",
        kerberos=KerberosConfig(
            service="HTTP@api.example.com",
        ),
    )
    
    assert auth.type == "kerberos"
    assert auth.credential_ref == "krb5_credentials"


def test_kerberos_authenticator_requires_library():
    """KerberosAuthenticator should raise ImportError if library not available."""
    from inandout.auth.kerberos import HAS_KERBEROS
    
    if not HAS_KERBEROS:
        from inandout.auth.kerberos import KerberosAuthenticator
        from inandout.config.auth import KerberosConfig
        
        config = KerberosConfig(
            service="HTTP@api.example.com",
        )
        
        with pytest.raises(ImportError, match="requests-kerberos"):
            KerberosAuthenticator(config, {})


def test_kerberos_config_delegation():
    """KerberosConfig should support credential delegation."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
        delegate=True,
    )
    
    assert config.delegate is True


def test_kerberos_config_credential_cache():
    """KerberosConfig should support custom credential cache."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
        credential_cache="/tmp/krb5cc_1000",
    )
    
    assert config.credential_cache == "/tmp/krb5cc_1000"


def test_kerberos_config_force_preemptive():
    """KerberosConfig should allow disabling preemptive auth."""
    from inandout.config.auth import KerberosConfig
    
    config = KerberosConfig(
        service="HTTP@api.example.com",
        force_preemptive=False,
    )
    
    assert config.force_preemptive is False


def test_kerberos_auth_config_validates():
    """Full Kerberos AuthConfig should validate correctly."""
    from inandout.config.auth import KerberosAuth, KerberosConfig
    
    auth_dict = {
        "type": "kerberos",
        "credential_ref": "krb5_creds",
        "kerberos": {
            "service": "HTTP@api.example.com",
            "keytab_path": "/etc/krb5.keytab",
            "mutual_authentication": "REQUIRED",
            "delegate": False,
            "force_preemptive": True,
        },
    }
    
    auth = KerberosAuth(**auth_dict)
    assert auth.type == "kerberos"
    assert auth.kerberos.service == "HTTP@api.example.com"
    assert auth.kerberos.keytab_path == "/etc/krb5.keytab"
