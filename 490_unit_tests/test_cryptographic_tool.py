# tests/test_crypto_tool.py
# One file covering BOTH methods:
# - extract_did_from_private_key_bytes
# - generate_did_key
#
# Assumes this file is in .../tests and the code is in .../src/cryptographic_tool.py

import sys
from pathlib import Path

# Make ../src importable
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(ROOT / "src"))

import base58  # pip install base58
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from cryptographic_tool import (
    extract_did_from_private_key_bytes,
    generate_did_key,
)

# ---------- Fixed test data for extract_did_from_private_key_bytes ----------
#John Smith pem
VALID_ED25519_PEM = b"""-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIBYyrSxxk6QhI3E1unPwqbmrTu+jmcIjqnEMtbNuTS0H
-----END PRIVATE KEY-----
"""
#John Smith DID
EXPECTED_DID = "did:key:6MkjKLczYJYYzj9oVuhetxVrSir7UjtQxru8tRWKiAPzH9D"


# ================================
# Tests for extract_did_from_private_key_bytes
# ================================

def test_extract_from_bytes_valid_pem_returns_expected_did():
    got = extract_did_from_private_key_bytes(VALID_ED25519_PEM)
    assert got == EXPECTED_DID
    assert got.startswith("did:key:")


@pytest.mark.parametrize("bad_bytes", [
    b"",  # empty buffer
    b"kjhkhsgjnsjgniuwenriugnweiu",  # completely invalid format
    b"-----BEGIN PRIVATE KEY-----\ninvalid pem\n-----END PRIVATE KEY-----\n",  # malformed PEM
])
def test_extract_from_bytes_bad_inputs_raise(bad_bytes):
    with pytest.raises(Exception):
        extract_did_from_private_key_bytes(bad_bytes)


def test_extract_from_bytes_wrong_key_type_raises_valueerror():
    # Generate a non-Ed25519 (RSA) private key PEM on the fly
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    rsa_pem = rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    with pytest.raises(ValueError) as e:
        extract_did_from_private_key_bytes(rsa_pem)
    assert "Invalid key type. Expected Ed25519." in str(e.value)


# ================================
# Tests for generate_did_key
# ================================

def test_generate_did_key_returns_tuple_and_prefix():
    did, priv = generate_did_key()
    assert isinstance(did, str)
    assert hasattr(priv, "public_key")  # Ed25519PrivateKey-like
    assert did.startswith("did:key:")


def test_generate_did_key_multicodec_prefix_and_length():
    did, _ = generate_did_key()
    b58 = did.split("did:key:")[1]
    raw = base58.b58decode(b58)
    # Should be 0xED 0x01 prefix + 32-byte raw Ed25519 public key = 34 bytes
    assert len(raw) == 34
    assert raw[0] == 0xED and raw[1] == 0x01


def test_generate_did_key_round_trip_matches_extractor():
    did, priv = generate_did_key()
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    round_trip = extract_did_from_private_key_bytes(pem)
    assert round_trip == did


def test_generate_did_key_uniqueness_two_calls():
    d1, _ = generate_did_key()
    d2, _ = generate_did_key()
    assert d1 != d2
