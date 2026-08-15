import hmac
import hashlib
from typing import Tuple
from app.config import settings


def verify_signature(raw_bytes: bytes, signature_header: str | None) -> bool:
    """
    Verify HMAC-SHA256 signature of raw webhook body bytes against X-PseudoGram-Signature header.
    Must use exact raw bytes (not re-serialized JSON) to ensure identical digest calculation.
    Uses hmac.compare_digest to prevent timing attacks.
    """
    if not settings.enable_signature_verification:
        # Signature verification bypassed when disabled in configuration
        return True

    if not signature_header:
        return False

    # Header format: sha256=<hex_digest>
    prefix = "sha256="
    if not signature_header.startswith(prefix):
        return False

    expected_hex = signature_header[len(prefix):]
    secret_bytes = settings.pseudogram_api_key.encode("utf-8")

    computed_digest = hmac.new(
        secret_bytes,
        msg=raw_bytes,
        digestmod=hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(computed_digest, expected_hex)
