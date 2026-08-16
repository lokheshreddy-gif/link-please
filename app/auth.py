import hmac
import hashlib
from typing import Tuple
from app.config import settings


def verify_signature(raw_bytes: bytes, signature_header: str | None) -> bool:
    """
    Verify HMAC-SHA256 signature of raw webhook body bytes against X-PseudoGram-Signature header.
    Must use exact raw bytes (not re-serialized JSON) to ensure identical digest calculation.
    Supports verifying against either the full API key or the base64-decoded email prefix (used by simulation).
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

    # Build candidates list: [full_key_bytes, decoded_email_bytes_if_present]
    secrets = [settings.pseudogram_api_key.encode("utf-8")]
    if "." in settings.pseudogram_api_key:
        part = settings.pseudogram_api_key.split(".")[0]
        missing_padding = len(part) % 4
        if missing_padding:
            part += "=" * (4 - missing_padding)
        try:
            import base64
            decoded = base64.b64decode(part)
            secrets.append(decoded)
        except Exception:
            pass

    for secret in secrets:
        computed_digest = hmac.new(
            secret,
            msg=raw_bytes,
            digestmod=hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(computed_digest, expected_hex):
            return True

    return False
