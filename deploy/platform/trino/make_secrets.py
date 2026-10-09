#!/usr/bin/env python3
"""Generate Trino's authentication material into a directory (REQ-PLT-11).

Writes, keeping any file that already exists unless ``--rotate`` is given:

- ``trino.pem`` (key + certificate) and ``tls.crt``: a self-signed TLS certificate
  for the coordinator's HTTPS port, for the given DNS names / IPs. Clients trust
  ``tls.crt``.
- ``password.db``: the file authenticator's bcrypt entries for the two service
  users, ``admin`` and ``superset``; their passwords are in ``passwords.env``.
- ``shared-secret`` (and ``shared-secret.env``, as ``TRINO_SHARED_SECRET=``):
  ``internal-communication.shared-secret``.
- with ``--test-jwt``: ``jwt-private.pem`` / ``jwt-public.pem``, a key pair the
  compose test stack trusts instead of the Keycloak realm's keys (tests sign tokens
  with the private key). Never used for the cluster.

Used by ``make trino-stack`` (test stack, ``--test-jwt``) and ``make trino-certs``
(cluster Secrets). Needs ``cryptography`` and ``bcrypt`` (dev dependencies).
"""
from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import secrets
from pathlib import Path

SERVICE_USERS = ("admin", "superset")


def _write(path: Path, data: str, rotate: bool, mode: int = 0o600) -> bool:
    if path.exists() and not rotate:
        return False
    path.write_text(data)
    path.chmod(mode)
    return True


def _rsa_pem() -> tuple[str, object]:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    return pem, key


def _certificate(names: list[str], days: int) -> tuple[str, str]:
    """(key PEM, certificate PEM), self-signed, with every name as a SAN."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.x509.oid import NameOID

    key_pem, key = _rsa_pem()
    sans = []
    for n in names:
        try:
            sans.append(x509.IPAddress(ipaddress.ip_address(n)))
        except ValueError:
            sans.append(x509.DNSName(n))
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
    now = dt.datetime.now(dt.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=days))
            .add_extension(x509.SubjectAlternativeName(sans), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    return key_pem, cert.public_bytes(serialization.Encoding.PEM).decode()


def generate(out: Path, names: list[str], *, test_jwt: bool = False, rotate: bool = False,
             days: int = 365) -> list[str]:
    """Write the material into ``out``; returns the files (re)written."""
    import bcrypt

    out.mkdir(parents=True, exist_ok=True)
    written = []
    if rotate or not (out / "trino.pem").exists():
        key_pem, cert_pem = _certificate(names, days)
        _write(out / "trino.pem", key_pem + cert_pem, True)
        _write(out / "tls.crt", cert_pem, True, 0o644)
        written += ["trino.pem", "tls.crt"]
    if rotate or not (out / "password.db").exists():
        passwords = {u: secrets.token_urlsafe(18) for u in SERVICE_USERS}
        entries = [f"{u}:{bcrypt.hashpw(p.encode(), bcrypt.gensalt(rounds=10)).decode()}"
                   for u, p in passwords.items()]
        _write(out / "password.db", "\n".join(entries) + "\n", True, 0o644)
        _write(out / "passwords.env",
               "".join(f"TRINO_{u.upper()}_PASSWORD={p}\n" for u, p in passwords.items()), True)
        written += ["password.db", "passwords.env"]
    if _write(out / "shared-secret", secrets.token_urlsafe(32), rotate):
        written.append("shared-secret")
    shared = (out / "shared-secret").read_text().strip()
    _write(out / "shared-secret.env", f"TRINO_SHARED_SECRET={shared}\n", True)   # follows the file
    if test_jwt and (rotate or not (out / "jwt-private.pem").exists()):
        from cryptography.hazmat.primitives import serialization
        key_pem, key = _rsa_pem()
        pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                            serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        _write(out / "jwt-private.pem", key_pem, True)
        _write(out / "jwt-public.pem", pub, True, 0o644)
        written += ["jwt-private.pem", "jwt-public.pem"]
    return written


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("out", type=Path, help="directory to write into")
    p.add_argument("--name", action="append", required=True,
                   help="certificate DNS name or IP (repeat; the first is the CN)")
    p.add_argument("--test-jwt", action="store_true", help="also write a test JWT signing key pair")
    p.add_argument("--rotate", action="store_true", help="regenerate files that already exist")
    p.add_argument("--days", type=int, default=365, help="certificate validity")
    a = p.parse_args(argv)
    written = generate(a.out, a.name, test_jwt=a.test_jwt, rotate=a.rotate, days=a.days)
    print(f"{a.out}: " + (", ".join(written) if written else "kept existing files"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
