"""Server TLS and mutual-TLS boundaries for the tenant runtime host."""

from __future__ import annotations

import http.client
import ipaddress
import ssl
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("cryptography")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from prometa.runtime import (
    RuntimeHostError,
    RuntimeHostResponse,
    RuntimeServerTlsConfig,
    build_runtime_server_ssl_context,
)
from prometa.runtime.host import _RuntimeHttpServer
from prometa.runtime.host import main as runtime_host_main


class _HealthApplication:
    max_request_bytes = 1024

    def handle(self, method, path, headers, body):
        if method == "GET" and path in {"/healthz", "/readyz"}:
            return RuntimeHostResponse(200, {"status": "ok"})
        return RuntimeHostResponse(404, {"error": {"code": "not_found"}})

    def close(self):
        return None


def _write_key(path: Path, key) -> None:
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )


def _create_material(directory: Path):
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "runtime-test-ca")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = directory / "ca.crt"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))

    def issue(name: str, usage, *, server: bool = False):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        builder = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        )
        if server:
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    ]
                ),
                critical=False,
            )
        certificate = builder.sign(ca_key, hashes.SHA256())
        cert_path = directory / (name + ".crt")
        key_path = directory / (name + ".key")
        cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        _write_key(key_path, key)
        return cert_path, key_path

    server_cert, server_key = issue(
        "runtime-server", ExtendedKeyUsageOID.SERVER_AUTH, server=True
    )
    client_cert, client_key = issue(
        "runtime-probe", ExtendedKeyUsageOID.CLIENT_AUTH
    )
    return ca_path, server_cert, server_key, client_cert, client_key


def _start_server(context):
    server = _RuntimeHttpServer(("127.0.0.1", 0), _HealthApplication(), context)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _request(server, context):
    connection = http.client.HTTPSConnection(
        "127.0.0.1", server.server_address[1], timeout=2, context=context
    )
    connection.request("GET", "/readyz")
    response = connection.getresponse()
    payload = response.read()
    connection.close()
    return response.status, payload


def test_server_tls_serves_health_with_ca_validation(tmp_path):
    ca, cert, key, _, _ = _create_material(tmp_path)
    context = build_runtime_server_ssl_context(
        RuntimeServerTlsConfig(certificate_file=cert, private_key_file=key)
    )
    server, thread = _start_server(context)
    try:
        client = ssl.create_default_context(cafile=str(ca))
        assert _request(server, client) == (200, b'{"status":"ok"}')
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_mutual_tls_rejects_anonymous_and_accepts_probe_identity(tmp_path):
    ca, cert, key, client_cert, client_key = _create_material(tmp_path)
    context = build_runtime_server_ssl_context(
        RuntimeServerTlsConfig(
            certificate_file=cert,
            private_key_file=key,
            client_ca_file=ca,
            require_client_certificate=True,
        )
    )
    server, thread = _start_server(context)
    try:
        anonymous = ssl.create_default_context(cafile=str(ca))
        with pytest.raises((ssl.SSLError, http.client.RemoteDisconnected)):
            _request(server, anonymous)

        authenticated = ssl.create_default_context(cafile=str(ca))
        authenticated.load_cert_chain(str(client_cert), str(client_key))
        assert _request(server, authenticated)[0] == 200
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_mutual_tls_requires_client_ca(tmp_path):
    _, cert, key, _, _ = _create_material(tmp_path)
    with pytest.raises(RuntimeHostError) as caught:
        build_runtime_server_ssl_context(
            RuntimeServerTlsConfig(
                certificate_file=cert,
                private_key_file=key,
                require_client_certificate=True,
            )
        )
    assert caught.value.code == "server_tls_client_ca_required"


def test_invalid_server_material_has_stable_error(tmp_path):
    with pytest.raises(RuntimeHostError) as caught:
        build_runtime_server_ssl_context(
            RuntimeServerTlsConfig(
                certificate_file=tmp_path / "missing.crt",
                private_key_file=tmp_path / "missing.key",
            )
        )
    assert caught.value.code == "server_tls_material_invalid"


def test_cli_rejects_partial_tls_configuration(tmp_path, capsys):
    result = runtime_host_main(
        ["--config", str(tmp_path / "missing.json"), "--tls-cert-file", "server.crt"]
    )
    assert result == 2
    assert '"code":"server_tls_configuration_invalid"' in capsys.readouterr().err


def test_cli_rejects_invalid_client_certificate_environment(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv(
        "PROMETA_RUNTIME_SERVER_TLS_REQUIRE_CLIENT_CERTIFICATE", "sometimes"
    )
    result = runtime_host_main(["--config", str(tmp_path / "missing.json")])
    assert result == 2
    assert '"code":"server_tls_configuration_invalid"' in capsys.readouterr().err
