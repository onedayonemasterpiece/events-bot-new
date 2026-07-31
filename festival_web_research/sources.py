"""URL canonicalization and SSRF guards for untrusted research sources."""
from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable, Iterable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


class UnsafeSourceURL(ValueError):
    """Raised when a URL cannot safely be fetched by the host."""


Resolver = Callable[[str, int], Iterable[str]]


def _public_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError as exc:
        raise UnsafeSourceURL(f"invalid resolved IP: {value!r}") from exc
    return bool(ip.is_global)


def validate_resolved_addresses(hostname: str, addresses: Iterable[str]) -> tuple[str, ...]:
    """Require a non-empty resolution set consisting only of public IPs.

    Call this for the initial request *and every redirect*.  Returning a mixed
    public/private set is rejected, which is important for DNS-rebinding guards.
    """
    values = tuple(dict.fromkeys(addresses))
    if not values:
        raise UnsafeSourceURL(f"host did not resolve: {hostname}")
    for value in values:
        if not _public_ip(value):
            raise UnsafeSourceURL(f"non-public address for {hostname}: {value}")
    return values


def system_resolver(hostname: str, port: int) -> tuple[str, ...]:
    try:
        infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise UnsafeSourceURL(f"DNS resolution failed for {hostname}") from exc
    return tuple(info[4][0] for info in infos)


def canonicalize_public_url(
    raw_url: str,
    *,
    resolver: Resolver | None = None,
    require_dns: bool = False,
    max_length: int = 4096,
) -> str:
    """Return a stable public HTTP(S) URL or fail closed.

    DNS is injectable so tests and offline planning never contact the network.
    Production callers should pass a resolver (or ``system_resolver``) and set
    ``require_dns=True`` immediately before each fetch/redirect.
    """
    if not isinstance(raw_url, str) or not raw_url or len(raw_url) > max_length:
        raise UnsafeSourceURL("URL is empty, non-string, or too long")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7f for ch in raw_url) or "\\" in raw_url:
        raise UnsafeSourceURL("URL contains control characters or backslashes")
    try:
        parts = urlsplit(raw_url)
        port = parts.port
    except ValueError as exc:
        raise UnsafeSourceURL("malformed URL") from exc
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise UnsafeSourceURL("only http and https are allowed")
    if not parts.hostname or parts.username is not None or parts.password is not None:
        raise UnsafeSourceURL("URL hostname is required and credentials are forbidden")

    hostname = parts.hostname.rstrip(".").lower()
    blocked_names = {"localhost", "localhost.localdomain", "metadata.google.internal"}
    blocked_suffixes = (".localhost", ".local", ".internal", ".home", ".lan")
    if not hostname or hostname in blocked_names or hostname.endswith(blocked_suffixes):
        raise UnsafeSourceURL("local/internal hostnames are forbidden")
    try:
        canonical_host = hostname.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise UnsafeSourceURL("invalid international hostname") from exc
    if len(canonical_host) > 253:
        raise UnsafeSourceURL("hostname is too long")

    try:
        literal_ip = ipaddress.ip_address(canonical_host.split("%", 1)[0])
    except ValueError:
        literal_ip = None
    if literal_ip is not None and not literal_ip.is_global:
        raise UnsafeSourceURL("non-public literal IP is forbidden")

    default_port = 443 if scheme == "https" else 80
    effective_port = port or default_port
    if not 1 <= effective_port <= 65535:
        raise UnsafeSourceURL("invalid port")
    if resolver is not None:
        validate_resolved_addresses(canonical_host, resolver(canonical_host, effective_port))
    elif require_dns:
        raise UnsafeSourceURL("DNS validation is required but no resolver was supplied")

    display_host = f"[{canonical_host}]" if literal_ip and literal_ip.version == 6 else canonical_host
    netloc = display_host if port in (None, default_port) else f"{display_host}:{port}"
    path = quote(parts.path or "/", safe="/%:@!$&'()*+,;=-._~")
    query_pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=False)
    query = urlencode(sorted(query_pairs), doseq=True, quote_via=quote)
    canonical = urlunsplit((scheme, netloc, path, query, ""))
    if len(canonical) > max_length:
        raise UnsafeSourceURL("canonical URL is too long")
    return canonical
