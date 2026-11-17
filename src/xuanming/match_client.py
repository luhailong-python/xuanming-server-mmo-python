"""Discover Match replicas and retry idempotent internal calls across them."""

import asyncio
import logging
import random

import grpc

from .grpc_runtime import GrpcClient

_LOG = logging.getLogger(__name__)
_MATCH = 16
_RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
_IDEMPOTENT_METHODS = {
    "/match.MatchInternal/CommitBattleOutcome",
    "/match.MatchInternal/GetBattleTerminalDecision",
    "/teampb.TeamAuthority/ReadMembership",
}


class MatchUnavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class MatchClient:
    """Use registry replicas first, with a configured address as a legacy fallback.

    Callers must only use this adapter for idempotent RPCs: a timed-out request
    may already have been handled by a different replica.
    """

    def __init__(self, directory, fallback: str, *, timeout: float = 5):
        if not fallback:
            raise ValueError("Match fallback address is required")
        self.directory, self.fallback = directory, fallback
        self.timeout = timeout
        self.clients = {}
        self._active = {}
        self._closed = False
        self._discovery_lock = asyncio.Lock()
        self._discovered = []
        self._has_snapshot = False
        self._expires = 0.0

    async def _addresses(self):
        clock = asyncio.get_running_loop().time
        async with self._discovery_lock:
            if clock() < self._expires:
                return list(self._discovered) if self._has_snapshot else [self.fallback]
            try:
                nodes = await self.directory.candidates(_MATCH, None)
            except (grpc.RpcError, OSError, TimeoutError):
                _LOG.warning("Match discovery failed; retaining last endpoints", exc_info=True)
                self._expires = clock() + 1
                return list(self._discovered) if self._has_snapshot else [self.fallback]
            addresses = set()
            for node in nodes:
                endpoint = node.grpc_endpoint
                if (node.node_type != _MATCH or node.protocol_type != 1 or not node.node_uuid
                        or node.release_track not in ("", "stable")
                        or not endpoint.ip or not 0 < endpoint.port < 65536):
                    continue
                host = f"[{endpoint.ip}]" if ":" in endpoint.ip else endpoint.ip
                addresses.add(f"{host}:{endpoint.port}")
            # When registrations exist, never bypass track validation through
            # the configured address (which might point at a canary).
            self._discovered = list(addresses) if nodes else [self.fallback]
            self._has_snapshot = True
            self._expires = clock() + 1
            return list(self._discovered)

    async def call(self, path: str, data: bytes, *, metadata: tuple = ()) -> bytes:
        if path not in _IDEMPOTENT_METHODS:
            raise ValueError("Match failover is restricted to idempotent methods")
        if self._closed:
            raise RuntimeError("Match client is closed")
        addresses = await self._addresses()
        if self._closed:
            raise RuntimeError("Match client is closed")
        # C++ connects every discovered replica before selecting READY nodes.
        # Trigger the same asynchronous connection warmup here; otherwise the
        # first successful address stays the only READY one indefinitely.
        for address in addresses:
            client = self.clients.get(address)
            if client is None:
                client = self.clients[address] = GrpcClient(address, timeout=self.timeout, enable_retries=False)
            client.channel.get_state(True)
        random.shuffle(addresses)
        # As in the C++ outcome pump, prefer channels already ready; idle
        # channels are still attempted so the initial connection can form.
        def priority(address):
            client = self.clients.get(address)
            return 0 if client and client.channel.get_state(True) == grpc.ChannelConnectivity.READY else 1
        addresses.sort(key=priority)
        last_error = None
        candidates = addresses[:3]
        if not candidates:
            raise MatchUnavailable("no eligible stable Match replica")
        deadline = asyncio.get_running_loop().time() + self.timeout
        try:
            for index, address in enumerate(candidates):
                if self._closed:
                    raise RuntimeError("Match client is closed")
                client = self.clients.get(address)
                if client is None:
                    client = self.clients[address] = GrpcClient(address, timeout=self.timeout, enable_retries=False)
                self._active[address] = self._active.get(address, 0) + 1
                try:
                    remaining = max(0.001, deadline - asyncio.get_running_loop().time())
                    attempt_timeout = remaining / (len(candidates) - index)
                    return await client.call(path, data, metadata=metadata, timeout=attempt_timeout)
                except grpc.RpcError as exc:
                    if exc.code() not in _RETRYABLE:
                        raise
                    last_error = exc
                finally:
                    self._active[address] -= 1
            if last_error is not None:
                raise last_error
            raise RuntimeError("no Match endpoint available")
        finally:
            # Retire removed registrations only when no call still uses them.
            stale = [address for address in self.clients
                     if address not in addresses and not self._active.get(address)]
            clients = [self.clients.pop(address) for address in stale]
            for address in stale:
                self._active.pop(address, None)
            await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)

    async def close(self):
        self._closed = True
        clients, self.clients = tuple(self.clients.values()), {}
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
