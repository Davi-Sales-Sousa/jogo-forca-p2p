"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : tests/fake_rendezvous.py
Papel   : servidor Rendezvous de TESTE escrito a partir da especificação
          (REGISTER/DISCOVER/UNREGISTER, uma conexão = um comando). NÃO é o
          servidor do professor (src/rendezvous/ não foi enviado); serve só
          para testar o cliente localmente, sem rede externa.
"""
import asyncio
import json
import time


class FakeRendezvous:
    def __init__(self, host="25.20.145.55", port=0):
        self.host, self.port = host, port
        self.peers = {}
        self.server = None
        self.requests = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()

    def _alive(self):
        now = time.monotonic()
        return [p for p in self.peers.values() if p["expires"] > now]

    async def _handle(self, reader, writer):
        try:
            raw = await asyncio.wait_for(reader.readline(), 5)
            ip = writer.get_extra_info("peername")[0]
            try:
                msg = json.loads(raw.decode())
                self.requests.append(msg.get("type"))
                resp = self._dispatch(msg, ip)
            except Exception:
                resp = {"status": "ERROR", "message": "Empty request line" if not raw.strip() else "bad_json"}
            writer.write((json.dumps(resp) + "\n").encode())
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    def _dispatch(self, m, ip):
        t = m.get("type")
        if t == "REGISTER":
            name, ns, port = m.get("name"), m.get("namespace"), m.get("port")
            if not isinstance(name, str) or not 1 <= len(name) <= 64:
                return {"status": "ERROR", "message": "bad_name"}
            if not isinstance(ns, str) or not 1 <= len(ns) <= 64:
                return {"status": "ERROR", "message": "bad_namespace"}
            if type(port) is not int or not 1 <= port <= 65535:
                return {"status": "ERROR", "message": "bad_port"}
            ttl = max(1, min(86400, m.get("ttl", 7200)))
            self.peers[(ns, name)] = {"ip": ip, "port": port, "name": name, "namespace": ns,
                                      "ttl": ttl, "expires": time.monotonic() + ttl}
            return {"status": "OK", "ttl": ttl, "ip": ip, "port": port}
        if t == "DISCOVER":
            if not any(p["ip"] == ip for p in self._alive()):
                return {"status": "ERROR", "message": "peer_not_registered"}
            ns = m.get("namespace")
            out = []
            for p in self._alive():
                if ns is None or p["namespace"] == ns:
                    q = {k: p[k] for k in ("ip", "port", "name", "namespace", "ttl")}
                    q["expires_in"] = int(p["expires"] - time.monotonic())
                    out.append(q)
            return {"status": "OK", "peers": out}
        if t == "UNREGISTER":
            key = (m.get("namespace"), m.get("name"))
            if key not in self.peers:
                return {"status": "ERROR", "message": "peer_not_registered"}
            del self.peers[key]
            return {"status": "OK"}
        return {"status": "ERROR", "message": "Unknown command"}


if __name__ == "__main__":
    # Uso: python3 -m tests.fake_rendezvous [porta]   (padrão 8080, só 127.0.0.1)
    import sys

    async def _main():
        srv = await FakeRendezvous("25.20.145.55", int(sys.argv[1]) if len(sys.argv) > 1 else 8080).start()
        print(f"Rendezvous de teste em 25.20.145.55:{srv.port} (Ctrl-C encerra)", flush=True)
        await asyncio.Event().wait()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
