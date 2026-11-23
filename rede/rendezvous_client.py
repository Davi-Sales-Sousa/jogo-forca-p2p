"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : rede/rendezvous_client.py (Parte 1)
Papel   : cliente do servidor Rendezvous (REGISTER / DISCOVER / UNREGISTER).
          Cada comando usa uma conexão TCP nova, pois o servidor fecha o
          socket após responder.
"""
import json
import socket

from .protocolo import MAX_LINE, encode_line


class RendezvousError(Exception):
    """Erro de rede, codificação ou resposta do Rendezvous."""


class RendezvousClient:
    """Uma conexão TCP nova para cada REGISTER, DISCOVER ou UNREGISTER."""

    def __init__(self, host="pyp2p.mfcaetano.cc", port=8080, timeout=4.0):
        self.host, self.port, self.timeout = host, port, timeout

    #------------------------------------------------------------
    # TCP
    def _request(self, message: dict) -> dict:
        
        try:
            with socket.create_connection((self.host, self.port), self.timeout) as conn:
                conn.settimeout(self.timeout)
                conn.sendall(encode_line(message))

                # O RDV fecha a conexão após responder; não reutilizar o socket.
                with conn.makefile("rb") as stream:
                    raw = stream.readline(MAX_LINE + 2)
                if not raw or not raw.endswith(b"\n") or len(raw) - 1 > MAX_LINE:
                    raise RendezvousError("Resposta incompleta ou maior que 32 KiB")
                
            response = json.loads(raw.decode("utf-8"))
            
            # veriica a resposta
            if not isinstance(response, dict) or response.get("status") not in {"OK", "ERROR"}:
                raise RendezvousError("Resposta inválida do Rendezvous")
            if response["status"] == "ERROR":
                raise RendezvousError(f"Rendezvous: {response.get('message', 'erro desconhecido')}")
            return response
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RendezvousError(f"Não foi possível comunicar com {self.host}:{self.port}: {exc}") from exc

    #------------------------------------------------------------
    # REGISTER
    def register(self, name: str, namespace: str, port: int, ttl: int = 7200) -> dict:
        return self._request({"type": "REGISTER", "namespace": namespace,
                              "name": name, "port": port, "ttl": ttl})
    
    #------------------------------------------------------------
    # DISCOVER
    def discover(self, namespace: str | None = None) -> list[dict]:
        msg = {"type": "DISCOVER"}
        if namespace is not None:
            msg["namespace"] = namespace
        result = self._request(msg)
        peers = result.get("peers")
        if not isinstance(peers, list):
            raise RendezvousError("Resposta DISCOVER sem lista de peers")
        return peers

    #------------------------------------------------------------
    # UNREGISTER
    def unregister(self, name: str, namespace: str, port: int) -> None:
        self._request({"type": "UNREGISTER", "namespace": namespace,
                       "name": name, "port": port})
