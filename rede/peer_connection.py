"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : rede/peer_connection.py (Parte 1)
Papel   : representa uma conexão TCP persistente entre dois peers e serializa
          as escritas concorrentes (um lock por conexão).
"""

import asyncio
import logging
from dataclasses import dataclass, field
from .protocolo import encode_line

wire = logging.getLogger("rede.wire")


@dataclass
class PeerConnection:
    peer_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    outbound: bool
    features: list[str]
    pending_pings: dict = field(default_factory=dict)
    missed: int = 0
    reader_running: bool = False
    heartbeat_task: asyncio.Task | None = None
    bye_waiter: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, message: dict):
        """
        Serialize um JSON por linha e proteja writes 
        concorrentes no mesmo TCP.
        """
        async with self.lock:
            if self.writer.is_closing():
                raise ConnectionError(f"Conexão encerrada: {self.peer_id}")
            data = encode_line(message)
            # JSON bruto de cada mensagem enviada (visível apenas em DEBUG).
            wire.debug("-> %s %s", self.peer_id, data.decode("utf-8").rstrip())
            self.writer.write(data)
            await self.writer.drain()

    async def close(self):
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), timeout=1)
        except (OSError, asyncio.TimeoutError):
            pass
