"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : rede/heartbeat.py (Parte 1)
Papel   : keep-alive PING/PONG. Um PING é enviado a cada ping_interval
          segundos; após ping_miss_limit (3) PINGs consecutivos sem PONG a
          conexão é marcada STALE e fechada.
"""
import asyncio
import logging
import time
from .protocolo import new_id, now_utc

log = logging.getLogger("rede.heartbeat")


async def heartbeat_loop(network, connection):
    """
    Envia PING periodicamente; 
    três ciclos sem resposta causam STALE.
    
    """
    while network.running and not connection.writer.is_closing():
        await asyncio.sleep(network.config.ping_interval)
        if not network.running or connection.writer.is_closing():
            break
        
        # Contabiliza PINGs dos intervalos anteriores ainda não respondidos
        if connection.pending_pings:
            connection.missed += 1
            connection.pending_pings.clear()
            if connection.missed >= network.config.ping_miss_limit:
                log.warning("[KeepAlive] %s missed %d PINGs — marking STALE",
                            connection.peer_id, connection.missed)
                await network._drop(connection, stale=True, reason="PING sem PONG")
                break
        msg_id = new_id()
        timestamp = now_utc()

        connection.pending_pings[msg_id] = (time.monotonic(), timestamp)

        try:
            await connection.send({
                                    "type": "PING",
                                    "msg_id": msg_id,
                                    "timestamp": timestamp,
                                    "ttl": 1
                                })
        except (OSError, ConnectionError):
            await network._drop(connection, stale=True, reason="Falha ao enviar PING")
            break
