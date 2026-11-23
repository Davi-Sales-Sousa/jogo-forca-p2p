"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]
Módulo  : rede/protocolo.py (Parte 1)
Papel   : enquadramento JSON-por-linha (UTF-8, quebra de linha, máx. 32 KiB) e
          validação do envelope comum das mensagens peer–peer, conforme a
          especificação.
"""
import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

MAX_LINE = 32768
VERSION = "1.0"


class ProtocolError(Exception):
    """Mensagem inválida ou violação do protocolo."""

#------------------------------------------------------------
# TIMESTAMP
def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

#------------------------------------------------------------
# ID da msg
def new_id():
    return str(uuid4())

#------------------------------------------------------------
# Converte o dicionário Python para JSON e adiciona \n
def encode_line(message: dict) -> bytes:
    if not isinstance(message, dict):
        raise ProtocolError("A mensagem precisa ser um objeto JSON")
    wire = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    if len(wire) - 1 > MAX_LINE:
        raise ProtocolError("Mensagem excede 32 KiB")
    return wire


async def read_line(reader: asyncio.StreamReader) -> dict:
    try:
        raw = await reader.readline()
    except ValueError as exc:
        raise ProtocolError("Linha acima do limite de leitura") from exc
    if not raw:
        raise EOFError("Conexão encerrada")
    if not raw.endswith(b"\n") or len(raw) - 1 > MAX_LINE:
        raise ProtocolError("Linha incompleta ou maior que 32 KiB")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("bad_json") from exc
    if not isinstance(data, dict):
        raise ProtocolError("O JSON deve ser um objeto")
    return data

#------------------------------------------------------------
# ENVELOPE COMUM
def validate_envelope(data: dict, *, session: bool = False):

    if not isinstance(data.get("type"), str) or not data["type"]:
        raise ProtocolError("bad_request: type")
    
    if not isinstance(data.get("msg_id"), str) or not data["msg_id"]:
        raise ProtocolError("bad_request: msg_id")
    
    if type(data.get("ttl")) is not int or data["ttl"] != 1:
        raise ProtocolError("bad_ttl")
    
    if not session:
        if not isinstance(data.get("src"), str) or not data["src"]:
            raise ProtocolError("bad_request: src")
        if not isinstance(data.get("dst"), str) or not data["dst"]:
            raise ProtocolError("bad_request: dst")
    # TIMESTAMP É OPCIONAL MAS RECOMENDADO PRA LOGS