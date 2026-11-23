#!/usr/bin/env python3
"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : main.py (ponto de entrada)
Papel   : lê argumentos e config.json, configura logging (tela: INFO; arquivo
          --log-file: tudo, inclusive JSON bruto em DEBUG), monta
          P2PNetwork + MessageRouter + CLI e executa o laço principal.

Precedência das configurações: linha de comando > config.json > padrões.
"""
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from rede import NetworkConfig, P2PNetwork          # noqa: E402
from jogo.message_router import MessageRouter       # noqa: E402
from interface.cli import CLI                       # noqa: E402

DEFAULTS = {
    "rdv_host": "pyp2p.mfcaetano.cc", "rdv_port": 8080, "namespace": "CIC", "name": "alice",
    "listen_port": 8081, "register_ttl": 7200, "discover_interval": 20, "ping_interval": 30,
    "ping_miss_limit": 3, "max_reconnect_attempts": 5, "base_backoff": 2, "max_backoff": 60,
    "lives": 6, "max_players": 4, "turn_timeout": 30, "words_file": "words.txt", "log_file": None,
}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="PyP2P-Forca: Jogo da Forca P2P")
    p.add_argument("--config", default=str(BASE_DIR / "config.json"), help="arquivo JSON de configuração")
    p.add_argument("--name"); p.add_argument("--namespace")
    p.add_argument("--listen-host", default="0.0.0.0", help="interface de escuta (padrão 0.0.0.0)")
    p.add_argument("--listen-port", type=int)
    p.add_argument("--rdv-host"); p.add_argument("--rdv-port", type=int)
    p.add_argument("--register-ttl", type=int); p.add_argument("--discover-interval", type=float)
    p.add_argument("--ping-interval", type=float); p.add_argument("--ping-miss-limit", type=int)
    p.add_argument("--turn-timeout", type=float); p.add_argument("--lives", type=int)
    p.add_argument("--max-players", type=int); p.add_argument("--words-file")
    p.add_argument("--log-file", help="grava todos os logs (inclusive DEBUG com JSON bruto)")
    return p.parse_args(argv)


def load_config(args) -> dict:
    cfg = dict(DEFAULTS)
    path = Path(args.config)
    if path.exists():
        try:
            cfg.update(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            sys.exit(f"Erro ao ler {path}: {exc}")
    for key in DEFAULTS:
        value = getattr(args, key, None)
        if value is not None:
            cfg[key] = value
    cfg["_base_dir"] = str(path.resolve().parent if path.exists() else BASE_DIR)
    return cfg


def setup_logging(log_file):
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    screen = logging.StreamHandler(sys.stderr)
    screen.setLevel(logging.INFO)
    screen.setFormatter(fmt)
    root.addHandler(screen)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        root.addHandler(fh)


async def amain(args):
    cfg = load_config(args)
    setup_logging(cfg.get("log_file"))
    net_cfg = NetworkConfig(
        name=cfg["name"], namespace=cfg["namespace"], listen_host=args.listen_host,
        listen_port=cfg["listen_port"], rdv_host=cfg["rdv_host"], rdv_port=cfg["rdv_port"],
        register_ttl=cfg["register_ttl"], discover_interval=cfg["discover_interval"],
        ping_interval=cfg["ping_interval"], ping_miss_limit=cfg["ping_miss_limit"],
        max_reconnect_attempts=cfg["max_reconnect_attempts"], base_backoff=cfg["base_backoff"],
        max_backoff=cfg["max_backoff"])
    network = P2PNetwork(net_cfg)
    router = MessageRouter(network, {"lives": cfg["lives"], "max_players": cfg["max_players"],
                                     "turn_timeout": cfg["turn_timeout"]})
    cli = CLI(network, router, cfg)
    await network.start()
    await cli.run()


def main(argv=None):
    args = parse_args(argv)
    try:
        asyncio.run(amain(args))
    except OSError as exc:  # ex.: porta de escuta já em uso
        sys.exit(f"Erro de rede: {exc}")
    except ValueError as exc:
        sys.exit(f"Configuração inválida: {exc}")


if __name__ == "__main__":
    main()
