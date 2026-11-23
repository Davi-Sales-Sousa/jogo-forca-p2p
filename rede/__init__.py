"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : rede (Parte 1 — comunicação e rede)
Papel   : camada de transporte reutilizável: Rendezvous, conexões TCP entre
          peers, HELLO, PING/PONG, BYE e reconexão. Não conhece as regras da
          forca; entrega mensagens de jogo à camada superior por callback.
"""
from .p2p_client import P2PNetwork, NetworkConfig
from .rendezvous_client import RendezvousClient, RendezvousError

__all__ = ["P2PNetwork", "NetworkConfig", "RendezvousClient", "RendezvousError"]
