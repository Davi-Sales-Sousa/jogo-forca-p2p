# PyP2P-Forca — Jogo da Forca P2P (RC202602)

Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Peer P2P em Python (asyncio, só biblioteca padrão) com descoberta via Rendezvous,
malha TCP (HELLO/PING/PONG/BYE) e Jogo da Forca (host autoritativo).

## Requisitos (Linux)
- Python >= 3.10 (validado em 3.11, 3.12 e 3.13). Sem pacotes pip.
- Acesso TCP ao Rendezvous e porta de escuta livre (padrão 8081, ver config.json).
- Não há etapa de compilação.

## Instalação
    unzip jogo-forca-p2p.zip && cd jogo-forca-p2p
    python3 --version

## Testes (não exigem rede externa)
    python3 -m unittest discover -s tests -v

## Rendezvous local para testes
    python3 -m tests.fake_rendezvous 7000

## Executar (dois terminais)
    python3 main.py --name alice --listen-port 4001 --rdv-host 127.0.0.1 --rdv-port 7000
    python3 main.py --name bob   --listen-port 4002 --rdv-host 127.0.0.1 --rdv-port 7000

Rendezvous real: ajuste rdv_host/rdv_port em config.json ou use --rdv-host/--rdv-port.
Precedência: argumentos CLI > config.json > padrões. Log completo (DEBUG) com --log-file.

## Comandos
/peers [*|#ns], /conn, /rtt, /reconnect, /games, /create <palavra>|--random [max] [vidas],
/start, /cancel, /join <host> [game_id], /leave, /guess <letra|palavra>, /word, /status,
/log, /help, /quit

## Estrutura
- main.py, config.json, words.txt
- rede/ (Parte 1): protocolo, rendezvous, conexões, heartbeat, reconexão
- jogo/ (Partes 2 e 3): hangman, game_host, game_player, message_router
- interface/cli.py
- tests/

## Limites conhecidos
- Migração de host não implementada (bônus opcional); queda do host aborta a partida.
- Não testado contra o Rendezvous público nem contra o cliente de referência.
