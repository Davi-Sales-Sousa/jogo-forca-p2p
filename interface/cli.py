"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]
Módulo  : interface/cli.py (Parte 3)
Papel   : interface de linha de comando. Interpreta os comandos da
          especificação (/peers, /conn, /rtt, /reconnect, /games, /create,
          /start, /cancel, /join, /leave, /guess, /word, /status, /log, /help,
          /quit), chama o MessageRouter/P2PNetwork e exibe os eventos do jogo.

Leitura do teclado: uma thread daemon lê stdin e entrega as linhas ao laço
asyncio por uma fila; assim o event loop nunca bloqueia e o encerramento por
Ctrl-C/EOF não fica preso em input().
"""

import asyncio
import logging
import random
import shlex
import signal
import threading
import time
from pathlib import Path

from jogo.game_host import GameStatus
from jogo.hangman import normalize_word, validate_word

log = logging.getLogger("interface.cli")

HELP = """\
[CLI] Comandos:
  /peers [* | #namespace]        descobre e lista peers (padrão: namespace local)
  /conn                          lista conexões ativas (sentido, estado, RTT)
  /rtt                           RTT médio por peer
  /reconnect                     força reconciliação imediata com o Rendezvous
  /games                         envia GAME_LIST a todos os vizinhos e lista partidas
  /create <palavra> [max_players] [vidas]
  /create --random [max_players] [vidas]
                                 cria partida e torna-se host (palavra de words.txt)
  /start                         inicia a partida hospedada (exige >= 1 jogador)
  /cancel                        cancela a partida hospedada (GAME_OVER ABORTED)
  /join <host_peer_id> [game_id] entra na partida do host indicado
  /leave                         sai da partida atual
  /guess <letra>                 palpite de letra
  /word <palavra>                palpite de palavra inteira
  /status                        estado local da partida
  /log <DEBUG|INFO|WARNING|ERROR> nível de log na tela
  /help                          esta ajuda
  /quit                          encerra (LEAVE/GAME_OVER, BYE, UNREGISTER)"""


class CLI:
    def __init__(self, network, router, config: dict | None = None):
        self.network = network
        self.router = router
        self.config = config or {}
        self.running = True
        self._lines: asyncio.Queue = asyncio.Queue()
        self._ready = threading.Event()
        self._ready.set()
        router.event_handler = self.notify

    @staticmethod
    def out(text: str = ""):
        print(text, flush=True)

    # --- eventos do lado jogador (vindos do MessageRouter) ---------------
    async def notify(self, event: str, data: dict):
        if event == "JOIN_OK":
            extra = " (reconexão)" if data.get("rejoin") else ""
            self.out(f"[Game] Entrou em {data['game_id']}{extra}. Jogadores: {', '.join(data['players'])}")
        elif event == "JOIN_ERR":
            self.out(f"[Game] Não foi possível entrar na partida: {data.get('reason')}")
        elif event == "LEAVE_OK":
            self.out("[Game] Você saiu da partida.")
        elif event == "GAME_START":
            self.out(f"[Game] A partida começou! Palavra com {data['word_length']} letras, "
                     f"{data['lives']} vidas, {data['turn_timeout']} s por turno. "
                     f"Ordem: {', '.join(data['players'])}")
        elif event == "STATE":
            self._show_state(data["state"])
        elif event == "GUESS_RESULT":
            r = data["result"]
            if r.get("accepted"):
                self.out("[Game] Palpite aceito: " + ("correto" if r.get("correct") else "errado"))
            else:
                self.out(f"[Game] Palpite rejeitado: {r.get('reason')}")
        elif event == "GAME_OVER":
            r = data["result"]
            self.out(f"[Game] FIM DA PARTIDA: {r['result']}  palavra={r['word']}  vencedor={r['winner']}")
            self.out(f"[Game] Pontuação: {r['scores']}")
            if r["result"] == "ABORTED":
                self.out(f"[Game] Motivo: {r.get('reason')}")
            if r["hash_matches"] is False:
                self.out("[Game] AVISO: hash da palavra não confere (host cheated: hash mismatch)")
        elif event == "HOST_UNREACHABLE":
            self.out(f"[Game] Host {data['peer_id']} unreachable — game aborted")
        else:
            self.out(f"[CLI] evento {event}: {data}")

    def _you(self, peer_id):
        return f"{peer_id} (você)" if peer_id == self.network.peer_id else str(peer_id)

    def _show_state(self, st: dict):
        masked = " ".join(st["masked"]) if st.get("masked") else "?"
        wrong = ", ".join(st["wrong_letters"]) or "-"
        self.out(f"[Game] Palavra: {masked}     Vidas: {st['lives']}     Erradas: {wrong}")
        last = st.get("last_guess")
        if last:
            if last.get("timeout"):
                self.out(f"[Game] {last['player']} perdeu a vez (timeout)")
            else:
                what = f"letra {last['letter']}" if "letter" in last else f"palavra {last['word']}"
                self.out(f"[Game] Última jogada: {last['player']} {what} -> "
                         f"{'correta' if last['correct'] else 'errada'}")
        self.out(f"[Game] Vez de: {self._you(st['turn'])}")

    # --- comandos --------------------------------------------------------
    async def cmd_peers(self, args):
        scope = args[0] if args else None
        if scope is None:
            peers = await self.network.discover_once()
        elif scope == "*":
            peers = await self.network.query_peers(None)
        elif scope.startswith("#") and len(scope) > 1:
            peers = await self.network.query_peers(scope[1:])
        else:
            self.out("[CLI] Uso: /peers [* | #namespace]")
            return
        self.out(f"[CLI] Peers descobertos ({len(peers)}):")
        for p in peers:
            pid = f"{p.get('name')}@{p.get('namespace')}"
            info = self.network.peers.get(pid)
            self.out(f"[CLI]   {pid:<24} {p.get('ip')}:{p.get('port')}  estado={info.state if info else '-'}")

    def cmd_conn(self):
        conns = self.network.connections_status()
        self.out(f"[CLI] Conexões ativas ({len(conns)}):")
        for c in conns:
            rtt = f"{c['rtt_ms']:.1f} ms" if c["rtt_ms"] is not None else "n/d"
            self.out(f"[CLI]   {c['peer_id']:<24} {c['direction']:<8} {c['state']:<10} RTT={rtt}")

    def cmd_rtt(self):
        conns = self.network.connections_status()
        if not conns:
            self.out("[CLI] Nenhuma conexão ativa.")
        for c in conns:
            rtt = f"{c['rtt_ms']:.1f} ms" if c["rtt_ms"] is not None else "n/d (aguardando PING/PONG)"
            self.out(f"[CLI]   {c['peer_id']:<24} RTT médio={rtt}")

    async def cmd_reconnect(self):
        peers = await self.network.reconnect_now()
        self.out(f"[CLI] Reconciliação concluída: {len(peers)} peer(s) no Rendezvous; "
                 f"{len(self.network.connections)} conexão(ões) ativa(s).")

    async def cmd_games(self):
        games = await self.router.request_games()
        if not games:
            self.out("[CLI] Nenhuma partida encontrada.")
            return
        self.out("[CLI] Partidas:")
        for g in games:
            self.out(f"[CLI]   {g.get('game_id')}  host={g.get('host')}  status={g.get('status')}  "
                     f"{len(g.get('players') or [])}/{g.get('max_players')} jogadores  "
                     f"{g.get('word_length')} letras  {g.get('lives')} vidas")

    def _random_word(self) -> str:
        path = Path(self.config.get("words_file") or "words.txt")
        if not path.is_absolute():
            path = Path(self.config.get("_base_dir", ".")) / path
        try:
            words = [w for w in (normalize_word(x.strip()) for x in path.read_text(encoding="utf-8").splitlines())
                     if validate_word(w)]
        except OSError as exc:
            raise ValueError(f"não foi possível ler {path}: {exc}") from exc
        if not words:
            raise ValueError(f"{path} não contém palavras válidas (a–z, 3 a 20 letras)")
        return random.choice(words)

    def cmd_create(self, args):
        if not args:
            self.out("[CLI] Uso: /create <palavra> [max_players] [vidas]  |  /create --random [max_players] [vidas]")
            return
        word = self._random_word() if args[0] == "--random" else args[0]
        extra = args[1:]
        max_players = int(extra[0]) if len(extra) >= 1 else None
        lives = int(extra[1]) if len(extra) >= 2 else None
        s = self.router.create_game(word, max_players, lives)
        self.out(f"[CLI] Partida criada: {s.game_id}")
        self.out(f"[CLI] Palavra com {len(s.word)} letras, {s.lives} vidas, máx. {s.max_players} jogadores, "
                 f"{s.turn_timeout:g} s por turno")
        self.out("[CLI] Aguardando jogadores (use /start com pelo menos 1 jogador).")

    async def cmd_start(self):
        host = self.router.host
        if host.status != GameStatus.OPEN:
            self.out("[CLI] Você não está hospedando uma partida aberta.")
            return
        if not host.session.players:
            self.out("[CLI] /start recusado: é preciso pelo menos 1 jogador.")
            return
        if await self.router.start_game():
            self.out("[CLI] Partida iniciada.")

    async def cmd_cancel(self):
        if await self.router.cancel_game():
            self.out("[CLI] Partida cancelada (GAME_OVER ABORTED enviado aos jogadores).")
        else:
            self.out("[CLI] Você não está hospedando uma partida aberta ou em andamento.")

    async def cmd_join(self, args):
        if len(args) not in (1, 2):
            self.out("[CLI] Uso: /join <host_peer_id> [game_id]")
            return
        game_id = await self.router.join_host(args[0], args[1] if len(args) == 2 else None)
        self.out(f"[CLI] JOIN enviado para {game_id}; aguardando resposta do host.")

    async def cmd_guess(self, args):
        if len(args) != 1:
            self.out("[CLI] Uso: /guess <letra>")
            return
        letter = normalize_word(args[0])
        if len(letter) != 1 or not ("a" <= letter <= "z"):
            self.out("[CLI] Informe exatamente uma letra de a a z.")
            return
        await self.router.send_guess(letter=letter)

    async def cmd_word(self, args):
        if len(args) != 1:
            self.out("[CLI] Uso: /word <palavra>")
            return
        await self.router.send_guess(word=normalize_word(args[0]))

    def cmd_status(self):
        player, host = self.router.player, self.router.host
        if player.status != "IDLE":
            st = player.get_state()
            running = st["status"] == "PLAYING" and st["masked"]
            self.out(f"[Game] {st['game_id']}  (host: {st['host_peer_id']})  status: "
                     f"{'RUNNING' if running else 'OPEN'}")
            if st["masked"]:
                wrong = ", ".join(st["wrong_letters"]) or "-"
                self.out(f"[Game] Palavra: {' '.join(st['masked'])}     Vidas: {st['lives']}     Erradas: {wrong}")
                left = ""
                if player.state_at is not None and player.turn_timeout:
                    rest = max(0, player.turn_timeout - (time.monotonic() - player.state_at))
                    left = f"  — {rest:.0f} s restantes"
                self.out(f"[Game] Jogadores: {', '.join(st['players'])}   Vez de: {self._you(st['turn'])}{left}")
            else:
                self.out(f"[Game] Jogadores inscritos: {', '.join(st['players']) or '-'} (aguardando início)")
            return
        s = host.session
        if s is not None:
            self.out(f"[Game] {s.game_id}  (host: {s.host_id} — você)  status: {s.status.name}")
            wrong = ", ".join(s.game.get_wrong_letters()) or "-"
            self.out(f"[Game] Palavra: {' '.join(s.game.get_masked_word())}     "
                     f"Vidas: {s.game.remaining_lives} / {s.lives}     Erradas: {wrong}     (secreta: {s.word})")
            turn = host.get_current_player()
            left = ""
            if turn and s.turn_started_at is not None:
                left = f"  — {max(0, s.turn_timeout - (time.monotonic() - s.turn_started_at)):.0f} s restantes"
            self.out(f"[Game] Jogadores: {', '.join(s.players) or '-'}   Vez de: {turn or '-'}{left}")
            self.out(f"[Game] Pontuação: {s.scores}")
            return
        self.out("[Game] Você não está em nenhuma partida.")

    def cmd_log(self, args):
        level = args[0].upper() if args else ""
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            self.out("[CLI] Uso: /log <DEBUG|INFO|WARNING|ERROR>")
            return
        for h in logging.getLogger().handlers:
            if type(h) is logging.StreamHandler:  # só a tela; o arquivo guarda tudo
                h.setLevel(level)
        self.out(f"[CLI] Nível de log na tela: {level}")

    # --- laço principal --------------------------------------------------
    async def execute(self, line: str):
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            self.out(f"[CLI] Comando inválido: {exc}")
            return
        if not parts:
            return
        cmd, args = parts[0].lower(), parts[1:]
        try:
            if cmd in ("/peers", "/discover"):
                await self.cmd_peers(args)
            elif cmd == "/conn":
                self.cmd_conn()
            elif cmd == "/rtt":
                self.cmd_rtt()
            elif cmd == "/reconnect":
                await self.cmd_reconnect()
            elif cmd == "/games":
                await self.cmd_games()
            elif cmd == "/create":
                self.cmd_create(args)
            elif cmd == "/start":
                await self.cmd_start()
            elif cmd == "/cancel":
                await self.cmd_cancel()
            elif cmd == "/join":
                await self.cmd_join(args)
            elif cmd == "/leave":
                await self.router.leave_game()
            elif cmd == "/guess":
                await self.cmd_guess(args)
            elif cmd == "/word":
                await self.cmd_word(args)
            elif cmd == "/status":
                self.cmd_status()
            elif cmd == "/log":
                self.cmd_log(args)
            elif cmd == "/help":
                self.out(HELP)
            elif cmd == "/quit":
                self.running = False
            else:
                self.out("[CLI] Comando desconhecido. Digite /help.")
        except ValueError as exc:
            self.out(f"[CLI] Argumento inválido: {exc}")
        except RuntimeError as exc:
            self.out(f"[CLI] Operação não permitida: {exc}")
        except ConnectionError as exc:
            self.out(f"[CLI] Erro de conexão: {exc}")
        except Exception as exc:  # um comando nunca deve derrubar a CLI
            log.exception("Erro ao executar %s", cmd)
            self.out(f"[CLI] Erro ao executar o comando: {exc}")

    def request_quit(self):
        """Usado por SIGINT/SIGTERM."""
        self.running = False
        self._lines.put_nowait(None)

    def _start_reader(self, loop):
        def worker():
            while True:
                self._ready.wait()
                self._ready.clear()  # reabilitado após executar o comando (prompt depois da saída)
                try:
                    line = input("PyP2P-Forca> ")
                except EOFError:
                    loop.call_soon_threadsafe(self._lines.put_nowait, None)
                    return
                loop.call_soon_threadsafe(self._lines.put_nowait, line)

        threading.Thread(target=worker, name="stdin-reader", daemon=True).start()

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_quit)
            except (NotImplementedError, RuntimeError):
                pass
        self.out(f"\n=== PyP2P-Forca ===  Peer: {self.network.peer_id}   (digite /help)")
        self._start_reader(loop)
        while self.running:
            line = await self._lines.get()
            if line is None:
                break
            await self.execute(line)
            self._ready.set()
        await self.shutdown()

    async def shutdown(self):
        self.out("\n[CLI] Encerrando...")
        try:
            await self.router.shutdown()  # LEAVE (jogador) ou GAME_OVER ABORTED (host)
        except Exception as exc:
            log.warning("Erro ao encerrar a partida: %s", exc)
        try:
            await self.network.stop()  # BYE/BYE_OK a todos e UNREGISTER
        except Exception as exc:
            log.warning("Erro ao encerrar a rede: %s", exc)
        self.out("[CLI] Encerrado.")
