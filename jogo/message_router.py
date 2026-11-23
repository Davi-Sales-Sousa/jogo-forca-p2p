"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]
  
Módulo  : jogo/message_router.py (integração das Partes 1, 2 e 3)
Papel   : despacha as mensagens de jogo recebidas de P2PNetwork:
            * GAME_LIST, JOIN, LEAVE, GUESS  -> GameHost (quem hospeda);
            * GAME_LIST_OK, JOIN_OK, JOIN_ERR, LEAVE_OK, GAME_START, STATE,
              GUESS_RESULT, GAME_OVER        -> GamePlayer (quem joga).
          Também traduz eventos de conexão (STALE/GONE) em remoção de jogador
          (host) ou aborto local da partida (jogador) e implementa as operações
          usadas pela CLI (/games, /join, /leave, /guess, ...).

Decisão de projeto: o roteador não conhece regras da forca; só liga a camada de
rede (rede/) à lógica de partida (game_host.py, game_player.py).
"""

import asyncio
import inspect
import logging
import uuid

from .game_host import GameHost, GameStatus
from .game_player import GamePlayer

log = logging.getLogger("jogo.router")

HOST_TYPES = frozenset({"GAME_LIST", "JOIN", "LEAVE", "GUESS"})
# Campos do envelope que P2PNetwork.send() preenche sozinho.
ENVELOPE_FIELDS = frozenset({"type", "msg_id", "ttl", "src", "dst"})


class MessageRouter:
    """Liga P2PNetwork, GameHost e GamePlayer.

    event_handler(evento, dados): callback (síncrono ou async) usado pela CLI
    para exibir eventos do lado jogador (JOIN_OK, STATE, GAME_OVER, ...).
    """

    def __init__(self, network, config: dict | None = None, event_handler=None):
        self.network = network
        self.event_handler = event_handler
        self.player = GamePlayer(network.peer_id)
        # O GameHost não conhece a rede: recebe a função de envio abaixo.
        self.host = GameHost(config or {}, network.peer_id, self._host_send)
        self._pending_lists: dict[str, asyncio.Future] = {}  # msg_id -> Future[list]
        self._leave_done = asyncio.Event()
        network.message_handler = self.handle_message
        network.peer_handler = self.handle_peer_event

    async def _notify(self, event: str, **data):
        if self.event_handler is None:
            return
        try:
            result = self.event_handler(event, data)
            if inspect.isawaitable(result):
                await result
        except Exception:
            log.exception("Erro ao notificar evento %s", event)

    async def _host_send(self, peer_id: str, payload: dict):
        """Adaptador GameHost -> P2PNetwork.send().

        O GameHost monta o envelope completo; a rede o monta de novo, por isso
        os campos de envelope são removidos e o msg_id é preservado (respostas
        devem repetir o msg_id da requisição).
        """
        fields = {k: v for k, v in payload.items() if k not in ENVELOPE_FIELDS}
        try:
            await self.network.send(peer_id, payload["type"],
                                    msg_id=payload.get("msg_id"), **fields)
        except (ConnectionError, ValueError, OSError) as exc:
            # Não propaga: falha em um jogador não pode interromper a difusão
            # aos demais nem deixar o estado do host inconsistente.
            log.warning("[Host] falha ao enviar %s a %s: %s", payload["type"], peer_id, exc)

    # ------------------------------------------------------------------
    # Mensagens recebidas
    # ------------------------------------------------------------------
    async def handle_message(self, peer_id: str, message: dict):
        kind = message.get("type")
        player_handlers = {
            "GAME_LIST_OK": self._on_game_list_ok, "JOIN_OK": self._on_join_ok,
            "JOIN_ERR": self._on_join_err, "LEAVE_OK": self._on_leave_ok,
            "GAME_START": self._on_game_start, "STATE": self._on_state,
            "GUESS_RESULT": self._on_guess_result, "GAME_OVER": self._on_game_over,
        }
        try:
            if kind in HOST_TYPES:
                await self.host.handle_message(message)
            elif kind in player_handlers:
                await player_handlers[kind](peer_id, message)
            else:
                log.warning("Tipo de jogo sem tratador: %s", kind)
        except Exception:
            log.exception("Erro ao processar %s de %s", kind, peer_id)

    async def _on_game_list_ok(self, peer_id, message):
        games = message.get("games")
        if not isinstance(games, list):
            log.warning("[Player] GAME_LIST_OK inválido de %s", peer_id)
            return
        fut = self._pending_lists.get(message.get("msg_id"))
        if fut is not None and not fut.done():
            fut.set_result(games)

    async def _on_join_ok(self, peer_id, message):
        if not self.player.handle_join_ok(peer_id, message):
            log.warning("[Player] JOIN_OK ignorado de %s", peer_id)
            return
        log.info("[Player] joined %s (players=%s, rejoin=%s)", self.player.game_id,
                 self.player.players, self.player.rejoin)
        await self._notify("JOIN_OK", peer_id=peer_id, game_id=self.player.game_id,
                           players=self.player.players.copy(), rejoin=self.player.rejoin)

    async def _on_join_err(self, peer_id, message):
        reason = message.get("reason")
        if not self.player.handle_join_err(peer_id, message):
            return
        log.info("[Player] JOIN_ERR from %s: %s", peer_id, reason)
        await self._notify("JOIN_ERR", peer_id=peer_id, reason=reason)

    async def _on_leave_ok(self, peer_id, message):
        game_id = self.player.game_id
        if not self.player.handle_leave_ok(peer_id, message):
            return
        self._leave_done.set()
        log.info("[Player] left %s", game_id)
        await self._notify("LEAVE_OK", peer_id=peer_id, game_id=game_id)

    async def _on_game_start(self, peer_id, message):
        if not self.player.handle_game_start(peer_id, message):
            log.warning("[Player] GAME_START inválido de %s", peer_id)
            return
        log.info("[Player] GAME_START %s players=%s lives=%s word_length=%s",
                 self.player.game_id, self.player.players, self.player.lives,
                 self.player.word_length)
        await self._notify("GAME_START", peer_id=peer_id, game_id=self.player.game_id,
                           players=self.player.players.copy(), lives=self.player.lives,
                           word_length=self.player.word_length,
                           turn_timeout=self.player.turn_timeout)

    async def _on_state(self, peer_id, message):
        previous = self.player.seq
        if not self.player.apply_state(peer_id, message):
            seq = message.get("seq")
            if isinstance(seq, int) and seq <= previous and self.player.status == "PLAYING":
                log.debug("[Player] STATE seq=%s ignored (already at seq=%s)", seq, previous)
            else:
                log.debug("[Player] STATE seq=%s descartado (inválido ou fora de contexto)", seq)
            return
        st = self.player.get_state()
        log.info("[Player] STATE seq=%s applied: %s lives=%s turn=%s", st["seq"], st["masked"],
                 st["lives"], st["turn"])
        await self._notify("STATE", peer_id=peer_id, state=st)

    async def _on_guess_result(self, peer_id, message):
        if not self.player.handle_guess_result(peer_id, message):
            return
        await self._notify("GUESS_RESULT", peer_id=peer_id, game_id=self.player.game_id,
                           result=self.player.last_guess_result.copy())

    async def _on_game_over(self, peer_id, message):
        if not self.player.handle_game_over(peer_id, message):
            log.warning("[Player] GAME_OVER inválido de %s", peer_id)
            return
        result = self.player.last_game_result.copy()
        if result["hash_matches"] is False:
            log.warning("[Player] host cheated: hash mismatch")
        hash_txt = {True: "hash OK", False: "hash MISMATCH", None: "hash not verified"}[result["hash_matches"]]
        log.info("[Player] GAME_OVER %s word=%s winner=%s — %s", result["result"], result["word"],
                 result["winner"], hash_txt)
        await self._notify("GAME_OVER", peer_id=peer_id, result=result)

    # ------------------------------------------------------------------
    # Eventos de conexão vindos de P2PNetwork
    # ------------------------------------------------------------------
    async def handle_peer_event(self, event: str, peer_id: str):
        if event not in ("STALE", "GONE"):
            return
        # Host: jogador caiu -> remove da partida e difunde STATE.
        await self.host.handle_player_disconnect(peer_id)
        # Jogador: host caiu -> aborta localmente e volta a IDLE (comportamento
        # mínimo da especificação; sem migração de host).
        if self.player.status in ("JOINED", "PLAYING") and self.player.host_peer_id == peer_id:
            game_id = self.player.game_id
            self.player.handle_host_unreachable(peer_id)
            self._leave_done.set()
            log.warning("[Game] Host %s unreachable — game aborted", peer_id)
            await self._notify("HOST_UNREACHABLE", peer_id=peer_id, game_id=game_id,
                               result=self.player.last_game_result.copy())

    # ------------------------------------------------------------------
    # Operações usadas pela CLI
    # ------------------------------------------------------------------
    async def request_games(self, timeout: float = 3.0) -> list[dict]:
        """/games: GAME_LIST a todos os vizinhos conectados e consolida as respostas."""
        futures = {}
        for peer_id in list(self.network.connections):
            fut = await self._send_game_list(peer_id)
            if fut is not None:
                futures[peer_id] = fut
        games = []
        if futures:
            await asyncio.wait(futures.values(), timeout=timeout)
        for peer_id, fut in futures.items():
            if fut.done() and not fut.cancelled():
                games.extend(g for g in fut.result() if isinstance(g, dict))
            else:
                log.info("[CLI] sem resposta de %s ao GAME_LIST", peer_id)
        self._pending_lists = {k: v for k, v in self._pending_lists.items() if not v.done()}
        return games

    async def _send_game_list(self, peer_id: str):
        msg_id = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending_lists[msg_id] = fut  # registrado ANTES de enviar: a resposta pode ser imediata
        try:
            await self.network.send(peer_id, "GAME_LIST", msg_id=msg_id)
        except (ConnectionError, ValueError, OSError) as exc:
            log.warning("[CLI] GAME_LIST para %s falhou: %s", peer_id, exc)
            self._pending_lists.pop(msg_id, None)
            return None
        return fut

    def _ensure_idle(self):
        if self.player.status != "IDLE":
            raise RuntimeError("já está em uma partida como jogador")
        if self.host.status in (GameStatus.OPEN, GameStatus.RUNNING):
            raise RuntimeError("já está hospedando uma partida")

    async def join_host(self, host_peer_id: str, game_id: str | None = None):
        """/join <host_peer_id>: descobre o game_id via GAME_LIST e envia JOIN."""
        self._ensure_idle()
        if game_id is None:
            fut = await self._send_game_list(host_peer_id)
            if fut is None:
                raise ConnectionError(f"peer não conectado: {host_peer_id}")
            try:
                games = await asyncio.wait_for(fut, 3.0)
            except asyncio.TimeoutError:
                raise ConnectionError(f"{host_peer_id} não respondeu ao GAME_LIST") from None
            usable = [g for g in games if isinstance(g, dict) and g.get("host") == host_peer_id
                      and g.get("status") in ("OPEN", "RUNNING")]
            if not usable:
                raise RuntimeError(f"{host_peer_id} não tem partida aberta")
            game_id = usable[0]["game_id"]
        await self.join_game(host_peer_id, game_id)
        return game_id

    async def join_game(self, host_peer_id: str, game_id: str):
        self._ensure_idle()
        self.player.begin_join(host_peer_id, game_id)
        try:
            await self.network.send(host_peer_id, "JOIN", game_id=game_id)
        except (ConnectionError, ValueError, OSError) as exc:
            self.player.reset()
            raise ConnectionError(f"host inacessível: {exc}") from exc

    async def leave_game(self, wait: float = 2.0):
        if self.player.status not in ("JOINED", "PLAYING"):
            raise RuntimeError("não está em uma partida")
        self._leave_done.clear()
        try:
            await self.network.send(self.player.host_peer_id, "LEAVE", game_id=self.player.game_id)
        except (ConnectionError, ValueError, OSError) as exc:
            raise ConnectionError(f"host inacessível: {exc}") from exc
        try:
            await asyncio.wait_for(self._leave_done.wait(), wait)
        except asyncio.TimeoutError:
            log.warning("[Player] LEAVE_OK não recebido em %.1fs", wait)

    async def send_guess(self, *, letter: str | None = None, word: str | None = None):
        data = self.player.build_guess(letter=letter, word=word)  # valida turno/estado localmente
        try:
            return await self.network.send(self.player.host_peer_id, "GUESS", **data)
        except (ConnectionError, ValueError, OSError) as exc:
            raise ConnectionError(f"host inacessível: {exc}") from exc

    def create_game(self, word: str, max_players: int | None = None, lives: int | None = None):
        if self.player.status != "IDLE":
            raise RuntimeError("já está em uma partida como jogador")
        return self.host.create_game(word, max_players, lives)

    async def start_game(self) -> bool:
        return await self.host.start_game()

    async def cancel_game(self) -> bool:
        return await self.host.cancel_game("host_shutdown")

    async def shutdown(self):
        """Antes de P2PNetwork.stop(): jogador envia LEAVE; host envia GAME_OVER ABORTED."""
        if self.player.status in ("JOINED", "PLAYING"):
            try:
                await self.leave_game()
            except Exception as exc:
                log.warning("[Player] LEAVE no encerramento falhou: %s", exc)
        await self.host.shutdown()
