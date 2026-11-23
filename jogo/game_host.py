"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : jogo/game_host.py (Parte 2)
Papel   : máquina de estados do host (IDLE → OPEN → RUNNING → FINISHED),
          turnos round-robin, timeout de turno, difusão de STATE com seq
          crescente, tratamento de JOIN/LEAVE/GUESS/GAME_LIST e de jogadores
          que caem. O host é a única fonte autoritativa do estado da partida.

Integração: o construtor recebe send_message(peer_id, mensagem), função
(async ou não) que o MessageRouter implementa sobre P2PNetwork.send().
O MessageRouter chama host.handle_message(msg) para GAME_LIST/JOIN/LEAVE/GUESS
e host.handle_player_disconnect(peer_id) quando a rede reporta STALE/GONE.
"""

#==================================================== Anotações ===============================================================
'''
1. Responsabilidades do módulo


Responsabilidade          Descrição

Criação                   Criar uma nova partida e gerar seu identificador
Gerenciamento             Adicionar e remover jogadores
Inicialização             Iniciar uma partida quando o host decidir
Turnos                    Determinar quem pode jogar
Validação                 Verificar se o palpite é válido e se pertence ao jogador da vez
Finalização               Detectar vitória, derrota ou encerramento e comunicar o resultado

Uma observação importante: o host deve manter uma única partida ativa por peer, conforme a restrição básica do trabalho.

'''

#==============================================================================================================================

#==================================================== Bibliotecas =============================================================

import asyncio
import inspect
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable
from .hangman import HangmanGame, normalize_word, validate_word, calculate_word_hash

#==============================================================================================================================

#=================================================== Objetos e Estruturas de Dados =============================================


log = logging.getLogger(__name__)

# -- Estrutura GameStatus --

class GameStatus(Enum):
    IDLE = auto()
    OPEN = auto()
    RUNNING = auto()
    FINISHED = auto()

# -- Estrutura GameSession --

@dataclass
class GameSession:
    game: HangmanGame
    game_id: str
    word: str
    word_hash: str
    status: GameStatus
    host_id: str
    winner: str | None = None
    players: list[str] = field(default_factory=list)
    scores: dict[str, int] = field(default_factory=dict)
    lives: int = 6
    max_players: int = 4
    current_turn: int = 0
    seq: int = 0
    last_guess: dict | None = None
    turn_timeout: float = 30
    result: str | None = None
    disconnected: set[str] = field(default_factory=set)
    turn_started_at: float | None = None  # monotônico; usado por /status

#==============================================================================================================================

#=================================================== Classe GameHost =========================================================

#Essa é a classe responsável pela administração da partida.
#Host autoritativo; todas as mutações assíncronas são serializadas por lock."
class GameHost:

    def __init__(self, config: dict, host_id: str, send_message: Callable):
        self.config = config or {}
        self.host_id = host_id
        self.send_message = send_message
        self.session: GameSession | None = None
        self.turn_task: asyncio.Task | None = None
        self.finished_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._turn_generation = 0

    # Retorna o estado atual do host. Se não houver sessão, considera que a máquina de estados está em IDLE
    @property
    def status(self) -> GameStatus:
        return self.session.status if self.session else GameStatus.IDLE
    
    # Busca uma configuração primeiro no nível principal de self.config, depois dentro de self.config["game"]. Se não existir, retorna default
    def _config_value(self, key, default):
        game_config = self.config.get('game', {})
        return self.config.get(key, game_config.get(key, default))

    # Constrói o dicionário de uma mensagem do protocolo, incluindo tipo, identificador, TTL, origem, destino e campos adicionais.
    def _message(self, peer_id: str, kind: str, msg_id: str | None = None, **fields) -> dict:
        return {
            'type': kind, 
            'msg_id': msg_id or str(uuid.uuid4()),
            'ttl': 1, 
            'src': self.host_id, 
            'dst': peer_id, 
            **fields
        }
    # Envia uma mensagem a um jogador utilizando a função de rede recebida no construtor
    async def _send(self, peer_id: str, kind: str, msg_id: str | None = None, **fields):
        payload = self._message(peer_id, kind, msg_id, **fields)
        result = self.send_message(peer_id, payload)
        if inspect.isawaitable(result):
            await result
        return payload
    # Distribui uma mensagem para vários jogadores
    async def _broadcast(self, kind: str, recipients: list[str] | None = None, **fields):
        if recipients is None:
            recipients = list(self.session.players) if self.session else []
        for peer_id in recipients:
            try:
                await self._send(peer_id, kind, **fields)
            except Exception:
                log.exception('Falha ao enviar %s para %s', kind, peer_id)
    
    # Função que cria uma partida
    def create_game(self, word: str, max_players: int | None = None, lives: int | None = None) -> GameSession:
        
        #Transição IDLE -> OPEN. Chamada pelo CLI /create."""
        # Uma partida FINISHED (visível por 60 s em GAME_LIST_OK) pode ser substituída.
        if self.session is not None and self.session.status != GameStatus.FINISHED:
            raise RuntimeError('Já existe partida OPEN ou RUNNING.')
        if self.finished_task is not None:
            self.finished_task.cancel()
            self.finished_task = None
        word = normalize_word(word)
        if not validate_word(word):
            raise ValueError('Palavra inválida: use de 3 a 20 letras a-z.')
        lives = int(lives if lives is not None else self._config_value('lives', 6))
        max_players = int(max_players if max_players is not None
                          else self._config_value('max_players', 4))
        timeout = float(self._config_value('turn_timeout', 30))
        if lives < 1 or max_players < 1 or timeout <= 0:
            raise ValueError('lives, max_players e turn_timeout devem ser positivos.')
        game = HangmanGame(word, lives)
        self.session = GameSession(
            game = game, 
            game_id = f'{self.host_id}/{uuid.uuid4().hex[:8]}',
            word = word, 
            word_hash = calculate_word_hash(word),
            status = GameStatus.OPEN, 
            host_id=self.host_id,
            lives=lives, 
            max_players = max_players, 
            turn_timeout = timeout
        )
        log.info('[Host] Game %s created (word_length=%d, lives=%d)',
                 self.session.game_id, len(word), lives)
        return self.session
    # Retorna as informações públicas da partida para divulgação na rede, como identificador, estado, jogadores, vidas e limite de participantes. 
    # Não revela a palavra secreta.
    def get_game_info(self) -> dict | None:
        sessao = self.session
        if sessao is None:
            return None
        return {
            'game_id': sessao.game_id, 
            'host': self.host_id,
            'status': sessao.status.name, 
            'players': list(sessao.players),
            'max_players': sessao.max_players, 
            'lives': sessao.game.remaining_lives,
            'word_length': len(sessao.word),
        }

    # Adiciona um jogador à partida enquanto ela estiver OPEN
    def add_player(self, peer_id: str) -> dict:
        sessao = self.session
        if sessao is None:
            return {'success': False, 'reason': 'unknown_game'}
        if sessao.status == GameStatus.RUNNING:
            return {'success': False, 'reason': 'game_running'}
        if sessao.status == GameStatus.FINISHED:
            return {'success': False, 'reason': 'game_finished'}
        if peer_id in sessao.players:
            return {'success': False, 'reason': 'already_joined'}
        if peer_id == self.host_id or len(sessao.players) >= sessao.max_players:
            return {'success': False, 'reason': 'game_full'}
        sessao.players.append(peer_id)
        sessao.scores[peer_id] = 0
        log.info('[Host] %s joined (%d/%d)', peer_id, len(sessao.players), sessao.max_players)
        return {'success': True}

    # Retorna o identificador do jogador que possui o turno atual
    def get_current_player(self) -> str | None:
        sessao = self.session
        if sessao is None or not sessao.players or sessao.status != GameStatus.RUNNING:
            return None
        return sessao.players[sessao.current_turn]

    # Avança o turno para o próximo jogador utilizando Round Robin
    def next_turn(self) -> str | None:
        sessao = self.session
        if sessao is None or not sessao.players:
            return None
        sessao.current_turn = (sessao.current_turn + 1) % len(sessao.players)
        return sessao.players[sessao.current_turn]

    # Cancela o temporizador do turno anterior
    def _cancel_turn_timer(self):
        self._turn_generation += 1
        task = self.turn_task
        self.turn_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    # Reinicia a contagem de tempo do turno atual
    def _restart_turn_timer(self):
        self._cancel_turn_timer()
        if self.status == GameStatus.RUNNING and self.session.players:
            generation = self._turn_generation
            self.session.turn_started_at = time.monotonic()
            self.turn_task = asyncio.create_task(self.start_turn_timer(generation))
    # Aguarda o tempo máximo permitido para uma jogada
    async def start_turn_timer(self, generation: int | None = None):
        #Espera um turno; um token evita que timers antigos alterem o jogo.
        if generation is None:
            generation = self._turn_generation
        try:
            await asyncio.sleep(self.session.turn_timeout)
            async with self._lock:
                if (self.status == GameStatus.RUNNING and generation == self._turn_generation):
                    await self._handle_turn_timeout_locked()
        except asyncio.CancelledError:
            return
    # Processa internamente o timeout de um turno
    async def _handle_turn_timeout_locked(self):
        sessao = self.session
        if not sessao or sessao.status != GameStatus.RUNNING or not sessao.players:
            return
        timed_out = self.get_current_player()
        log.info('[Host] Turn timeout for %s — skipping', timed_out)
        sessao.last_guess = {'player': timed_out, 'timeout': True}
        self.next_turn()
        await self.broadcast_state()
        self._restart_turn_timer()

    # Permite disparar o tratamento de timeout externamente, adquirindo o lock antes de alterar o estado da partida.
    async def handle_turn_timeout(self):
        async with self._lock:
            await self._handle_turn_timeout_locked()

    # Constrói o dicionário STATE com as informações atuais da partida
    def build_state(self) -> dict:
        sessao = self.session
        if sessao is None:
            raise RuntimeError('Nenhuma partida ativa.')
        return {
            'game_id': sessao.game_id, 'seq': sessao.seq,
            'masked': sessao.game.get_masked_word(),
            'wrong_letters': list(sessao.game.get_wrong_letters()),
            'lives': sessao.game.remaining_lives,
            'turn': self.get_current_player(),
            'players': list(sessao.players),
            'last_guess': sessao.last_guess.copy() if sessao.last_guess else None
        }

    # Incrementa o número de sequência (seq) e envia o STATE
    async def broadcast_state(self):
        sessao = self.session
        if sessao is None or sessao.status != GameStatus.RUNNING:
            return
        sessao.seq += 1
        log.info('[Host] STATE seq=%d masked=%s lives=%d turn=%s (timeout %ss)', sessao.seq,
                 sessao.game.get_masked_word(), sessao.game.remaining_lives,
                 self.get_current_player(), sessao.turn_timeout)
        await self._broadcast('STATE', **self.build_state())

    # Envia GAME_START aos jogadores, informando os dados iniciais da partida, incluindo o hash da palavra e o tempo de turno
    async def broadcast_game_start(self):
        sessao = self.session
        await self._broadcast(
            'GAME_START', 
            game_id = sessao.game_id, 
            players = list(sessao.players),
            lives = sessao.lives, 
            word_length = len(sessao.word), 
            word_hash = sessao.word_hash,
            # Especificação: inteiro de segundos (30.0 -> 30).
            turn_timeout = int(sessao.turn_timeout) if float(sessao.turn_timeout).is_integer() else sessao.turn_timeout
        )
    # Inicia uma partida que esteja OPEN e possua pelo menos um jogador
    async def start_game(self) -> bool:
        #Transição OPEN -> RUNNING; GAME_START, STATE seq=1, timer.
        async with self._lock:
            sessao = self.session
            if sessao is None or sessao.status != GameStatus.OPEN or not sessao.players:
                return False
            sessao.status = GameStatus.RUNNING
            sessao.current_turn = 0
            sessao.word_hash = calculate_word_hash(sessao.word)
            await self.broadcast_game_start()
            await self.broadcast_state()
            self._restart_turn_timer()
            return True

    # Verifica se uma jogada pode ser aceita
    def validate_guess(self, peer_id: str, guess: dict) -> str | None:
        #None = válido; caso contrário retorna código de rejeição.
        sessao = self.session
        if sessao is None or sessao.status != GameStatus.RUNNING:
            return 'game_not_running'
        if peer_id != self.get_current_player():
            return 'not_your_turn'
        has_letter = 'letter' in guess
        has_word = 'word' in guess
        if has_letter == has_word:
            return 'invalid_letter' if has_letter else 'invalid_word'
        if has_letter:
            letter = guess['letter']
            if not isinstance(letter, str):
                return 'invalid_letter'
            letter = normalize_word(letter)
            if not re.fullmatch(r'[a-z]', letter):
                return 'invalid_letter'
            if letter in sessao.game.used_letters:
                return 'letter_already_used'
        else:
            word = guess['word']
            if not isinstance(word, str) or not validate_word(normalize_word(word)):
                return 'invalid_word'
        return None

    # Acrescenta os pontos obtidos ao placar do jogador.
    def update_score(self, peer_id: str, points: int):
        if self.session and peer_id in self.session.scores:
            self.session.scores[peer_id] += points

    # Consulta a lógica da forca para verificar se houve vitória ou derrota. 
    def check_game_over(self) -> str | None:
        sessao = self.session
        if not sessao:
            return None
        if sessao.game.is_won():
            return 'WON'
        if sessao.game.is_lost():
            return 'LOST'
        return None


    # Processa uma jogada recebida de um jogador
    # Valida, pontua, envia GUESS_RESULT e atualiza a partida.
    async def process_guess(self, peer_id: str, guess: dict, msg_id: str | None = None) -> dict:

        async with self._lock:
            sessao = self.session
            reason = self.validate_guess(peer_id, guess)
            game_id = sessao.game_id if sessao else guess.get('game_id')
            if reason:
                response = {'game_id': game_id, 'accepted': False, 'reason': reason}
                await self._send(peer_id, 'GUESS_RESULT', msg_id, **response)
                return response

            is_letter = 'letter' in guess
            value = normalize_word(guess['letter' if is_letter else 'word'])
            before = sessao.game.get_masked_word()
            if is_letter:
                outcome = sessao.game.guess_letter(value)
            else:
                outcome = sessao.game.guess_word(value)
            if not outcome.is_accepted:
                response = {'game_id': sessao.game_id, 'accepted': False,
                            'reason': outcome.reason or 'invalid_word'}
                await self._send(peer_id, 'GUESS_RESULT', msg_id, **response)
                return response

            after = sessao.game.get_masked_word()
            points = sum(a == '_' and b != '_' for a, b in zip(before, after))
            self.update_score(peer_id, points)
            log.info('[Host] GUESS %s %s=%s -> %s, masked=%s', peer_id,
                     'letter' if is_letter else 'word', value,
                     'correct' if outcome.is_correct else 'wrong', after)
            sessao.last_guess = {'player': peer_id,
                                 'letter' if is_letter else 'word': value,
                                 'correct': outcome.is_correct}
            response = {'game_id': sessao.game_id, 'accepted': True,
                        'correct': outcome.is_correct}
            await self._send(peer_id, 'GUESS_RESULT', msg_id, **response)
            terminal = self.check_game_over()
            if terminal:
                if terminal == 'WON':
                    sessao.winner = peer_id
                await self._finish_locked(terminal)
            else:
                self.next_turn()
                await self.broadcast_state()
                self._restart_turn_timer()
            return response

    # Envia GAME_OVER aos jogadores, informando resultado, palavra secreta, vencedor, pontuações e motivo do 
    # encerramento quando o resultado for ABORTED
    async def broadcast_game_over(self, result: str, reason: str | None = None):
        sessao = self.session
        if not sessao:
            return
        fields = {
            'game_id': sessao.game_id, 'result': result, 'word': sessao.word,
            'winner': sessao.winner if result == 'WON' else None,
            'scores': dict(sessao.scores),
        }
        if result == 'ABORTED':
            fields['reason'] = reason or 'host_shutdown'
        await self._broadcast('GAME_OVER', **fields)
    
    # Centraliza o encerramento da partida: cancela o temporizador, registra o resultado, altera o estado para FINISHED e envia GAME_OVER 
    async def _finish_locked(self, result: str, reason: str | None = None, immediate_idle: bool = False):
        sessao = self.session
        if not sessao or sessao.status == GameStatus.FINISHED:
            return
        self._cancel_turn_timer()
        sessao.result = result
        sessao.status = GameStatus.FINISHED
        await self.broadcast_game_over(result, reason)
        log.info('[Host] Game %s finished: %s%s', sessao.game_id, result,
                 f' ({reason})' if reason else '')
        if self.finished_task is not None:
            self.finished_task.cancel()
        if immediate_idle:
            self.session = None
        else:
            self.finished_task = asyncio.create_task(self._finished_expiry(sessao))

    # Aguarda 60 segundos após o término da partida e remove a sessão encerrada, retornando a máquina de estados para IDLE
    async def _finished_expiry(self, finished_session: GameSession):
        try:
            await asyncio.sleep(60)
            async with self._lock:
                if self.session is finished_session and self.status == GameStatus.FINISHED:
                    self.session = None
        except asyncio.CancelledError:
            return

    # Cancela uma partida OPEN ou RUNNING, registrando o resultado ABORTED
    async def cancel_game(self, reason: str = 'host_shutdown') -> bool:
        #OPEN/RUNNING -> FINISHED por 60s, via GAME_OVER ABORTED.
        async with self._lock:
            if self.status not in (GameStatus.OPEN, GameStatus.RUNNING):
                return False
            await self._finish_locked('ABORTED', reason)
            return True

    # Remove um jogador da sessão, preservando sua pontuação. Ajusta o índice do turno e distribui STATE se a partida
    # estiver RUNNING. Se não restarem jogadores, encerra a partida com resultado ABORTED e motivo no_players.
    async def _remove_player_locked(self, peer_id: str) -> bool:
        sessao = self.session
        if not sessao or peer_id not in sessao.players or sessao.status not in (GameStatus.OPEN, GameStatus.RUNNING):
            return False
        was_running = sessao.status == GameStatus.RUNNING
        idx = sessao.players.index(peer_id)
        old_turn = sessao.current_turn
        sessao.players.pop(idx)
        sessao.disconnected.discard(peer_id)
        if not was_running:
            # Partida ainda OPEN: quem sai não pontuou; a partida segue aberta
            # (máquina de estados: OPEN --LEAVE--> OPEN), mesmo sem jogadores.
            sessao.scores.pop(peer_id, None)
            return True
        # Partida RUNNING: scores são preservados para o placar final.
        if not sessao.players:
            await self._finish_locked('ABORTED', 'no_players', immediate_idle=True)
            return True
        if was_running:
            if idx < old_turn:
                sessao.current_turn -= 1
            elif idx == old_turn and old_turn >= len(sessao.players):
                sessao.current_turn = 0
            await self.broadcast_state()
            if idx == old_turn:
                self._restart_turn_timer()
        return True
    # Interface pública para remover um jogador da partida
    async def remove_player(self, peer_id: str) -> bool:
        async with self._lock:
            return await self._remove_player_locked(peer_id)

    # Trata a desconexão inesperada de um jogador, identificada pelo módulo de conexão ou KeepAlive. Remove o jogador,
    # atualiza a partida e registra sua desconexão para permitir uma possível reconexão durante RUNNING.
    # Chamado pelo módulo de conexões após detectar STALE/queda de socket.
    async def handle_player_disconnect(self, peer_id: str):
    
        async with self._lock:
            sessao = self.session
            if not sessao or peer_id not in sessao.players:
                return
            log.warning('[Host] Player %s STALE — removed from game', peer_id)
            sessao.disconnected.add(peer_id)
            await self._remove_player_locked(peer_id)
            # Marca como conhecido para permitir JOIN rejoin:true em RUNNING.
            if self.session is sessao and sessao.status == GameStatus.RUNNING:
                sessao.disconnected.add(peer_id)

    # Processa uma solicitação JOIN. Verifica se a partida existe, se permite entrada e se há vagas.
    async def handle_join(self, message: dict) -> dict:
        async with self._lock:
            peer_id = message['src']
            sessao = self.session
            game_id = message.get('game_id')
            rejoin = False
            if not sessao or sessao.game_id != game_id:
                reason = 'unknown_game'
            elif sessao.status == GameStatus.FINISHED:
                reason = 'game_finished'
            elif sessao.status == GameStatus.RUNNING:
                if peer_id in sessao.disconnected and peer_id in sessao.scores:
                    # O jogador reconectado entra no fim do rodízio.
                    if len(sessao.players) >= sessao.max_players:
                        reason = 'game_full'
                    else:
                        sessao.disconnected.remove(peer_id)
                        sessao.players.append(peer_id)
                        rejoin = True
                        reason = None
                elif peer_id in sessao.players:
                    # JOIN idempotente do jogador já inscrito.
                    rejoin = True
                    reason = None
                else:
                    reason = 'game_running'
            else:
                added = self.add_player(peer_id)
                reason = added.get('reason') if not added['success'] else None
            if reason:
                response = {'game_id': game_id, 'reason': reason}
                await self._send(peer_id, 'JOIN_ERR', message.get('msg_id'), **response)
                return {'type': 'JOIN_ERR', **response}
            response = {'game_id': game_id, 'players': list(sessao.players), 'rejoin': rejoin}
            await self._send(peer_id, 'JOIN_OK', message.get('msg_id'), **response)
            if rejoin and sessao.status == GameStatus.RUNNING:
                # Reenvio do STATE corrente sem incrementar seq.
                await self._send(peer_id, 'STATE', **self.build_state())
                if peer_id == self.get_current_player():
                    self._restart_turn_timer()
            return {'type': 'JOIN_OK', **response}

    # Processa uma solicitação LEAVE
    async def handle_leave(self, message: dict):
        peer_id = message['src']
        async with self._lock:
            sessao = self.session
            if not sessao or sessao.game_id != message.get('game_id') or peer_id not in sessao.players:
                await self._send(peer_id, 'ERROR', ref_msg_id=message.get('msg_id'),
                                 code='invalid_state', message='Jogador não inscrito nesta partida')
                return False
            await self._send(peer_id, 'LEAVE_OK', message.get('msg_id'), game_id=sessao.game_id)
            return await self._remove_player_locked(peer_id)

    # Recebe uma mensagem GUESS, verifica o identificador da partida e encaminha o palpite para process_guess()
    async def handle_guess(self, message: dict):
        peer_id = message['src']
        sessao = self.session
        if sessao is not None and message.get('game_id') != sessao.game_id:
            await self._send(peer_id, 'GUESS_RESULT', message.get('msg_id'), game_id = message.get('game_id'), accepted = False,
                             reason='game_not_running')
            return
        return await self.process_guess(peer_id, message, message.get('msg_id'))

    # Responde a uma solicitação GAME_LIST com GAME_LIST_OK
    async def handle_game_list(self, message: dict):
        game = self.get_game_info()
        response = {'games': [game] if game else []}
        await self._send(message['src'], 'GAME_LIST_OK', message.get('msg_id'), **response)
        return response

    # Atua como despachante das mensagens recebidas da rede
    async def handle_message(self, message: dict):
        #Despachante de eventos de rede para a máquina de estados.
        if message.get('ttl') != 1:
            return None
        if message.get('dst') != self.host_id:
            if message.get('src'):
                await self._send(message['src'], 'ERROR', ref_msg_id=message.get('msg_id'), code='not_for_me', message ='Destino diferente deste peer')
            return None
        kind = message.get('type')
        handlers = {
            'GAME_LIST': self.handle_game_list,
            'JOIN': self.handle_join,
            'LEAVE': self.handle_leave,
            'GUESS': self.handle_guess,
        }
        handler = handlers.get(kind)
        if handler is None:
            if message.get('src'):
                await self._send(message['src'], 'ERROR', ref_msg_id=message.get('msg_id'), code='unknown_type', message=f'Tipo não suportado: {kind}')
            return None
        if not message.get('src') or not message.get('msg_id'):
            return None
        return await handler(message)

    # Realiza o desligamento do host
    async def shutdown(self):
        #Use antes de fechar as conexões TCP do host
        await self.cancel_game('host_shutdown')
        self._cancel_turn_timer()
        if self.finished_task is not None:
            self.finished_task.cancel()
#==============================================================================================================================