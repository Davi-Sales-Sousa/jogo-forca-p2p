"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : tests/test_integracao.py
Papel   : testes de integração das três partes sobre TCP real em 127.0.0.1,
          contra o Rendezvous de teste (tests/fake_rendezvous.py). Cobrem os
          cenários mínimos da especificação (1–10) que podem ser executados
          em um único processo. NÃO substituem a interoperabilidade (cenário
          11) com pyp2p-forca/forca-bot nem o teste com o Rendezvous público.
"""
import asyncio
import json
import logging
import socket
import time
import unittest

from jogo.message_router import MessageRouter
from rede import NetworkConfig, P2PNetwork
from tests.fake_rendezvous import FakeRendezvous

logging.disable(logging.CRITICAL)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def until(cond, timeout=8.0, step=0.05):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return True
        await asyncio.sleep(step)
    return cond()


class Node:
    def __init__(self, rdv, name, port=None, ping=0.4, turn_timeout=30, **cfg):
        self.name = name
        self.port = port or free_port()
        self.events = []
        self.net = P2PNetwork(NetworkConfig(
            name=name, namespace="CIC", listen_host="127.0.0.1", listen_port=self.port,
            rdv_host="127.0.0.1", rdv_port=rdv.port, discover_interval=0.5,
            ping_interval=ping, base_backoff=0.2, max_backoff=1, **cfg))
        self.router = MessageRouter(self.net, {"turn_timeout": turn_timeout, "lives": 6},
                                    event_handler=lambda e, d: self.events.append((e, d)))
        self.pid = self.net.peer_id

    async def start(self):
        await self.net.start()
        return self

    def kinds(self):
        return [e for e, _ in self.events]

    def last(self, kind):
        return next((d for e, d in reversed(self.events) if e == kind), None)

    async def kill(self):
        """Simula kill -9: fecha sockets sem BYE/UNREGISTER e para as tarefas."""
        n = self.net
        n.running = False
        for t in n._tasks:
            t.cancel()
        for c in list(n.connections.values()):
            c.writer.transport.abort()
        if n.server:
            n.server.close()
        await self.router.host.shutdown() if False else None


class Base(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.rdv = await FakeRendezvous().start()
        self.nodes = []

    async def asyncTearDown(self):
        for n in self.nodes:
            try:
                await n.router.host.shutdown()
                await n.net.stop()
            except Exception:
                pass
        await self.rdv.stop()

    async def node(self, name, **kw):
        n = Node(self.rdv, name, **kw)
        self.nodes.append(n)
        return await n.start()

    async def mesh(self, names, **kw):
        nodes = [await self.node(n, **kw) for n in names]
        ok = await until(lambda: all(len(n.net.connections) == len(nodes) - 1 for n in nodes), 10)
        self.assertTrue(ok, [len(n.net.connections) for n in nodes])
        return nodes

    async def lobby(self, word="banana", **kw):
        alice, bob, carol = await self.mesh(["alice", "bob", "carol"], **kw)
        alice.router.create_game(word, 3)
        await bob.router.join_host("alice@CIC")
        await until(lambda: "JOIN_OK" in bob.kinds())
        await carol.router.join_host("alice@CIC")
        await until(lambda: "JOIN_OK" in carol.kinds())
        return alice, bob, carol

    async def play(self, alice, bob, carol):
        self.assertTrue(await alice.router.start_game())
        self.assertTrue(await until(lambda: bob.router.player.turn is not None and carol.router.player.turn is not None))

    async def guess(self, node, **kw):
        before = node.router.player.seq
        await node.router.send_guess(**kw)
        await until(lambda: node.last("GUESS_RESULT") is not None and
                    (node.router.player.seq > before or node.router.player.status != "PLAYING"))


class T1_Malha(Base):
    async def test_malha_sem_duplicadas_e_rtt(self):
        nodes = await self.mesh(["alice", "bob", "carol"])
        await asyncio.sleep(1.5)
        for n in nodes:
            st = n.net.connections_status()
            self.assertEqual(len(st), 2)
            self.assertEqual(len({s["peer_id"] for s in st}), 2)
        self.assertTrue(await until(lambda: all(s["rtt_ms"] is not None for n in nodes
                                                for s in n.net.connections_status()), 5))

    async def test_filtra_proprio_peer_no_discover(self):
        a, b = await self.mesh(["alice", "bob"])
        peers = await a.net.discover_once()
        self.assertEqual({f"{p['name']}@{p['namespace']}" for p in peers}, {"bob@CIC"})

    async def test_refresh_de_ttl(self):
        a = await self.node("alice", register_ttl=2)
        n0 = self.rdv.requests.count("REGISTER")
        await asyncio.sleep(2.6)
        self.assertGreater(self.rdv.requests.count("REGISTER"), n0)


class T2_Partidas(Base):
    async def test_vitoria_scores_e_hash(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        self.assertEqual(bob.router.player.turn, "bob@CIC")
        await self.guess(bob, letter="a")      # bob: 3 pontos
        await self.guess(carol, letter="n")    # carol: 2 pontos
        await self.guess(bob, word="banana")   # bob acerta a palavra: +1 (letra 'b' restante)
        self.assertTrue(await until(lambda: bob.last("GAME_OVER") and carol.last("GAME_OVER")))
        r = bob.last("GAME_OVER")["result"]
        self.assertEqual((r["result"], r["word"], r["winner"]), ("WON", "banana", "bob@CIC"))
        self.assertTrue(r["hash_matches"])
        self.assertEqual(r["scores"], {"bob@CIC": 4, "carol@CIC": 2})

    async def test_derrota(self):
        alice, bob, carol = await self.lobby("casa")
        alice.router.host.session.game.remaining_lives = 2  # atalho: forca curta
        await self.play(alice, bob, carol)
        await self.guess(bob, letter="x")
        await self.guess(carol, letter="y")
        self.assertTrue(await until(lambda: bob.last("GAME_OVER")))
        r = bob.last("GAME_OVER")["result"]
        self.assertEqual((r["result"], r["winner"], r["word"]), ("LOST", None, "casa"))

    async def test_fora_do_turno_sem_state(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        seq = carol.router.player.seq
        carol.router.player.turn = "carol@CIC"  # força o cliente a enviar mesmo fora da vez
        await carol.router.send_guess(letter="a")
        self.assertTrue(await until(lambda: carol.last("GUESS_RESULT")))
        self.assertEqual(carol.last("GUESS_RESULT")["result"]["reason"], "not_your_turn")
        await asyncio.sleep(0.4)
        self.assertEqual(alice.router.host.session.seq, seq)  # nenhum STATE novo

    async def test_letra_repetida_palavra_errada_e_certa(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        await self.guess(bob, letter="a")
        await self.guess(carol, letter="z")
        lives = alice.router.host.session.game.remaining_lives
        carol.events.clear(); bob.events.clear()
        await bob.router.send_guess(letter="a")  # repetida (vez de bob)
        self.assertTrue(await until(lambda: bob.last("GUESS_RESULT")))
        self.assertEqual(bob.last("GUESS_RESULT")["result"]["reason"], "letter_already_used")
        await self.guess(bob, word="mamona")  # errada: custa vida
        self.assertEqual(alice.router.host.session.game.remaining_lives, lives - 1)
        await self.guess(carol, word="banana")  # certa (agora é a vez de carol)
        self.assertTrue(await until(lambda: carol.last("GAME_OVER")))
        self.assertEqual(carol.last("GAME_OVER")["result"]["result"], "WON")

    async def test_timeout_de_turno(self):
        alice, bob, carol = await self.lobby("banana", turn_timeout=1)
        await self.play(alice, bob, carol)
        self.assertTrue(await until(lambda: (carol.router.player.last_guess or {}).get("timeout") is True, 4))
        self.assertEqual(carol.router.player.last_guess["player"], "bob@CIC")
        self.assertEqual(carol.router.player.turn, "carol@CIC")

    async def test_join_err_e_game_list(self):
        alice, bob, carol = await self.mesh(["alice", "bob", "carol"])
        self.assertEqual(await bob.router.request_games(), [])
        alice.router.create_game("casa", 1)
        games = await bob.router.request_games()
        self.assertEqual(len(games), 1)
        self.assertEqual((games[0]["status"], games[0]["host"], games[0]["word_length"]), ("OPEN", "alice@CIC", 4))
        await bob.router.join_host("alice@CIC")
        await until(lambda: "JOIN_OK" in bob.kinds())
        await carol.router.join_game("alice@CIC", games[0]["game_id"])  # lotada (max 1)
        await until(lambda: "JOIN_ERR" in carol.kinds())
        self.assertEqual(carol.last("JOIN_ERR")["reason"], "game_full")
        await carol.router.join_game("alice@CIC", "alice@CIC/00000000")
        await until(lambda: len([k for k in carol.kinds() if k == "JOIN_ERR"]) == 2)
        self.assertEqual(carol.last("JOIN_ERR")["reason"], "unknown_game")
        await alice.router.start_game()
        await carol.router.join_game("alice@CIC", games[0]["game_id"])
        await until(lambda: len([k for k in carol.kinds() if k == "JOIN_ERR"]) == 3)
        self.assertEqual(carol.last("JOIN_ERR")["reason"], "game_running")

    async def test_start_sem_jogador_recusado(self):
        alice, bob = await self.mesh(["alice", "bob"])
        alice.router.create_game("casa", 2)
        self.assertFalse(await alice.router.start_game())

    async def test_cancel_em_open(self):
        alice, bob, carol = await self.lobby("banana")
        self.assertTrue(await alice.router.cancel_game())
        self.assertTrue(await until(lambda: bob.last("GAME_OVER") and carol.last("GAME_OVER")))
        r = bob.last("GAME_OVER")["result"]
        self.assertEqual((r["result"], r["reason"]), ("ABORTED", "host_shutdown"))
        self.assertEqual(bob.router.player.status, "IDLE")


class T3_Falhas(Base):
    async def test_queda_de_jogador_remove_e_continua(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        await carol.kill()
        self.assertTrue(await until(lambda: "carol@CIC" not in alice.router.host.session.players, 6))
        self.assertTrue(await until(lambda: bob.router.player.players == ["bob@CIC"], 6))
        await self.guess(bob, letter="a")
        self.assertEqual(alice.router.host.session.game.get_masked_word(), "_a_a_a")

    async def test_reconexao_rejoin(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        port = carol.port
        await carol.kill()
        await until(lambda: "carol@CIC" not in alice.router.host.session.players, 6)
        self.nodes.remove(carol)
        carol2 = await self.node("carol", port=port)
        await until(lambda: "alice@CIC" in carol2.net.connections, 10)
        gid = alice.router.host.session.game_id
        await carol2.router.join_game("alice@CIC", gid)
        self.assertTrue(await until(lambda: carol2.last("JOIN_OK") is not None, 5))
        self.assertTrue(carol2.last("JOIN_OK")["rejoin"])
        self.assertTrue(await until(lambda: carol2.router.player.masked is not None, 5))
        self.assertEqual(alice.router.host.session.players[-1], "carol@CIC")

    async def test_queda_do_host_aborta_jogadores(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        await alice.kill()
        self.assertTrue(await until(lambda: bob.last("HOST_UNREACHABLE") and carol.last("HOST_UNREACHABLE"), 6))
        self.assertEqual((bob.router.player.status, carol.router.player.status), ("IDLE", "IDLE"))

    async def test_tres_pings_sem_pong_marca_stale(self):
        """Peer 'mudo': completa o HELLO mas nunca responde a PING."""
        writers = []

        async def silent(reader, writer):
            writers.append(writer)
            hello = json.loads(await reader.readline())
            writer.write((json.dumps({"type": "HELLO_OK", "msg_id": hello["msg_id"], "ttl": 1,
                                      "peer_id": "zed@CIC", "version": "1.0",
                                      "features": ["hangman"]}) + "\n").encode())
            await writer.drain()
            while await reader.read(4096):
                pass
        srv = await asyncio.start_server(silent, "127.0.0.1", 0)
        port = srv.sockets[0].getsockname()[1]
        a = await self.node("alice", ping=0.3)
        self.rdv.peers[("CIC", "zed")] = {"ip": "127.0.0.1", "port": port, "name": "zed", "namespace": "CIC",
                                          "ttl": 9999, "expires": time.monotonic() + 9999}
        events = []
        prev = a.net.peer_handler
        a.net.peer_handler = lambda e, p: (events.append((e, p)), prev(e, p))[1]
        self.assertTrue(await until(lambda: "zed@CIC" in a.net.connections, 5))
        t0 = time.monotonic()
        self.assertTrue(await until(lambda: ("STALE", "zed@CIC") in events, 6))
        self.assertGreaterEqual(time.monotonic() - t0, 0.3 * 2)  # >= 3 ciclos de PING
        srv.close()
        for w in writers:  # Python >= 3.12: wait_closed() espera as conexões; fecha-se antes
            w.close()

    async def test_quit_do_host_ordem_gameover_bye_unregister(self):
        alice, bob, carol = await self.lobby("banana")
        await self.play(alice, bob, carol)
        log_lines = []

        class H(logging.Handler):
            def emit(self, rec):
                log_lines.append(rec.getMessage())
        h = H(); h.setLevel(logging.DEBUG)
        names = ("rede.wire", "rede.p2p_client")
        loggers = [logging.getLogger(n) for n in names]
        logging.disable(logging.NOTSET)
        for lg in loggers:
            lg.addHandler(h); lg.setLevel(logging.DEBUG)
        try:
            await alice.router.shutdown()
            await alice.net.stop()
        finally:
            for lg in loggers:
                lg.removeHandler(h)
            logging.disable(logging.CRITICAL)
        def idx(sub):
            return next(i for i, l in enumerate(log_lines) if sub in l)
        go = idx('-> bob@CIC {"type":"GAME_OVER"')
        bye = idx('-> bob@CIC {"type":"BYE"')
        unreg = idx("UNREGISTER alice@CIC")
        self.assertLess(go, bye)
        self.assertLess(bye, unreg)
        self.assertTrue(await until(lambda: bob.last("GAME_OVER"), 3))
        self.assertEqual(bob.last("GAME_OVER")["result"]["reason"], "host_shutdown")
        self.assertNotIn(("alice", "CIC"), self.rdv.peers)


if __name__ == "__main__":
    unittest.main()
