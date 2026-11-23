"""
Projeto : PyP2P-Forca — Jogo da Forca P2P (Redes de Computadores, 2026/2)
Grupo   : [Grupo 14]
Integrantes (nome completo — matrícula):
  - [Davi Sales de Sousa] — [180015401]
  - [Felipe Salomão da Cruz] — [202068119]
  - [Vinícius Magao de Oliveira] — [261008780]

Módulo  : tests/test_hangman.py
Papel   : testes unitários das regras puras da forca (sem rede).
"""
import hashlib
import unittest

from jogo.hangman import HangmanGame, calculate_word_hash, normalize_word, validate_word


class HangmanTests(unittest.TestCase):
    def test_normalize_and_validate(self):
        self.assertEqual(normalize_word("AçÃo"), "acao")
        self.assertTrue(validate_word("abc"))
        self.assertFalse(validate_word("ab"))
        self.assertFalse(validate_word("a" * 21))
        self.assertFalse(validate_word("ab-c"))
        self.assertFalse(validate_word("ab c"))

    def test_hash_is_sha256_hex_lowercase(self):
        self.assertEqual(calculate_word_hash("casa"), hashlib.sha256(b"casa").hexdigest())

    def test_letter_correct_reveals_all_occurrences(self):
        g = HangmanGame("banana", 6)
        r = g.guess_letter("a")
        self.assertTrue(r.is_accepted and r.is_correct)
        self.assertEqual(r.word_revealed_count, 3)
        self.assertEqual(g.get_masked_word(), "_a_a_a")

    def test_wrong_letter_costs_life_and_repeat_rejected(self):
        g = HangmanGame("banana", 6)
        r = g.guess_letter("z")
        self.assertTrue(r.is_accepted and not r.is_correct)
        self.assertEqual(g.remaining_lives, 5)
        self.assertEqual(g.get_wrong_letters(), ["z"])
        r2 = g.guess_letter("z")
        self.assertFalse(r2.is_accepted)
        self.assertEqual(r2.reason, "letter_already_used")
        self.assertEqual(g.remaining_lives, 5)

    def test_invalid_letter(self):
        self.assertEqual(HangmanGame("casa").guess_letter("1").reason, "invalid_letter")

    def test_word_guess(self):
        g = HangmanGame("casa", 3)
        self.assertFalse(g.guess_word("mesa").is_correct)
        self.assertEqual(g.remaining_lives, 2)
        r = g.guess_word("casa")
        self.assertTrue(r.is_correct)
        self.assertEqual(r.word_revealed_count, 4)
        self.assertTrue(g.is_won())

    def test_loss(self):
        g = HangmanGame("casa", 2)
        g.guess_letter("x"); g.guess_letter("y")
        self.assertTrue(g.is_lost() and g.is_finished())
        self.assertEqual(g.guess_letter("z").reason, "game_not_running")

    def test_invalid_construction(self):
        with self.assertRaises(ValueError):
            HangmanGame("ab")
        with self.assertRaises(ValueError):
            HangmanGame("casa", 0)


if __name__ == "__main__":
    unittest.main()
