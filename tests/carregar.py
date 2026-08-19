# -*- coding: utf-8 -*-
"""
Carrega o `financeiro.py` como módulo, sem subir a aplicação.

O arquivo termina com uma chamada a `main()`, que renderiza a tela de login e
tentaria falar com o banco. Para testar as funções isoladamente, o carregador
remove essa última chamada e executa o resto.

Funciona a partir de qualquer diretório: o caminho é derivado da localização
deste arquivo, não de onde o comando foi chamado.
"""

from __future__ import annotations

import pathlib
import types

RAIZ = pathlib.Path(__file__).resolve().parent.parent
ORIGEM = RAIZ / "financeiro.py"

_CHAMADA_FINAL = "main()"


def carregar_financeiro() -> types.ModuleType:
    """Devolve o módulo `financeiro` carregado, sem executar a aplicação."""
    if not ORIGEM.exists():
        raise FileNotFoundError(f"Não encontrei {ORIGEM}. Rode os testes a partir do repositório.")

    codigo = ORIGEM.read_text(encoding="utf-8").rstrip()
    if not codigo.endswith(_CHAMADA_FINAL):
        raise RuntimeError(
            f"Esperava que {ORIGEM.name} terminasse com `{_CHAMADA_FINAL}`. "
            "Se a estrutura do arquivo mudou, ajuste tests/carregar.py."
        )
    codigo = codigo[: -len(_CHAMADA_FINAL)]

    modulo = types.ModuleType("financeiro")
    modulo.__file__ = str(ORIGEM)
    exec(compile(codigo, str(ORIGEM), "exec"), modulo.__dict__)
    return modulo


class Resultado:
    """Placar simples de asserções, para não depender de framework de teste."""

    def __init__(self, titulo: str) -> None:
        self.titulo = titulo
        self.falhas: list[str] = []
        self.total = 0

    def secao(self, nome: str) -> None:
        print(f"\n{nome}")

    def checar(self, rotulo: str, obtido, esperado) -> None:
        self.total += 1
        if obtido == esperado:
            print(f"  ok      {rotulo}")
        else:
            self.falhas.append(f"{rotulo}: obtido={obtido!r} esperado={esperado!r}")
            print(f"  FALHOU  {rotulo}: {obtido!r} != {esperado!r}")

    def verdadeiro(self, rotulo: str, condicao) -> None:
        self.checar(rotulo, bool(condicao), True)

    def encerrar(self) -> int:
        print("\n" + "=" * 62)
        if self.falhas:
            print(f"{self.titulo}: {len(self.falhas)} de {self.total} FALHARAM")
            for falha in self.falhas:
                print("  -", falha)
            return 1
        print(f"{self.titulo}: {self.total} verificações, todas passaram")
        return 0
