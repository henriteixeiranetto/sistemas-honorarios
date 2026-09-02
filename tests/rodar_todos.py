# -*- coding: utf-8 -*-
"""
Roda a suíte inteira e devolve um resumo.

Rodar:  python tests/rodar_todos.py

Sai com código 1 se qualquer suíte falhar — é o que o GitHub Actions usa
para marcar o commit como quebrado.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

AQUI = pathlib.Path(__file__).resolve().parent
SUITES = ["teste_funcoes.py", "teste_banco.py", "teste_sql.py"]

# Sem isto, dependência faltando vira um traceback por suíte no meio do log e
# um resumo que só diz "falhou" — sem dizer o que fazer.
NECESSARIOS = {
    "pandas": "pandas",
    "streamlit": "streamlit",
    "psycopg2": "psycopg2-binary",
    "fpdf": "fpdf2",
    "openpyxl": "openpyxl",
    "altair": "altair",
}


def _conferir_dependencias() -> list[str]:
    import importlib.util

    return [
        pacote
        for modulo, pacote in NECESSARIOS.items()
        if importlib.util.find_spec(modulo) is None
    ]


def main() -> int:
    faltando = _conferir_dependencias()
    if faltando:
        print("Faltam dependências para rodar os testes:\n", flush=True)
        for pacote in faltando:
            print(f"  - {pacote}", flush=True)
        print(
            "\nInstale tudo de uma vez com:\n"
            "\n    pip install -r requirements-dev.txt\n"
            f"\n(Python em uso: {sys.executable})",
            flush=True,
        )
        return 1
    return _rodar_suites()


def _rodar_suites() -> int:
    resultados: list[tuple[str, int]] = []

    for suite in SUITES:
        # flush=True: sem isso a saída do processo pai fica no buffer e aparece
        # depois da saída dos filhos, embaralhando o log do CI.
        print("\n" + "#" * 62, flush=True)
        print(f"# {suite}", flush=True)
        print("#" * 62, flush=True)
        processo = subprocess.run(
            [sys.executable, str(AQUI / suite)],
            cwd=str(AQUI),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        resultados.append((suite, processo.returncode))

    print("\n" + "=" * 62)
    print("RESUMO")
    for suite, codigo in resultados:
        print(f"  {'PASSOU ' if codigo == 0 else 'FALHOU '}  {suite}")

    falhas = [s for s, c in resultados if c != 0]
    if falhas:
        print(f"\n{len(falhas)} suíte(s) falharam: {', '.join(falhas)}")
        return 1
    print("\nTudo verde.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
