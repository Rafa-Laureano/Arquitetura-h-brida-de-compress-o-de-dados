#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rafaella — Pipeline de DEScompressão (Estágio 1: algoritmo → contêiner bruto ; Estágio 2: desempacote)

Fluxo:
1) Detecta o algoritmo pelo sufixo/manifest e roda o comando de descompressão configurado,
   gerando o contêiner bruto (.bin).
2) Desempacota o contêiner bruto → arquivos internos em stage2_extracted/.

Gera:
- CSV (restore_log.csv) listando arquivos extraídos (nome, tamanho, algoritmo inferido).
- Manifesto (restore_manifest.json) com metadados da execução.

Sem argparse — edite CONFIG conforme seu caso.
"""

import os
import re
import csv
import json
import time
import struct
import subprocess
from datetime import datetime
from pathlib import Path
import sys

# Tenta garantir UTF-8; se o terminal não suportar, cai em ASCII seguro.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# =============================
# CONFIG — edite aqui
# =============================

# Aponte para seu arquivo FINAL comprimido pelo algoritmo (pode ser .cmix .gmix .bsc .lstm .paq8px ...)
INPUT_FINAL = Path("/home/rafaella/Desktop/raspberry5/experimentos/bsc/bsc-m03/build/final_gps_huff/final_container_iter_300_N_0300.bsc")
OUTDIR = Path("restore_run-gps-huff")          # raiz de saída da restauração
TIMEOUT_BASE_SEC = 3600

# Caminhos dos binários (ajuste conforme seu ambiente)
BIN = {
    "cmix": "./cmix",
    "gmix": "./gmix",
    "bsc":  "./bsc-m03",
    "lstm": "./lstm-compress",
    "paq8px": "./paq8px",
}

# Comandos de DEScompressão por algoritmo.
# Use {inp} e {out} como placeholders. Ajuste se seu binário tiver sintaxe diferente.
DECOMPRESS_CMDS = {
    "cmix":  [BIN["cmix"], "-d", "{inp}", "{out}"],
    "gmix":  [BIN["gmix"], "-d", "{inp}", "{out}"],     # ajuste se seu gmix usar outra flag
    "bsc":   [BIN["bsc"],  "d",  "{inp}", "{out}"],     # bsc-m03: 'd' geralmente descomprime
    "lstm":  [BIN["lstm"], "-d", "{inp}", "{out}"],     # lstm-compress: usual '-d'; ajuste se necessário
    "paq8px":[BIN["paq8px"], "-d", "{inp}", "{out}"],   # paq8px: '-d' costuma descomprimir
}

# Heurística de mapeamento de extensão → algoritmo
EXT_MAP = {
    ".cmix": "cmix",
    ".gmix": "gmix",
    ".bsc":  "bsc",
    ".lstm": "lstm",
    ".paq8": "paq8px",
    ".paq8px": "paq8px",
    ".paq":  "paq8px",
}

SUPPORTED_ALGOS = list(DECOMPRESS_CMDS.keys())

NAME_TO_ALGO = [
    (re.compile(r"\bcmix\b", re.I),   "cmix"),
    (re.compile(r"\bgmix\b", re.I),   "gmix"),
    (re.compile(r"\bbsc\b", re.I),    "bsc"),
    (re.compile(r"lstm", re.I),       "lstm"),
    (re.compile(r"paq8px|paq8|paq", re.I), "paq8px"),
]

# =============================
# Utilitários
# =============================

def ensure_dirs():
    (OUTDIR / "stage1_raw").mkdir(parents=True, exist_ok=True)
    (OUTDIR / "stage2_extracted").mkdir(parents=True, exist_ok=True)

def run_cmd(cmd_list, timeout_s: int):
    t0 = time.time()
    p = subprocess.run(cmd_list, capture_output=True, text=True, timeout=timeout_s)
    dt = time.time() - t0
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out, dt

def detect_algo_from_ext(path: Path) -> str | None:
    ext = "".join(path.suffixes) or path.suffix
    # tenta maior sufixo primeiro (ex.: .paq8px)
    for k in sorted(EXT_MAP.keys(), key=len, reverse=True):
        if str(path).lower().endswith(k):
            return EXT_MAP[k]
    return None

def detect_algo_from_name(path: Path) -> str | None:
    s = path.name
    for rx, key in NAME_TO_ALGO:
        if rx.search(s):
            return key
    return None

def load_manifest_algo_if_any(path: Path) -> str | None:
    # se o arquivo estiver ao lado de um manifest.json (pasta final), tenta ler "algorithm"
    # Ex.: final/manifest.json
    cand = path.parent / "manifest.json"
    try:
        if cand.exists():
            j = json.loads(cand.read_text(encoding="utf-8"))
            alg = (j.get("algorithm") or "").strip().lower()
            return alg if alg in SUPPORTED_ALGOS else None
    except Exception:
        pass
    return None

def decide_algo(input_final: Path) -> str:
    # Ordem de decisão: manifest → extensão → nome → erro
    alg = load_manifest_algo_if_any(input_final)
    if alg:
        return alg
    alg = detect_algo_from_ext(input_final)
    if alg:
        return alg
    alg = detect_algo_from_name(input_final)
    if alg:
        return alg
    raise RuntimeError(
        f"Não foi possível inferir o algoritmo para '{input_final.name}'. "
        f"Renomeie o arquivo com sufixo conhecido ({', '.join(EXT_MAP.keys())}) "
        f"ou ajuste manualmente o mapeamento."
    )

def decompress_generic(algo: str, inp_file: Path, out_raw: Path, timeout_s: int):
    if algo not in DECOMPRESS_CMDS:
        raise RuntimeError(f"Algoritmo '{algo}' não suportado. Suportados: {list(DECOMPRESS_CMDS.keys())}")
    tpl = [s.format(inp=str(inp_file), out=str(out_raw)) for s in DECOMPRESS_CMDS[algo]]
    rc, log, dt = run_cmd(tpl, timeout_s)
    if rc != 0:
        raise RuntimeError(f"Falha ao descomprimir com {algo}: rc={rc}\n{log}")
    return {"time_s": dt, "raw_log": log, "algo": algo}

def is_container(path: Path) -> bool:
    try:
        with path.open('rb') as f:
            if len(f.read(4)) < 4:
                return False
            f.seek(0)
            n = struct.unpack('<I', f.read(4))[0]
            if n == 0 or n > 1_000_000:
                return False
            # sanity check da 1ª entrada
            name_len_b = f.read(2)
            if len(name_len_b) < 2:
                return False
            name_len = struct.unpack('<H', name_len_b)[0]
            if name_len <= 0 or name_len > 4096:
                return False
            name = f.read(name_len)
            if len(name) < name_len:
                return False
            size_b = f.read(8)
            if len(size_b) < 8:
                return False
            size = struct.unpack('<Q', size_b)[0]
            if size > (path.stat().st_size - f.tell()):
                return False
            return True
    except Exception:
        return False

def unpack_container(container_path: Path, out_dir: Path):
    files = []
    with container_path.open('rb') as f:
        n = struct.unpack('<I', f.read(4))[0]
        for _ in range(n):
            name_len = struct.unpack('<H', f.read(2))[0]
            # nomes no contêiner foram gravados em UTF-8; se não forem, cai no replace
            name = f.read(name_len).decode('utf-8', errors='replace')
            data_len = struct.unpack('<Q', f.read(8))[0]
            data = f.read(data_len)
            out_path = out_dir / name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(data)
            files.append(out_path)
    return files

def guess_algo(filename: str):
    for rx, key in NAME_TO_ALGO:
        if rx.search(filename):
            return key
    return None

# =============================
# Pipeline
# =============================

def main():
    ensure_dirs()
    if not INPUT_FINAL.exists():
        raise FileNotFoundError(f"Arquivo final não encontrado: {INPUT_FINAL}")

    # 1) Detecta algoritmo e descomprime para contêiner bruto
    algo = decide_algo(INPUT_FINAL)
    print(f"[1/2] Detectado algoritmo: {algo}")
    raw_container = OUTDIR / "stage1_raw" / "container.bin"
    timeout_alg = max(TIMEOUT_BASE_SEC, int(INPUT_FINAL.stat().st_size / 1000))
    meta = decompress_generic(algo, INPUT_FINAL, raw_container, timeout_alg)
    print(f"   OK em {meta['time_s']:.2f}s -> {raw_container} ({raw_container.stat().st_size} bytes)")

    # 2) Desempacotar contêiner
    print("[2/2] Desempacotando contêiner...")
    if not is_container(raw_container):
        raise RuntimeError("Arquivo resultante da descompressão não segue o formato de contêiner esperado.")
    extracted = unpack_container(raw_container, OUTDIR / "stage2_extracted")
    print(f"   Extraídos {len(extracted)} arquivos.")

    # CSV (apenas inventário dos extraídos)
    csv_path = OUTDIR / "restore_log.csv"
    with csv_path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","file","size_bytes","algo_inferido"])
        w.writeheader()
        for p in extracted:
            w.writerow({
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "file": str(p.name),
                "size_bytes": p.stat().st_size,
                "algo_inferido": guess_algo(p.name) or "",
            })

    # Manifesto
    manifest = {
        "run_at": datetime.now().isoformat(),
        "input_final": str(INPUT_FINAL.resolve()),
        "detected_algorithm": algo,
        "stage1_container": str(raw_container.resolve()),
        "extracted_count": len(extracted),
        "out_stage2": str((OUTDIR/"stage2_extracted").resolve()),
        "supported_algos_for_reference": SUPPORTED_ALGOS,
        "notes": "Pipeline genérico: Estágio 1 (descompressão por algoritmo) + Estágio 2 (desempacote).",
    }
    (OUTDIR/"restore_manifest.json").write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print("\nFinalizado. Consulte:")
    print(f"  - {csv_path}")
    print(f"  - {(OUTDIR/'restore_manifest.json')}")
    print(f"  - Diretório com extraídos: {(OUTDIR/'stage2_extracted')}")

if __name__ == "__main__":
    main()
