"""
 Pipeline de RE-compressão com contêiner de metadados (versão BSC-M03)

• Um arquivo de entrada já-comprimido (ex.: "log_huff.bin" ou "gps_lzw.bin").
• Empacota N cópias em um contêiner com metadados (nome, tamanho, dados), SEM depender de argparse.
• Re-comprime o contêiner com BSC-M03 a cada iteração.
• Continua enquanto a redução for >= THRESHOLD_PCT (ex.: 2%) e até MAX_ITERS.
• Salva CSV com métricas por iteração + artefatos organizados em pasta de trabalho.
• Gera artefatos finais (contêiner e .bsc) na pasta "final" com manifesto JSON.
"""

import os
import re
import csv
import json
import time
import shutil
import struct
import subprocess
from datetime import datetime
from pathlib import Path
import sys

# --- Robustez de encoding ---
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# =============================
# CONFIG
# =============================
INPUT_FILE = "lzw_gps.bin"
BSC_PATH   = "./bsc-m03"    # caminho do binário bsc-m03
THRESHOLD_PCT = -3.0
MAX_ITERS = 500
WORKDIR = Path("work_teste")
FINALDIR = Path("final_teste")
CSV_LOG  = WORKDIR / "recompress_teste.csv"

TIMEOUT_BASE_SEC = 14400
ADAPTIVE_SAFETY_MARGIN = 600
RETRY_MAX = 6

# =============================
# Utilitários
# =============================

def ensure_dirs():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    FINALDIR.mkdir(parents=True, exist_ok=True)

def human(n):
    for unit in ["B","KB","MB","GB"]:
        if n < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} TB"

def pack_container(src_file: Path, copies: int, out_path: Path):
    src_file = Path(src_file)
    out_path = Path(out_path)
    data = src_file.read_bytes()
    with out_path.open('wb') as f:
        f.write(struct.pack('<I', copies))
        base = src_file.name
        for i in range(1, copies+1):
            name = f"copy_{i:04d}_" + base
            name_b = name.encode('utf-8')
            f.write(struct.pack('<H', len(name_b)))
            f.write(name_b)
            f.write(struct.pack('<Q', len(data)))
            f.write(data)
    return out_path

def bsc_compress(inp: Path, out: Path, timeout_s: int):
    """Executa o bsc-m03 e retorna métricas básicas."""
    cmd = [BSC_PATH, "e", str(inp), str(out)]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    elapsed = time.time() - t0
    out_text = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        raise RuntimeError(f"BSC-M03 falhou (rc={p.returncode}):\n{out_text}")

    # BSC normalmente não imprime estatísticas, então medimos tamanhos diretamente
    original = inp.stat().st_size
    compressed = out.stat().st_size if out.exists() else None
    meta = {
        "original": original,
        "compressed": compressed,
        "time_s": elapsed,
        "raw_log": out_text,
        "cross_entropy": None,
        "block_types": None
    }
    return meta

def pct_reduction(orig: int, comp: int) -> float:
    if not orig or not comp or orig <= 0:
        return 0.0
    return (1.0 - (comp / orig)) * 100.0

def write_csv_header(csv_path: Path):
    if not csv_path.exists():
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=[
                "timestamp","iter","copies","container_bytes","bsc_bytes",
                "reduction_pct","time_s","cross_entropy","block_types",
                "container_path","bsc_path"
            ])
            w.writeheader()

def append_csv(csv_path: Path, row: dict):
    with csv_path.open('a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp","iter","copies","container_bytes","bsc_bytes",
            "reduction_pct","time_s","cross_entropy","block_types",
            "container_path","bsc_path"
        ])
        w.writerow(row)

def est_timeout_adaptativo(container_bytes: int, last_bps: float | None) -> int:
    if last_bps and last_bps > 0:
        estimado = int(container_bytes / max(last_bps * 0.30, 1)) + ADAPTIVE_SAFETY_MARGIN
        return max(TIMEOUT_BASE_SEC, estimado)
    return TIMEOUT_BASE_SEC + ADAPTIVE_SAFETY_MARGIN

# =============================
# Main loop
# =============================

def main():
    ensure_dirs()
    write_csv_header(CSV_LOG)

    src = Path(INPUT_FILE)
    if not src.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {src}")

    best = None
    last_bps = None
    break_all = False

    for it in range(1, MAX_ITERS+1):
        copies = it
        iter_dir = WORKDIR / f"iter_{it:03d}"
        iter_dir.mkdir(parents=True, exist_ok=True)
        container_path = iter_dir / f"container_{it:03d}.bin"
        bsc_out = iter_dir / f"container_{it:03d}.bsc"

        pack_container(src, copies, container_path)
        cont_bytes = container_path.stat().st_size
        timeout_s = est_timeout_adaptativo(cont_bytes, last_bps)

        attempt = 1
        stop_due_timeout = False
        while True:
            try:
                meta = bsc_compress(container_path, bsc_out, timeout_s)
                break
            except subprocess.TimeoutExpired:
                if attempt >= RETRY_MAX:
                    print(f"[TIMEOUT] N={copies} excedeu {timeout_s}s após {RETRY_MAX} tentativas.")
                    if best is None:
                        raise
                    stop_due_timeout = True
                    break
                attempt += 1
                timeout_s *= 2
                print(f"[RETRY] Timeout - tentando novamente com timeout={timeout_s}s (tentativa {attempt}/{RETRY_MAX})...")
                continue

        if stop_due_timeout:
            break_all = True
            break

        if not Path(bsc_out).exists():
            raise RuntimeError(f"BSC-M03 não gerou o arquivo de saída esperado: {bsc_out}")

        red = pct_reduction(meta["original"], meta["compressed"])

        append_csv(CSV_LOG, {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "iter": it,
            "copies": copies,
            "container_bytes": cont_bytes,
            "bsc_bytes": meta["compressed"],
            "reduction_pct": f"{red:.4f}",
            "time_s": f"{meta['time_s']:.4f}",
            "cross_entropy": meta.get("cross_entropy"),
            "block_types": meta.get("block_types"),
            "container_path": str(container_path),
            "bsc_path": str(bsc_out),
        })

        if meta.get('time_s') and meta['time_s'] > 0 and meta.get('original'):
            last_bps = meta['original'] / meta['time_s']

        print(f"Iter {it:03d} | N={copies} | cont={human(cont_bytes)} -> bsc={human(meta['compressed'])} | redução={red:.3f}% | t={meta['time_s']:.2f}s")

        if red >= THRESHOLD_PCT:
            best = {
                "iter": it,
                "copies": copies,
                "container": str(container_path),
                "bsc": str(bsc_out),
                "meta": meta,
            }
        else:
            break

    if best is None:
        print("\nATENÇÃO: nenhuma iteração atingiu redução >= THRESHOLD_PCT. Salvando a primeira mesmo assim.")
        first_dir = WORKDIR / "iter_001"
        container_path = first_dir / "container_001.bin"
        bsc_out = first_dir / "container_001.bsc"
        FINALDIR.mkdir(exist_ok=True)
        shutil.copy2(container_path, FINALDIR / Path(container_path.name))
        shutil.copy2(bsc_out, FINALDIR / Path(bsc_out.name))
        return

    FINALDIR.mkdir(exist_ok=True)
    fin_container = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.bin"
    fin_bsc = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.bsc"
    shutil.copy2(best["container"], fin_container)
    shutil.copy2(best["bsc"], fin_bsc)

    manifest = {
        "created_at": datetime.now().isoformat(),
        "input_file": str(Path(INPUT_FILE).resolve()),
        "algorithm": "bsc-m03",
        "threshold_pct": THRESHOLD_PCT,
        "max_iters": MAX_ITERS,
        "best": {
            "iter": best["iter"],
            "copies": best["copies"],
            "original_bytes": best["meta"]["original"],
            "bsc_bytes": best["meta"]["compressed"],
            "reduction_pct": pct_reduction(best["meta"]["original"], best["meta"]["compressed"]),
            "time_s": best["meta"]["time_s"],
        },
        "container_format": "<I N> then N * {<H name_len><name utf8><Q data_len><data>}",
        "inner_files": [f"copy_{i+1:04d}_" + Path(INPUT_FILE).name for i in range(best['copies'])]
    }
    (FINALDIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print("\n===== RESUMO FINAL =====")
    print(f"Melhor iteração: {best['iter']} (N={best['copies']})")
    print(f"Final container: {fin_container}")
    print(f"Final bsc:       {fin_bsc}")
    print(f"Manifesto:       {FINALDIR / 'manifest.json'}")

if __name__ == "__main__":
    main()
