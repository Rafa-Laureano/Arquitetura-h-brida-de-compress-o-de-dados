"""
 Pipeline de RE-compressão com contêiner de metadados (versão GMIX)

• Um arquivo de entrada já-comprimido (ex.: "log_huff.bin" ou "gps_lzw.bin").
• Empacota N cópias em um contêiner com metadados (nome, tamanho, dados), SEM depender de argparse.
• Re-comprime o contêiner com GMIX a cada iteração.
• Continua enquanto a redução for >= THRESHOLD_PCT (ex.: 2%) e até MAX_ITERS.
• Salva CSV com métricas por iteração + artefatos organizados em pasta de trabalho.
• Gera artefatos finais (contêiner e .gmix) na pasta "final" com manifesto JSON.
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

# --- Robustez de encoding na saída (não falha se indisponível) ---
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# =============================
# CONFIG — edite aqui
# =============================
INPUT_FILE = "log_huff.bin"   # arquivo já comprimido (ex.: "gps_lzw.bin", "log_huff.bin")
GMIX_PATH  = "./gmix"        # caminho do binário gmix
THRESHOLD_PCT = -3.0          # continuar enquanto a redução >= 2.0%
MAX_ITERS = 300              # segurança para testes
WORKDIR = Path("work_gmix_log_huff")  # pasta de trabalho
FINALDIR = Path("final_log_huff")     # sai o melhor resultado aqui
CSV_LOG  = WORKDIR / "recompress_log-huff_gmix.csv"

# Timeouts e retry (conservadores)
TIMEOUT_BASE_SEC = 14400       # 4 horas de base por tentativa
ADAPTIVE_SAFETY_MARGIN = 600   # +10 min de folga
RETRY_MAX = 6                  # até 6 tentativas com backoff exponencial

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
    """Cria contêiner com metadados repetindo o mesmo src_file 'copies' vezes.
    Formato: [u32 N] [[u16 len_nome][nome][u64 len_dados][dados]] * N
    """
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

def parse_gmix_output(text: str):
    """
    Extrai métricas típicas se o GMIX imprimir algo no formato:
      "1860 bytes -> 69 bytes in 11.58 s."
      "cross entropy: 0.297"
    É tolerante: se não achar, preenche com None e o chamador faz fallback.
    """
    sizes = re.search(r"(\d+) bytes -> (\d+) bytes in ([\d.]+) s\.", text)
    entropy = re.search(r"cross entropy: ([\d.]+)", text, flags=re.I)
    block_types = None
    for line in text.splitlines():
        if line.strip().lower().startswith("detected block types:"):
            block_types = line.split(":", 1)[1].strip()
            break
    return {
        "original": int(sizes.group(1)) if sizes else None,
        "compressed": int(sizes.group(2)) if sizes else None,
        "time_s": float(sizes.group(3)) if sizes else None,
        "cross_entropy": float(entropy.group(1)) if entropy else None,
        "block_types": block_types,
    }

def gmix_compress(inp: Path, out: Path, timeout_s: int):
    """Executa o gmix -c com timeout e devolve as métricas."""
    cmd = [GMIX_PATH, "-c", str(inp), str(out)]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    elapsed = time.time() - t0
    out_text = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        raise RuntimeError(f"GMIX falhou (rc={p.returncode}):\n{out_text}")
    meta = parse_gmix_output(out_text)
    # Fallbacks se o log não trouxer algum campo:
    if meta["original"] is None:
        meta["original"] = inp.stat().st_size
    if meta["compressed"] is None:
        meta["compressed"] = out.stat().st_size if out.exists() else None
    if meta["time_s"] is None:
        meta["time_s"] = elapsed
    meta["raw_log"] = out_text
    return meta

def pct_reduction(orig: int, comp: int) -> float:
    if not orig or not comp or orig <= 0:
        return 0.0
    return (1.0 - (comp / orig)) * 100.0

def write_csv_header(csv_path: Path):
    if not csv_path.exists():
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=[
                "timestamp","iter","copies","container_bytes","gmix_bytes",
                "reduction_pct","time_s","cross_entropy","block_types",
                "container_path","gmix_path"
            ])
            w.writeheader()

def append_csv(csv_path: Path, row: dict):
    with csv_path.open('a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp","iter","copies","container_bytes","gmix_bytes",
            "reduction_pct","time_s","cross_entropy","block_types",
            "container_path","gmix_path"
        ])
        w.writerow(row)

def est_timeout_adaptativo(container_bytes: int, last_bps: float | None) -> int:
    """Estima timeout com base no tamanho do contêiner e throughput (bytes/s) observado."""
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
        gmix_out = iter_dir / f"container_{it:03d}.gmix"

        # 1) cria contêiner
        pack_container(src, copies, container_path)
        cont_bytes = container_path.stat().st_size

        # 2) timeout adaptativo
        timeout_s = est_timeout_adaptativo(cont_bytes, last_bps)

        # 3) roda gmix com retry/backoff
        attempt = 1
        stop_due_timeout = False
        while True:
            try:
                meta = gmix_compress(container_path, gmix_out, timeout_s)
                break  # sucesso
            except subprocess.TimeoutExpired:
                if attempt >= RETRY_MAX:
                    print(f"[TIMEOUT] N={copies} excedeu {timeout_s}s apos {RETRY_MAX} tentativas.")
                    if best is None:
                        raise
                    stop_due_timeout = True
                    break
                attempt += 1
                timeout_s *= 2  # backoff exponencial
                # ASCII apenas para evitar UnicodeEncodeError em latin-1
                print(f"[RETRY] Timeout - tentando novamente com timeout={timeout_s}s (tentativa {attempt}/{RETRY_MAX})...")
                continue

        if stop_due_timeout:
            break_all = True
            break

        # Segurança: se por algum motivo não gerou saída, aborta educadamente
        if not Path(gmix_out).exists():
            raise RuntimeError(f"GMIX nao gerou o arquivo de saida esperado: {gmix_out}")

        red = pct_reduction(meta["original"], meta["compressed"])

        append_csv(CSV_LOG, {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "iter": it,
            "copies": copies,
            "container_bytes": cont_bytes,
            "gmix_bytes": meta["compressed"],
            "reduction_pct": f"{red:.4f}",
            "time_s": f"{meta['time_s']:.4f}",
            "cross_entropy": meta.get("cross_entropy"),
            "block_types": meta.get("block_types"),
            "container_path": str(container_path),
            "gmix_path": str(gmix_out),
        })

        # Atualiza throughput observado (bytes/s) p/ próximos timeouts
        if meta.get('time_s') and meta['time_s'] > 0 and meta.get('original'):
            last_bps = meta['original'] / meta['time_s']

        print(f"Iter {it:03d} | N={copies} | cont={human(cont_bytes)} -> gmix={human(meta['compressed'])} | redução={red:.3f}% | t={meta['time_s']:.2f}s")

        if red >= THRESHOLD_PCT:
            best = {
                "iter": it,
                "copies": copies,
                "container": str(container_path),
                "gmix": str(gmix_out),
                "meta": meta,
            }
        else:
            break

    if best is None:
        print("\nATENCAO: nenhuma iteracao alcancou reducao >= THRESHOLD_PCT. Salvando a primeira mesmo assim.")
        first_dir = WORKDIR / "iter_001"
        container_path = first_dir / "container_001.bin"
        gmix_out = first_dir / "container_001.gmix"
        FINALDIR.mkdir(exist_ok=True)
        shutil.copy2(container_path, FINALDIR / Path(container_path.name))
        shutil.copy2(gmix_out, FINALDIR / Path(gmix_out.name))
        return

    FINALDIR.mkdir(exist_ok=True)
    fin_container = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.bin"
    fin_gmix = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.gmix"
    shutil.copy2(best["container"], fin_container)
    shutil.copy2(best["gmix"], fin_gmix)

    manifest = {
        "created_at": datetime.now().isoformat(),
        "input_file": str(Path(INPUT_FILE).resolve()),
        "algorithm": "gmix",
        "threshold_pct": THRESHOLD_PCT,
        "max_iters": MAX_ITERS,
        "best": {
            "iter": best["iter"],
            "copies": best["copies"],
            "original_bytes": best["meta"]["original"],
            "gmix_bytes": best["meta"]["compressed"],
            "reduction_pct": pct_reduction(best["meta"]["original"], best["meta"]["compressed"]),
            "time_s": best["meta"]["time_s"],
            "cross_entropy": best["meta"].get("cross_entropy"),
            "block_types": best["meta"].get("block_types"),
        },
        "container_format": "<I N> then N * {<H name_len><name utf8><Q data_len><data>}",
        "inner_files": [f"copy_{i+1:04d}_" + Path(INPUT_FILE).name for i in range(best['copies'])]
    }
    (FINALDIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print("\n===== RESUMO FINAL =====")
    print(f"Melhor iteracao: {best['iter']} (N={best['copies']})")
    print(f"Final container: {fin_container}")
    print(f"Final gmix:      {fin_gmix}")
    print(f"Manifesto:       {FINALDIR / 'manifest.json'}")

if __name__ == "__main__":
    main()
