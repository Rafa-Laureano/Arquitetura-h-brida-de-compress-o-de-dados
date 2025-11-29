"""
 Pipeline de RE-compressão com contêiner de metadados (versão PAQ8PX)

• Um arquivo de entrada já-comprimido (ex.: "log_huff.bin" ou "gps_lzw.bin").
• Empacota N cópias em um contêiner com metadados (nome, tamanho, dados), SEM depender de argparse.
• Re-comprime o contêiner com PAQ8PX a cada iteração.
• Continua enquanto a redução for >= THRESHOLD_PCT (ex.: 2%) e até MAX_ITERS.
• Salva CSV com métricas por iteração + artefatos organizados em pasta de trabalho.
• Gera artefatos finais (contêiner e .paq8px) na pasta "final" com manifesto JSON.
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
INPUT_FILE = "iot_huff.bin"     # arquivo já comprimido (ex.: "gps_lzw.bin", "log_huff.bin")
PAQ8PX_PATH  = "./paq8px"      # caminho do binário paq8px
THRESHOLD_PCT = -3.0           # continuar enquanto a redução >= (ex.: 2.0) — aqui negativo permite seguir mesmo sem ganho
MAX_ITERS = 300                # segurança para testes
WORKDIR = Path("work_paq8px_iot-huff")  # pasta de trabalho
FINALDIR = Path("final_iot-huff")       # sai o melhor resultado aqui
CSV_LOG  = WORKDIR / "recompress_iot-huff_paq8px.csv"

# Timeouts e retry (conservadores)
TIMEOUT_BASE_SEC = 14400         # 4 horas de base por tentativa
ADAPTIVE_SAFETY_MARGIN = 600     # +10 min de folga
RETRY_MAX = 6                    # até 6 tentativas com backoff exponencial

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

def parse_paq8px_output(text: str):
    """
    Tenta extrair métricas do PAQ8PX.
    Exemplos possíveis (variantes entre builds):
      "Compressed from 12345 to 6789 bytes."
      "Time 11.58 sec"
      "12345 -> 6789 in 11.58 s"
    Se não achar, retorna None e chamador usa fallbacks.
    """
    # Tamanho original e comprimido
    m_sizes = (
        re.search(r"Compressed from\s+(\d+)\s+to\s+(\d+)\s+bytes", text, flags=re.I)
        or re.search(r"(\d+)\s*bytes?\s*->\s*(\d+)\s*bytes?", text, flags=re.I)
        or re.search(r"(\d+)\s*->\s*(\d+)", text, flags=re.I)
    )
    # Tempo
    m_time = (
        re.search(r"Time\s+([\d.]+)\s*s(ec)?", text, flags=re.I)
        or re.search(r"in\s+([\d.]+)\s*s(ec)?", text, flags=re.I)
    )

    original = int(m_sizes.group(1)) if m_sizes else None
    compressed = int(m_sizes.group(2)) if m_sizes else None
    time_s = float(m_time.group(1)) if m_time else None

    return {
        "original": original,
        "compressed": compressed,
        "time_s": time_s,
        "raw_log": text
    }

def paq8px_compress(inp: Path, out: Path, timeout_s: int):
    """Executa o paq8px com nível -8 e arquivo de saída explícito, com timeout, e devolve métricas."""
    # Evita prompts de overwrite: remove saída existente, se houver
    try:
        if out.exists():
            out.unlink()
    except Exception:
        pass

    cmd = [PAQ8PX_PATH, "-8", str(inp), str(out)]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    elapsed = time.time() - t0
    out_text = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        raise RuntimeError(f"PAQ8PX falhou (rc={p.returncode}):\n{out_text}")

    meta = parse_paq8px_output(out_text)
    # Fallbacks se o log não trouxer algum campo:
    if meta["original"] is None:
        meta["original"] = inp.stat().st_size
    if meta["compressed"] is None:
        meta["compressed"] = out.stat().st_size if out.exists() else None
    if meta["time_s"] is None:
        meta["time_s"] = elapsed
    return meta

def pct_reduction(orig: int, comp: int) -> float:
    if not orig or not comp or orig <= 0:
        return 0.0
    return (1.0 - (comp / orig)) * 100.0

def write_csv_header(csv_path: Path):
    if not csv_path.exists():
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=[
                "timestamp","iter","copies","container_bytes","paq8px_bytes",
                "reduction_pct","time_s",
                "container_path","paq8px_path"
            ])
            w.writeheader()

def append_csv(csv_path: Path, row: dict):
    with csv_path.open('a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp","iter","copies","container_bytes","paq8px_bytes",
            "reduction_pct","time_s",
            "container_path","paq8px_path"
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
        paq_out = iter_dir / f"container_{it:03d}.paq8px"

        # 1) cria contêiner
        pack_container(src, copies, container_path)
        cont_bytes = container_path.stat().st_size

        # 2) timeout adaptativo
        timeout_s = est_timeout_adaptativo(cont_bytes, last_bps)

        # 3) roda paq8px com retry/backoff
        attempt = 1
        stop_due_timeout = False
        while True:
            try:
                meta = paq8px_compress(container_path, paq_out, timeout_s)
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
                print(f"[RETRY] Timeout - tentando novamente com timeout={timeout_s}s (tentativa {attempt}/{RETRY_MAX})...")
                continue

        if stop_due_timeout:
            break_all = True
            break

        # Segurança: se por algum motivo não gerou saída, aborta educadamente
        if not Path(paq_out).exists():
            raise RuntimeError(f"PAQ8PX nao gerou o arquivo de saida esperado: {paq_out}")

        red = pct_reduction(meta["original"], meta["compressed"])

        append_csv(CSV_LOG, {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "iter": it,
            "copies": copies,
            "container_bytes": cont_bytes,
            "paq8px_bytes": meta["compressed"],
            "reduction_pct": f"{red:.4f}",
            "time_s": f"{meta['time_s']:.4f}",
            "container_path": str(container_path),
            "paq8px_path": str(paq_out),
        })

        # Atualiza throughput observado (bytes/s) p/ próximos timeouts
        if meta.get('time_s') and meta['time_s'] > 0 and meta.get('original'):
            last_bps = meta['original'] / meta['time_s']

        print(f"Iter {it:03d} | N={copies} | cont={human(cont_bytes)} -> paq8px={human(meta['compressed'])} | redução={red:.3f}% | t={meta['time_s']:.2f}s")

        if red >= THRESHOLD_PCT:
            best = {
                "iter": it,
                "copies": copies,
                "container": str(container_path),
                "paq8px": str(paq_out),
                "meta": meta,
            }
        else:
            break

    if best is None:
        print("\nATENCAO: nenhuma iteracao alcancou reducao >= THRESHOLD_PCT. Salvando a primeira mesmo assim.")
        first_dir = WORKDIR / "iter_001"
        container_path = first_dir / "container_001.bin"
        paq_out = first_dir / "container_001.paq8px"
        FINALDIR.mkdir(exist_ok=True)
        shutil.copy2(container_path, FINALDIR / Path(container_path.name))
        shutil.copy2(paq_out, FINALDIR / Path(paq_out.name))
        return

    FINALDIR.mkdir(exist_ok=True)
    fin_container = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.bin"
    fin_paq = FINALDIR / f"final_container_iter_{best['iter']:03d}_N_{best['copies']:04d}.paq8px"
    shutil.copy2(best["container"], fin_container)
    shutil.copy2(best["paq8px"], fin_paq)

    manifest = {
        "created_at": datetime.now().isoformat(),
        "input_file": str(Path(INPUT_FILE).resolve()),
        "algorithm": "paq8px",
        "threshold_pct": THRESHOLD_PCT,
        "max_iters": MAX_ITERS,
        "best": {
            "iter": best["iter"],
            "copies": best["copies"],
            "original_bytes": best["meta"]["original"],
            "paq8px_bytes": best["meta"]["compressed"],
            "reduction_pct": pct_reduction(best["meta"]["original"], best["meta"]["compressed"]),
            "time_s": best["meta"]["time_s"],
        },
        "container_format": "<I N> then N * {<H name_len><name utf8><Q data_len><data>}",
        "inner_files": [f"copy_{i+1:04d}_" + Path(INPUT_FILE).name for i in range(best['copies'])]
    }
    (FINALDIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print("\n===== RESUMO FINAL =====")
    print(f"Melhor iteracao: {best['iter']} (N={best['copies']})")
    print(f"Final container: {fin_container}")
    print(f"Final paq8px:    {fin_paq}")
    print(f"Manifesto:       {FINALDIR / 'manifest.json'}")

if __name__ == "__main__":
    main()
