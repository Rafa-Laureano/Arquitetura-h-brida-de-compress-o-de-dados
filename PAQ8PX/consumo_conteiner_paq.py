#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Medição robusta de CPU e memória durante:
  (1) empacotamento do contêiner (N cópias)  -> phase=pack (psutil)
  (2) compressão com GMIX dentro de cgroup v2 -> phase=compress (cgroup)
Salva tempo-série em CSV + resumo em JSON.

Requisitos:
  - Python 3 + psutil:    sudo apt-get install python3-psutil
  - (fallback) pidstat:   sudo apt-get install sysstat
  - Rodar com sudo (criação de cgroup).
"""

import os, sys, time, json, csv, struct, subprocess
from pathlib import Path
from datetime import datetime

# =============== CONFIG ===============
INPUT_FILE = "lzw_gps.bin"
GMIX_PATH  = "./paq8px"             # caminho do compressor (aqui: paq8px)
COPIES     = 500                    # número de cópias no contêiner (5 p/ teste)
WORKDIR    = Path("consumo_gps-lzw_paq8px")
OUTDIR     = Path("final_consumo_gps-lzw_paq8px")
CSV_PATH   = WORKDIR / "gps-lzw_pack_paq8px.csv"
SUMMARY_JSON = WORKDIR / "summary.json"
SAMPLE_DT  = 0.05                    # 50 ms (ajuste se quiser menos ruído)
# ======================================

# ---------- util ----------
def ensure_dirs():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    OUTDIR.mkdir(parents=True, exist_ok=True)

def now_iso():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

def human(n):
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PB"

# ---------- contêiner ----------
def pack_container(src_file: Path, copies: int, out_path: Path):
    """
    Formato: [u32 N] [[u16 len_nome][nome][u64 len_dados][dados]] * N
    """
    data = src_file.read_bytes()
    with out_path.open('wb') as f:
        f.write(struct.pack('<I', copies))
        base = src_file.name
        for i in range(1, copies+1):
            name = f"copy_{i:04d}_{base}"
            name_b = name.encode('utf-8')
            f.write(struct.pack('<H', len(name_b)))
            f.write(name_b)
            f.write(struct.pack('<Q', len(data)))
            f.write(data)
    return out_path

# ---------- CSV ----------
CSV_FIELDS = [
    "time_iso","t_rel_s","phase",
    "cpu_pct","cpu_usage_s","mem_bytes",
    "note"
]

def csv_init(path: Path):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()

def csv_row(path: Path, row: dict):
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writerow(row)

# ---------- medição: fase PACK (psutil) ----------
def measure_pack_phase(container_path: Path):
    """
    Amostra CPU% e RSS do processo Python atual durante a criação do contêiner.
    """
    import psutil
    proc = psutil.Process(os.getpid())
    proc.cpu_percent(None)  # baseline

    t0 = time.time()
    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": 0.0, "phase": "pack",
                       "cpu_pct": 0.0, "cpu_usage_s": 0.0, "mem_bytes": proc.memory_info().rss, "note": "start"})
    pack_container(Path(INPUT_FILE), COPIES, container_path)
    t1 = time.time()
    cpu_pct = proc.cpu_percent(None)              # % desde a última chamada
    cpu_time = sum(proc.cpu_times()[:2])          # utime+stime
    rss = proc.memory_info().rss
    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": t1 - t0, "phase": "pack",
                       "cpu_pct": cpu_pct, "cpu_usage_s": cpu_time, "mem_bytes": rss, "note": "end"})
    return t1 - t0

# ---------- helpers cgroup v2 + fallbacks ----------
def _file_read(p: Path, default=None):
    try:
        return p.read_text().strip()
    except Exception:
        return default

def _file_write(p: Path, text: str) -> bool:
    try:
        p.write_text(text)
        return True
    except Exception:
        return False

def cgv2_root():
    root = Path("/sys/fs/cgroup")
    return root if (root / "cgroup.controllers").exists() else None

def cgv2_enable_controllers(cg_parent: Path, controllers=("cpu","memory")) -> bool:
    """
    Liga controladores no PAI da subárvore (ex.: /sys/fs/cgroup/measure),
    evitando 'Permission denied' da raiz gerida pelo systemd.
    """
    ctrl_file = cg_parent / "cgroup.subtree_control"
    avail = _file_read(cg_parent / "cgroup.controllers", "")
    if not avail:
        return False
    ok = True
    current = _file_read(ctrl_file, "")
    for c in controllers:
        if c in avail and f"+{c}" not in (current or ""):
            ok = _file_write(ctrl_file, f"+{c}") and ok
            current = _file_read(ctrl_file, "")
    return ok

def cgv2_create(name: str) -> Path | None:
    root = cgv2_root()
    if not root:
        return None
    # Criar subárvore 'measure' (pai) para ligar controladores sem tocar na raiz
    parent = root / "measure"
    parent.mkdir(exist_ok=True)
    cgv2_enable_controllers(parent, ("cpu","memory"))  # habilita no pai
    cg = parent / name
    cg.mkdir(exist_ok=True)
    return cg

def cgv2_attach_pid(cg: Path, pid: int):
    (cg / "cgroup.procs").write_text(str(pid))

def cgv2_has_cpu(cg: Path) -> bool:
    return (cg / "cpu.stat").exists()

def cgv2_has_mem(cg: Path) -> bool:
    return (cg / "memory.current").exists()

def cgv2_read_cpu_usage_s(cg: Path) -> float:
    txt = _file_read(cg / "cpu.stat", "")
    usec = 0
    for line in (txt or "").splitlines():
        if line.startswith("usage_usec"):
            usec = int(line.split()[1])
            break
    return usec / 1_000_000.0

def cgv2_read_mem_current(cg: Path) -> int:
    v = _file_read(cg / "memory.current", None)
    return int(v) if v is not None else 0

def proc_rss_bytes(pid: int) -> int:
    """
    Fallback de memória por processo (RSS).
    Prioriza /proc/<pid>/smaps_rollup; se indisponível, usa /proc/<pid>/status (VmRSS).
    """
    try:
        with open(f"/proc/{pid}/smaps_rollup", "r") as f:
            for line in f:
                if line.startswith("Rss:"):
                    kb = int(line.split()[1])
                    return kb * 1024
    except Exception:
        pass
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return kb * 1024
    except Exception:
        pass
    return 0

# ---------- medição: fase COMPRESS (cgroup v2 OU fallback pidstat) ----------
def run_gmix_and_measure(container_path: Path, gmix_out: Path):
    # Aqui apenas trocamos o comando para paq8px (-8 input output)
    cmd = [GMIX_PATH, "-8", str(container_path), str(gmix_out)]

    root = cgv2_root()
    if root:
        cgname = f"gmix_{int(time.time())}"
        cg = cgv2_create(cgname)
        if cg:
            # inicia o compressor
            p = subprocess.Popen(cmd, preexec_fn=os.setsid)
            try:
                cgv2_attach_pid(cg, p.pid)
            except Exception:
                # falhou ao anexar → fallback pidstat
                p.kill(); p.wait()
                return run_gmix_with_pidstat(container_path, gmix_out)

            t0 = time.time()
            if not cgv2_has_cpu(cg):
                # Sem cpu.stat → não adianta seguir; cai p/ pidstat
                p.terminate()
                try: p.wait(timeout=1)
                except Exception: p.kill(); p.wait()
                return run_gmix_with_pidstat(container_path, gmix_out)

            last_cpu = cgv2_read_cpu_usage_s(cg)
            # Primeira amostra (memória: cgroup se disponível, senão RSS do processo)
            mem0 = cgv2_read_mem_current(cg) if cgv2_has_mem(cg) else proc_rss_bytes(p.pid)
            csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": 0.0, "phase": "compress",
                               "cpu_pct": 0.0, "cpu_usage_s": last_cpu,
                               "mem_bytes": mem0, "note": "start"})

            ncpu = os.cpu_count() or 1
            last_mem = mem0  # ---- clamp anti-zero ----
            while True:
                if p.poll() is not None:
                    cpu_now = cgv2_read_cpu_usage_s(cg)
                    # leitura final: preferir cgroup (se existir), sem clamp
                    mem_now = cgv2_read_mem_current(cg) if cgv2_has_mem(cg) else proc_rss_bytes(p.pid)
                    dt = time.time() - t0
                    cpu_pct = ((cpu_now - last_cpu) / max(1e-6, dt)) * 100.0 / ncpu
                    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": dt, "phase": "compress",
                                       "cpu_pct": max(cpu_pct, 0.0), "cpu_usage_s": cpu_now,
                                       "mem_bytes": mem_now, "note": "end"})
                    break

                time.sleep(SAMPLE_DT)
                cpu_now = cgv2_read_cpu_usage_s(cg)

                # memória durante a execução: aplica clamp anti-zero se o processo ainda está vivo
                if cgv2_has_mem(cg):
                    mem_now = cgv2_read_mem_current(cg)
                else:
                    mem_now = proc_rss_bytes(p.pid)
                if mem_now == 0 and p.poll() is None:
                    mem_now = last_mem
                else:
                    last_mem = mem_now

                dt = time.time() - t0
                cpu_pct = ((cpu_now - last_cpu) / SAMPLE_DT) * 100.0 / ncpu
                last_cpu = cpu_now
                csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": dt, "phase": "compress",
                                   "cpu_pct": max(cpu_pct, 0.0), "cpu_usage_s": cpu_now,
                                   "mem_bytes": mem_now, "note": ""})

            rc = p.returncode
            # limpar cgroup
            try:
                (cg / "cgroup.procs").write_text("")
                cg.rmdir()
            except Exception:
                pass

            if rc != 0:
                raise RuntimeError(f"GMIX falhou (rc={rc})")
            return

    # Sem cgroup v2 ou não deu pra usar → fallback pidstat
    return run_gmix_with_pidstat(container_path, gmix_out)

def run_gmix_with_pidstat(container_path: Path, gmix_out: Path):
    """
    Fallback robusto quando cgroup v2 não está disponível:
      - Lança o compressor
      - Roda pidstat 1/SAMPLE_DT s para esse PID (captura CPU e memória)
      - Agrega no CSV
    """
    interval = max(1, int(round(SAMPLE_DT)))  # pidstat requer inteiro em s
    p = subprocess.Popen([GMIX_PATH, "-8", str(container_path), str(gmix_out)])
    pidstat_cmd = ["pidstat", "-h", "-r", "-u", "-p", str(p.pid), str(interval)]
    pid = subprocess.Popen(pidstat_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    t0 = time.time()
    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": 0.0, "phase": "compress",
                       "cpu_pct": 0.0, "cpu_usage_s": 0.0, "mem_bytes": 0, "note": "start(fallback_pidstat)"})
    try:
        while True:
            if p.poll() is not None:
                break
            line = pid.stdout.readline()
            if not line:
                time.sleep(0.05)
                continue
            parts = line.strip().split()
            if len(parts) >= 7 and parts[0].isdigit():
                try:
                    usr = float(parts[1].replace(",", "."))
                    sysc = float(parts[2].replace(",", "."))
                    cpu_pct = usr + sysc
                    rss_kb = int(parts[-1])  # última coluna costuma ser RSS (kB)
                    dt = time.time() - t0
                    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": dt, "phase": "compress",
                                       "cpu_pct": cpu_pct, "cpu_usage_s": 0.0,
                                       "mem_bytes": rss_kb * 1024, "note": ""})
                except Exception:
                    continue
    finally:
        try:
            pid.communicate(timeout=1)
        except Exception:
            pass

    rc = p.wait()
    csv_row(CSV_PATH, {"time_iso": now_iso(), "t_rel_s": time.time()-t0, "phase": "compress",
                       "cpu_pct": 0.0, "cpu_usage_s": 0.0, "mem_bytes": 0, "note": "end(fallback_pidstat)"})
    if rc != 0:
        raise RuntimeError(f"GMIX falhou (rc={rc})")

# ---------- main ----------
def main():
    ensure_dirs()
    csv_init(CSV_PATH)

    src = Path(INPUT_FILE)
    if not src.exists():
        raise FileNotFoundError(f"Entrada não encontrada: {src}")

    container = WORKDIR / f"container_{COPIES:04d}.bin"
    gmix_out  = WORKDIR / f"container_{COPIES:04d}.gmix"

    # Fase 1: medição do empacotamento
    t_pack = measure_pack_phase(container)
    cont_bytes = container.stat().st_size

    # Fase 2: compressão + medição robusta
    t0 = time.time()
    run_gmix_and_measure(container, gmix_out)
    t_comp = time.time() - t0
    comp_bytes = gmix_out.stat().st_size

    # Copia artefatos finais
    fin_container = OUTDIR / f"final_container_N_{COPIES:04d}.bin"
    fin_gmix      = OUTDIR / f"final_container_N_{COPIES:04d}.gmix"
    try:
        import shutil
        shutil.copy2(container, fin_container)
        shutil.copy2(gmix_out, fin_gmix)
    except Exception:
        pass

    summary = {
        "created_at": datetime.now().isoformat(),
        "input_file": str(src.resolve()),
        "copies": COPIES,
        "container_bytes": cont_bytes,
        "gmix_bytes": comp_bytes,
        "reduction_pct": (1 - comp_bytes/max(cont_bytes,1)) * 100.0,
        "pack_time_s": t_pack,
        "compress_time_s": t_comp,
        "csv_timeseries": str(CSV_PATH.resolve()),
        "final_container": str(fin_container.resolve()),
        "final_gmix": str(fin_gmix.resolve()),
        "notes": {
            "pack_phase": "psutil (processo Python)",
            "compress_phase": "cgroup v2 (cpu.stat/memory.current) com clamp anti-zero; fallback p/ RSS ou pidstat"
        }
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n==== RESUMO ====")
    print(f"Contêiner: {human(cont_bytes)} | GMIX: {human(comp_bytes)} | Redução: {summary['reduction_pct']:.2f}%")
    print(f"Pack: {t_pack:.3f}s | Compress: {t_comp:.3f}s")
    print(f"CSV: {CSV_PATH}")
    print(f"Resumo JSON: {SUMMARY_JSON}")
    print(f"Saídas finais: {fin_container} , {fin_gmix}")

if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
    main()
