---
name: resilient-long-running-scripts
description: Use when writing any script or process that runs unattended, takes more than a few minutes, iterates over units of work, or could fail silently — ML sweeps, test suites, builds, deployments, data pipelines.
---

# Resilient Long-Running Scripts

## Overview
Every unattended script must be observable, resumable, and self-healing. Silent failure with no checkpoints means lost work and no diagnosis.

## The 7 Hard Requirements

| # | Requirement | Never do |
|---|-------------|----------|
| 1 | Flush stdout immediately | Rely on OS buffering |
| 2 | Append results per unit to disk | Write all output at end |
| 3 | Log progress with timestamps | Silent iteration |
| 4 | Per-unit timeout | Timeout only the whole process |
| 5 | Checkpoint completed units | Re-run everything on restart |
| 6 | Self-heal on failure (max 2 retries, auto-apply with guardrails) | Crash on first error |
| 7 | Exit non-zero if any unit permanently fails | Swallow exceptions |

## Python Pattern

```python
import concurrent.futures, json, subprocess, sys, traceback
from datetime import datetime
from pathlib import Path

CHECKPOINT = Path("checkpoint.json")
RESULTS    = Path("results.jsonl")

def log(msg):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)

def load_done() -> set:
    return set(json.loads(CHECKPOINT.read_text())) if CHECKPOINT.exists() else set()

def save_done(done: set):
    CHECKPOINT.write_text(json.dumps(list(done)))

def append_result(row: dict):
    with RESULTS.open("a") as f:
        f.write(json.dumps(row) + "\n")

def heal(script: str, item: str, error: str, tb: str) -> str:
    prompt = (
        f"A step failed in {script}.\nInput: {item}\nError: {error}\n"
        f"Traceback:\n{tb}\n\n"
        f'Respond with JSON only: {{"diagnosis":"one sentence","fix_command":"shell command or empty string"}}'
    )
    out = subprocess.run(
        ["grok", "--print", prompt],
        capture_output=True, text=True, timeout=120
    ).stdout.strip()
    try:
        return json.loads(out).get("fix_command", "")
    except json.JSONDecodeError:
        return ""

def run_unit(item):
    pass  # replace with your work; raise on failure

def run(items, unit_timeout=60, max_retries=2):
    done = load_done()
    failed = []
    log(f"Starting: {len(items)} items, {len(done)} already done")

    for i, item in enumerate(items):
        key = str(item)
        if key in done:
            continue
        log(f"[{i+1}/{len(items)}] {key}")

        for attempt in range(max_retries + 1):
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    result = ex.submit(run_unit, item).result(timeout=unit_timeout)
                append_result({"key": key, "result": result})
                done.add(key)
                save_done(done)
                break
            except Exception as e:
                tb = traceback.format_exc()
                if attempt == max_retries:
                    log(f"FAILED permanently: {key} — {e}")
                    append_result({"key": key, "error": str(e), "failed": True})
                    failed.append(key)
                    break
                fix = heal(__file__, str(item), str(e), tb)
                log(f"Attempt {attempt+1} failed. Agent diagnosis logged. Fix: {fix or '(none)'}")
                if fix:
                    subprocess.run(fix, shell=True, timeout=30, check=False)

    log(f"Done: {len(done) - len(failed)} ok, {len(failed)} failed")
    if failed:
        log(f"Failed units: {failed}")
        sys.exit(1)
```

## JavaScript / Node

```js
const fs = require('fs');
const { spawnSync, execSync } = require('child_process');

const CHECKPOINT = 'checkpoint.json';
const RESULTS    = 'results.jsonl';

const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);
const loadDone = () =>
  fs.existsSync(CHECKPOINT) ? new Set(JSON.parse(fs.readFileSync(CHECKPOINT))) : new Set();
const saveDone = (done) => fs.writeFileSync(CHECKPOINT, JSON.stringify([...done]));
const appendResult = (row) => fs.appendFileSync(RESULTS, JSON.stringify(row) + '\n');
const withTimeout = (fn, ms) =>
  Promise.race([fn(), new Promise((_, r) => setTimeout(() => r(new Error('timeout')), ms))]);

const heal = (item, error) => {
  const prompt =
    `A step failed.\nInput: ${item}\nError: ${error}\n` +
    `Respond with JSON only: {"diagnosis":"...","fix_command":"shell cmd or empty"}`;
  try {
    const r = spawnSync('grok', ['--print', prompt], { timeout: 120_000, encoding: 'utf8' });
    return JSON.parse(r.stdout).fix_command ?? '';
  } catch { return ''; }
};

async function run(items, { unitTimeoutMs = 60_000, maxRetries = 2 } = {}) {
  const done = loadDone();
  const failed = [];
  log(`Starting: ${items.length} items, ${done.size} already done`);

  for (let i = 0; i < items.length; i++) {
    const key = String(items[i]);
    if (done.has(key)) continue;
    log(`[${i+1}/${items.length}] ${key}`);

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const result = await withTimeout(() => runUnit(items[i]), unitTimeoutMs);
        appendResult({ key, result });
        done.add(key); saveDone(done);
        break;
      } catch (e) {
        if (attempt === maxRetries) {
          log(`FAILED permanently: ${key} — ${e.message}`);
          appendResult({ key, error: e.message, failed: true });
          failed.push(key); break;
        }
        const fix = heal(key, e.message);
        log(`Attempt ${attempt+1} failed. Fix: ${fix || '(none)'}`);
        if (fix) execSync(fix, { timeout: 30_000 });
      }
    }
  }

  log(`Done: ${done.size - failed.length} ok, ${failed.length} failed`);
  if (failed.length) { log(`Failed: ${failed}`); process.exit(1); }
}
```

## Bash (Linux / macOS)

> **macOS:** Install `brew install coreutils` for `gtimeout`. The pattern below auto-detects the OS.

```bash
#!/usr/bin/env bash
set -euo pipefail

CHECKPOINT_DIR=".done"
RESULTS_FILE="results.jsonl"
MAX_RETRIES=2
UNIT_TIMEOUT=60

TIMEOUT_CMD="timeout"
[[ "$(uname)" == "Darwin" ]] && TIMEOUT_CMD="gtimeout"

mkdir -p "$CHECKPOINT_DIR"
log() { echo "[$(date +%Y-%m-%dT%H:%M:%S)] $*"; }

heal() {
  local item="$1" error="$2"
  local prompt="A step failed. Input: $item Error: $error Respond with JSON only: {\"diagnosis\":\"...\",\"fix_command\":\"shell cmd or empty\"}"
  grok --print "$prompt" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('fix_command',''))" \
    || true
}

run_items() {
  local items=("$@")
  local failed=()

  for item in "${items[@]}"; do
    [ -f "$CHECKPOINT_DIR/$item" ] && continue
    log "Processing: $item"

    local attempt=0
    while [ "$attempt" -le "$MAX_RETRIES" ]; do
      if "$TIMEOUT_CMD" "$UNIT_TIMEOUT" bash -c "run_unit '$item'" >> "$RESULTS_FILE" 2>&1; then
        touch "$CHECKPOINT_DIR/$item"
        break
      else
        error="exit code $?"
        if [ "$attempt" -eq "$MAX_RETRIES" ]; then
          log "FAILED permanently: $item"
          echo "{\"key\":\"$item\",\"failed\":true}" >> "$RESULTS_FILE"
          failed+=("$item")
          break
        fi
        fix=$(heal "$item" "$error")
        log "Attempt $((attempt+1)) failed. Fix: ${fix:-(none)}"
        [ -n "$fix" ] && eval "$fix" || true
      fi
      attempt=$((attempt + 1))
    done
  done

  log "Done. Failed: ${#failed[@]}"
  [ "${#failed[@]}" -gt 0 ] && { log "Failed units: ${failed[*]}"; exit 1; }
}
```

## PowerShell (Windows)

```powershell
$CHECKPOINT = "checkpoint.json"
$RESULTS    = "results.jsonl"
$MAX_RETRIES = 2
$UNIT_TIMEOUT = 60

function Write-Log($msg) { Write-Host "[$([datetime]::Now.ToString('s'))] $msg" }

function Get-Done {
    if (Test-Path $CHECKPOINT) { return [System.Collections.Generic.HashSet[string]](Get-Content $CHECKPOINT | ConvertFrom-Json) }
    return [System.Collections.Generic.HashSet[string]]::new()
}
function Save-Done($done) { $done | ConvertTo-Json | Set-Content $CHECKPOINT }
function Append-Result($row) { ($row | ConvertTo-Json -Compress) | Add-Content $RESULTS }

function Invoke-Heal($item, $error) {
    $prompt = "A step failed. Input: $item Error: $error Respond with JSON only: {`"diagnosis`":`"...`",`"fix_command`":`"shell cmd or empty`"}"
    try {
        $out = grok --print $prompt 2>$null
        return ($out | ConvertFrom-Json).fix_command
    } catch { return "" }
}

function Invoke-WithTimeout($scriptBlock, $timeoutSecs) {
    $job = Start-Job -ScriptBlock $scriptBlock
    if (-not (Wait-Job $job -Timeout $timeoutSecs)) {
        Stop-Job $job; Remove-Job $job -Force
        throw "Timeout after ${timeoutSecs}s"
    }
    $result = Receive-Job $job
    Remove-Job $job
    return $result
}

function Run-Items($items) {
    $done   = Get-Done
    $failed = @()
    Write-Log "Starting: $($items.Count) items, $($done.Count) already done"

    for ($i = 0; $i -lt $items.Count; $i++) {
        $key = [string]$items[$i]
        if ($done.Contains($key)) { continue }
        Write-Log "[$($i+1)/$($items.Count)] $key"

        for ($attempt = 0; $attempt -le $MAX_RETRIES; $attempt++) {
            try {
                $item   = $items[$i]
                $result = Invoke-WithTimeout { Invoke-RunUnit $using:item } $UNIT_TIMEOUT
                Append-Result @{ key=$key; result=$result }
                $done.Add($key) | Out-Null; Save-Done $done
                break
            } catch {
                if ($attempt -eq $MAX_RETRIES) {
                    Write-Log "FAILED permanently: $key — $_"
                    Append-Result @{ key=$key; error="$_"; failed=$true }
                    $failed += $key; break
                }
                $fix = Invoke-Heal $key "$_"
                Write-Log "Attempt $($attempt+1) failed. Fix: $(if($fix){$fix}else{'(none)'})"
                if ($fix) { Invoke-Expression $fix }
            }
        }
    }

    Write-Log "Done: $($done.Count - $failed.Count) ok, $($failed.Count) failed"
    if ($failed.Count -gt 0) { Write-Log "Failed: $($failed -join ', ')"; exit 1 }
}
```

## Parallelism

Running units sequentially wastes available hardware. Before setting a fixed concurrency, detect what the machine has.

```python
import os, subprocess

def detect_concurrency(task_type: str = "cpu") -> int:
    cpu_count = os.cpu_count() or 4
    if task_type == "gpu":
        try:
            r = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                capture_output=True, text=True, timeout=10,
            )
            gpu_count = len([l for l in r.stdout.strip().splitlines() if l.strip()])
            if gpu_count > 0:
                return gpu_count
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return 1
    if task_type == "browser":
        return min(cpu_count, 8)
    if task_type == "cpu":
        return max(1, cpu_count // 2)
    return cpu_count * 2  # io/network
```

| Workload | type | Rationale |
|----------|------|-----------|
| Browser automation, Playwright | `"browser"` | IO-bound, JS engine does the work |
| Model inference (GPU) | `"gpu"` | One process per GPU |
| Data crunching, compression | `"cpu"` | CPU-bound, cap at half cores |
| API calls, DB queries | `"io"` | High latency tolerance |

**Async (IO/browser):** use `asyncio.Semaphore` + `asyncio.gather` within a single parameter value.
**Threaded (CPU):** use `ThreadPoolExecutor` + `as_completed`.
**Warning:** module-level mutable state creates race conditions across parameter values — only parallelise within one parameter at a time.

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| `python script.py` buffers all output | `python -u script.py` or `flush=True` on every print |
| Writing results with pandas at end | Open file in append mode, write each row immediately |
| Top-level `try/except` that `pass`es | Let exceptions bubble up to the retry loop |
| Global process timeout only | Wrap each unit in `ThreadPoolExecutor.result(timeout=...)` |
| `grok --print` returns prose | Prompt explicitly: `Respond with JSON only: {...}` |
| Healing loop retries forever | Always bound retries; mark-and-continue on exhaustion |
| Checkpoint saved only at end | `save_done()` immediately after each successful unit |
| Node.js: string-interpolated shell command | Use `spawnSync('grok', ['--print', prompt])` — avoids quoting bugs on Windows |
| Bash on macOS: `timeout` not found | Install `brew install coreutils`; use `gtimeout` |
