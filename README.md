# Longhaul

![Longhaul](longhaul.jpg)

Scripting standards for any process that runs unattended. Longhaul enforces 7 hard requirements that prevent the most common failure modes in long-running scripts: silent freezes, lost progress, and unrecoverable crashes.

Works with **Claude Code**, **Codex**, **Cursor**, and **Grok Build**.

---

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

Applies to: ML sweeps, test suites, builds, deployments, data pipelines — anything that runs for more than a few minutes without supervision.

---

## Installation

### Claude Code
```
/plugin install github:Retlinac/longhaul
```

### Codex
Copy the skill directory to your user skills folder:
```bash
cp -r .agents/skills/resilient-long-running-scripts ~/.agents/skills/
```

### Cursor
Copy the rule into your project:
```bash
cp .cursor/rules/resilient-long-running-scripts.mdc <your-project>/.cursor/rules/
```
The rule auto-attaches to `.py`, `.js`, `.ts`, `.sh`, and `.ps1` files.

### Grok Build
Copy the skill directory to your user skills folder:
```bash
cp -r .grok/skills/resilient-long-running-scripts ~/.grok/skills/
```

---

## License

MIT

---

## What's Included

Patterns for **Python**, **JavaScript/Node**, **Bash** (Linux + macOS), and **PowerShell** (Windows), each covering:

- Flushed, timestamped logging
- Per-unit timeouts via `ThreadPoolExecutor` / `Promise.race` / `timeout` / `Start-Job`
- Incremental result writes to `.jsonl`
- JSON checkpoint file for resumability
- Self-healing loop: on failure, invokes your AI CLI to diagnose and apply a fix, then retries (max 2 attempts, mark-and-continue on exhaustion)
- Non-zero exit if any units permanently fail
