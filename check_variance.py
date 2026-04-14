"""Quick check for powered-off server count across patterns."""
from full_audit import run_one, algos
import src.schedulers as S

_orig = S.rl_pa_scheduler
off_counts = []

def logging_scheduler(sim, pending, servers):
    off = sum(1 for s in servers if not s.is_powered_on)
    off_counts.append(off)
    return _orig(sim, pending, servers)

S.rl_pa_scheduler = logging_scheduler
for pat in ['poisson', 'time_varying', 'bursty']:
    off_counts.clear()
    run_one('RLPA', logging_scheduler, pattern=pat)
    if off_counts:
        print(f"{pat:15s} off_min={min(off_counts)} off_max={max(off_counts)} off_mean={sum(off_counts)/len(off_counts):.2f} calls={len(off_counts)}")
