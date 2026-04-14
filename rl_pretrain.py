"""
Pre-training for the RLPA scheduler using batch_task.csv trace data.

Runs several full SimPy simulation episodes driven by the Alibaba-style
batch_task trace so the agent accumulates real placement experience *before*
any live comparison run.  Weights are saved after every episode so partially-
trained policies are never lost if the process is interrupted.

Usage (callable from API or command line):
    python -m src.rl_pretrain
"""

import os
import threading
from typing import Optional

_TRACE_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "batch_task.csv")
_pretrain_lock = threading.Lock()


def pretrain_from_trace(
    weights_path: Optional[str] = None,
    trace_csv_path: str = _TRACE_CSV,
    sim_duration: float = 5000.0,
    episodes: int = 15,
    verbose: bool = True,
) -> bool:
    """
    Pre-train RLPA by replaying batch_task.csv through full SimPy episodes.

    Each episode extends the Q-weights so subsequent runs start with a
    meaningful, energy-aware policy rather than exploring from scratch.

    Parameters
    ----------
    weights_path : str or None
        Where to save/load weights.  Defaults to RLPA_WEIGHTS_PATH in schedulers.
    trace_csv_path : str
        Path to batch_task.csv (real or synthetic).
    sim_duration : float
        SimPy time horizon per episode.
    episodes : int
        Number of replay passes over the trace.
    verbose : bool
        Print progress to stdout.

    Returns
    -------
    bool
        True on success, False if trace is missing and could not be generated.
    """
    # Lazy imports to avoid circular references at module load time.
    from config.default_config import (
        ENABLE_SERVER_POWER_STATES,
        ENABLE_VM_MIGRATION,
        MAX_MIGRATIONS_PER_VM,
        MAX_WAIT_TIME,
        MIGRATION_BANDWIDTH_GB_PER_TIME,
        MIGRATION_BASE_DOWNTIME,
        MIGRATION_CHECK_INTERVAL,
        MIGRATION_DEST_MAX_UTIL,
        MIGRATION_ENERGY_GAIN_HORIZON,
        MIGRATION_ENERGY_OVERHEAD_KWH,
        MIGRATION_MIN_NET_ENERGY_GAIN_KWH,
        MIGRATION_SOURCE_MAX_UTIL,
        PHYSICAL_SERVERS,
        SERVER_IDLE_SHUTDOWN_TIME,
        SERVER_MIGRATION_COOLDOWN,
        SERVER_POWER_STATE_CHECK_INTERVAL,
        SERVER_WAKEUP_DELAY,
        SERVER_WAKEUP_ENERGY_KWH,
        VM_MIGRATION_COOLDOWN,
        VM_PROFILES,
    )
    from src.schedulers import RLPA_WEIGHTS_PATH, rl_pa_scheduler, save_rlpa_weights
    from src.simulation_engine import CloudDataCenterSimulation
    from src.workload_generator import TraceReplayWorkloadGenerator
    from utils.metrics import MetricsCollector

    if weights_path is None:
        weights_path = RLPA_WEIGHTS_PATH

    # Auto-generate the synthetic trace if the CSV is missing.
    if not os.path.exists(trace_csv_path):
        if verbose:
            print("[RLPA Pretrain] batch_task.csv not found — generating synthetic trace …")
        try:
            from data.generate_synthetic_trace import generate
            generate(trace_csv_path)
        except Exception as exc:
            print(f"[RLPA Pretrain] Could not generate trace: {exc}")
            return False

    with _pretrain_lock:
        for episode in range(1, episodes + 1):
            if verbose:
                print(f"[RLPA Pretrain] Episode {episode}/{episodes} starting …")

            metrics = MetricsCollector(simulation_name=f"rlpa_pretrain_ep{episode}")
            workload_gen = TraceReplayWorkloadGenerator(
                trace_csv_path=trace_csv_path,
                vm_profiles=VM_PROFILES,
                sim_duration=sim_duration,
                max_tasks_per_episode=800,
            )

            sim = CloudDataCenterSimulation(
                servers=PHYSICAL_SERVERS,
                vm_profiles=VM_PROFILES,
                workload_generator=workload_gen,
                scheduler=rl_pa_scheduler,
                max_sla_time=MAX_WAIT_TIME,
                metrics_collector=metrics,
                random_seed=42 + episode,
                enable_vm_migration=ENABLE_VM_MIGRATION,
                migration_check_interval=MIGRATION_CHECK_INTERVAL,
                migration_source_max_util=MIGRATION_SOURCE_MAX_UTIL,
                migration_dest_max_util=MIGRATION_DEST_MAX_UTIL,
                max_migrations_per_vm=MAX_MIGRATIONS_PER_VM,
                vm_migration_cooldown=VM_MIGRATION_COOLDOWN,
                server_migration_cooldown=SERVER_MIGRATION_COOLDOWN,
                migration_base_downtime=MIGRATION_BASE_DOWNTIME,
                migration_bandwidth_gb_per_time=MIGRATION_BANDWIDTH_GB_PER_TIME,
                migration_energy_overhead_kwh=MIGRATION_ENERGY_OVERHEAD_KWH,
                migration_energy_gain_horizon=MIGRATION_ENERGY_GAIN_HORIZON,
                migration_min_net_energy_gain_kwh=MIGRATION_MIN_NET_ENERGY_GAIN_KWH,
                enable_server_power_states=ENABLE_SERVER_POWER_STATES,
                server_idle_shutdown_time=SERVER_IDLE_SHUTDOWN_TIME,
                server_power_state_check_interval=SERVER_POWER_STATE_CHECK_INTERVAL,
                server_wakeup_delay=SERVER_WAKEUP_DELAY,
                server_wakeup_energy_kwh=SERVER_WAKEUP_ENERGY_KWH,
                progress_callback=None,
                realtime_delay_seconds=0.0,
            )

            sim.run(simulation_time=sim_duration, workload_pattern="poisson", log_interval=500)

            # Persist weights after every episode so a crash never loses progress.
            save_rlpa_weights(sim, weights_path)

            if verbose:
                stats = metrics.get_statistics()
                agent = getattr(sim, "_rl_pa_agent", None)
                updates = getattr(agent, "total_updates", 0) if agent else 0
                eps = agent.epsilon if agent else 0.0
                print(
                    f"[RLPA Pretrain] Ep {episode} done  "
                    f"energy={stats['total_energy_kwh']:.2f} kWh  "
                    f"carbon={stats['carbon_footprint_kg_baseline']:.2f} kg  "
                    f"vms={stats['total_vms_completed']}  "
                    f"Q-updates={updates}  epsilon={eps:.4f}"
                )

    if verbose:
        print(f"[RLPA Pretrain] Complete. Weights saved → {weights_path}")
    return True


if __name__ == "__main__":
    pretrain_from_trace(verbose=True)
