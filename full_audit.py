"""Full comparison: RLPA vs every challenger through the same code path the UI uses."""
from config.default_config import *
from src.schedulers import (
    rl_pa_scheduler, aco_scheduler, energy_aware_scheduler,
    gwo_scheduler, best_fit_scheduler, fcfs_scheduler,
    round_robin_scheduler, sjf_scheduler,
)
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector


def run_one(name, scheduler_fn, seed=42, pattern="poisson"):
    m = MetricsCollector(simulation_name=name)
    wg = WorkloadGenerator(
        arrival_rate=WORKLOAD_ARRIVAL_RATE, vm_profiles=VM_PROFILES,
        service_time_mean=SERVICE_TIME_MEAN, service_time_std=SERVICE_TIME_STD,
        random_seed=seed,
    )
    sim = CloudDataCenterSimulation(
        servers=PHYSICAL_SERVERS, vm_profiles=VM_PROFILES, workload_generator=wg,
        scheduler=scheduler_fn, max_sla_time=MAX_WAIT_TIME, metrics_collector=m,
        random_seed=seed,
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
    )
    sim._rl_pa_inference_mode = True
    sim.run(simulation_time=SIMULATION_TIME, workload_pattern=pattern, log_interval=25)
    return m.get_statistics()


algos = {
    "RLPA": rl_pa_scheduler,
    "ACO": aco_scheduler,
    "GWO": gwo_scheduler,
    "EnergyAware": energy_aware_scheduler,
    "BestFit": best_fit_scheduler,
    "FCFS": fcfs_scheduler,
    "RoundRobin": round_robin_scheduler,
    "SJF": sjf_scheduler,
}

for pattern in ["poisson", "bursty", "time_varying"]:
    print(f"\n{'='*120}")
    print(f"PATTERN: {pattern}")
    print(f"{'='*120}")

    results = {}
    for name, fn in algos.items():
        results[name] = run_one(name, fn, pattern=pattern)

    metrics_to_show = [
        ("total_energy_kwh", "lower"),
        ("carbon_footprint_kg_baseline", "lower"),
        ("sla_violation_rate", "lower"),
        ("avg_waiting_time", "lower"),
        ("avg_turnaround_time", "lower"),
        ("avg_power_watts", "lower"),
        ("peak_power_watts", "lower"),
        ("avg_utilization", "higher"),
        ("total_vms_completed", "higher"),
        ("total_migrations", "lower"),
    ]

    header = f"{'Metric':<32}" + "".join(f"{n:>12}" for n in algos.keys())
    print(header)
    print("-" * len(header))

    rlpa_wins = 0
    total = 0
    for key, direction in metrics_to_show:
        row = f"{key:<32}"
        vals = {}
        for n in algos.keys():
            v = results[n].get(key, 0)
            vals[n] = v
            row += f"{v:>12.4f}"

        if direction == "lower":
            best = min(vals, key=vals.get)
        else:
            best = max(vals, key=vals.get)

        marker = " <<< RLPA BEST" if best == "RLPA" else f" ({best})"
        row += marker
        if best == "RLPA":
            rlpa_wins += 1
        total += 1
        print(row)

    print(f"\nRLPA wins: {rlpa_wins}/{total}")
