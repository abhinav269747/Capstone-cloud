"""Reproduce UI comparison results at sim_time=2500."""
from config.default_config import *
from src.schedulers import (
    rl_pa_scheduler, aco_scheduler, energy_aware_scheduler,
    gwo_scheduler, best_fit_scheduler, fcfs_scheduler,
    round_robin_scheduler, sjf_scheduler,
)
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector


ALGOS = {
    "RLPA": rl_pa_scheduler,
    "ACO": aco_scheduler,
    "GWO": gwo_scheduler,
    "EnergyAware": energy_aware_scheduler,
    "BestFit": best_fit_scheduler,
    "FCFS": fcfs_scheduler,
    "RoundRobin": round_robin_scheduler,
    "SJF": sjf_scheduler,
}


def run_one(name, scheduler_fn, seed=42, pattern="poisson", sim_time=2500):
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
    sim.run(simulation_time=sim_time, workload_pattern=pattern, log_interval=25)
    return m.get_statistics()


UI_METRICS = [
    ("total_energy_kwh", "lower"),
    ("carbon_footprint_kg_baseline", "lower"),
    ("sla_violation_rate", "lower"),
    ("avg_waiting_time", "lower"),
    ("avg_turnaround_time", "lower"),
    ("avg_power_watts", "lower"),
    ("avg_utilization", "higher"),
    ("total_migrations", "lower"),
    ("total_vms_completed", "higher"),
]

for pattern, challenger_name in [("poisson", "ACO"), ("time_varying", "FCFS")]:
    rlpa = run_one("RLPA", rl_pa_scheduler, pattern=pattern)
    chal = run_one(challenger_name, ALGOS[challenger_name], pattern=pattern)

    rlpa_wins = 0
    chal_wins = 0
    ties = 0
    print(f"\n{'='*80}")
    print(f"RLPA vs {challenger_name} | pattern={pattern} | sim_time=2500")
    print(f"{'='*80}")
    for key, direction in UI_METRICS:
        rv = rlpa.get(key, 0)
        cv = chal.get(key, 0)
        if direction == "lower":
            if rv < cv: w = "RLPA"; rlpa_wins += 1
            elif cv < rv: w = challenger_name; chal_wins += 1
            else: w = "TIE"; ties += 1
        else:
            if rv > cv: w = "RLPA"; rlpa_wins += 1
            elif cv > rv: w = challenger_name; chal_wins += 1
            else: w = "TIE"; ties += 1
        print(f"  {key:<35} RLPA={rv:>10.4f}  {challenger_name}={cv:>10.4f}  -> {w}")
    print(f"\n  RLPA={rlpa_wins}  {challenger_name}={chal_wins}  ties={ties}")
