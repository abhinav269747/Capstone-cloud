"""Verify RLPA is deterministic and beats ACO consistently."""
from config.default_config import *
from src.schedulers import rl_pa_scheduler, aco_scheduler
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector


def run_pair(seed=42):
    results = {}
    for name, fn in [("RLPA", rl_pa_scheduler), ("ACO", aco_scheduler)]:
        m = MetricsCollector(simulation_name=name)
        wg = WorkloadGenerator(
            arrival_rate=WORKLOAD_ARRIVAL_RATE, vm_profiles=VM_PROFILES,
            service_time_mean=SERVICE_TIME_MEAN, service_time_std=SERVICE_TIME_STD,
            random_seed=seed,
        )
        sim = CloudDataCenterSimulation(
            servers=PHYSICAL_SERVERS, vm_profiles=VM_PROFILES, workload_generator=wg,
            scheduler=fn, max_sla_time=MAX_WAIT_TIME, metrics_collector=m, random_seed=seed,
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
        sim.run(simulation_time=SIMULATION_TIME, workload_pattern="poisson", log_interval=50000)
        results[name] = m.get_statistics()
    return results


print("=== RUN 1 ===")
r1 = run_pair()
print(f"RLPA energy={r1['RLPA']['total_energy_kwh']:.4f}  ACO energy={r1['ACO']['total_energy_kwh']:.4f}")
print(f"RLPA carbon={r1['RLPA']['carbon_footprint_kg_baseline']:.4f}  ACO carbon={r1['ACO']['carbon_footprint_kg_baseline']:.4f}")
print(f"RLPA wait={r1['RLPA']['avg_waiting_time']:.4f}  ACO wait={r1['ACO']['avg_waiting_time']:.4f}")

print()
print("=== RUN 2 (identical settings) ===")
r2 = run_pair()
print(f"RLPA energy={r2['RLPA']['total_energy_kwh']:.4f}  ACO energy={r2['ACO']['total_energy_kwh']:.4f}")
print(f"RLPA carbon={r2['RLPA']['carbon_footprint_kg_baseline']:.4f}  ACO carbon={r2['ACO']['carbon_footprint_kg_baseline']:.4f}")
print(f"RLPA wait={r2['RLPA']['avg_waiting_time']:.4f}  ACO wait={r2['ACO']['avg_waiting_time']:.4f}")

print()
match = r1["RLPA"]["total_energy_kwh"] == r2["RLPA"]["total_energy_kwh"]
print(f"DETERMINISTIC: {match}")

lower_better = ["total_energy_kwh", "carbon_footprint_kg_baseline", "avg_waiting_time", "avg_power_watts", "sla_violation_rate"]
rlpa_wins = sum(1 for k in lower_better if r1["RLPA"][k] <= r1["ACO"][k])
print(f"RLPA wins (lower-better metrics): {rlpa_wins}/{len(lower_better)}")
