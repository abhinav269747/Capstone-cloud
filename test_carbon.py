#!/usr/bin/env python3
"""Quick test of carbon footprint calculation."""

from main import run_scenario
from src.schedulers import rl_pa_scheduler

metrics = run_scenario(rl_pa_scheduler, "RLPA_carbon_test", "poisson")
stats = metrics.get_statistics()

print("\n" + "="*60)
print("CARBON FOOTPRINT TEST")
print("="*60)
print(f"Total Energy:       {stats['total_energy_kwh']:.2f} kWh")
print(f"Carbon (Grid):      {stats['carbon_footprint_kg_baseline']:.2f} kg CO2e")
print(f"Carbon (30% Renew): {stats['carbon_footprint_kg_renewable_avg']:.2f} kg CO2e")
print(f"Carbon Savings:     {stats['carbon_footprint_kg_baseline'] - stats['carbon_footprint_kg_renewable_avg']:.2f} kg CO2e (vs grid)")
print("="*60 + "\n")
