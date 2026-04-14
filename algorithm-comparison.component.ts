import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { SimulationApiService, ComparisonRequest, ComparisonResponse } from './simulation-api.service';

@Component({
  selector: 'app-algorithm-comparison',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './algorithm-comparison.component.html',
  styleUrl: './algorithm-comparison.component.css',
})
export class AlgorithmComparisonComponent implements OnInit {
  challengers: string[] = [];
  running = false;
  error = '';
  result?: ComparisonResponse;

  form: ComparisonRequest = {
    challenger_scheduler: 'FCFS',
    workload_pattern: 'poisson',
    simulation_time: 2500,
    log_interval: 25,
    arrival_rate: 5,
    service_time_mean: 100,
    service_time_std: 30,
    random_seed: 42,
  };

  readonly metricOrder = [
    'total_energy_kwh',
    'carbon_footprint_kg_baseline',
    'sla_violation_rate',
    'avg_waiting_time',
    'avg_turnaround_time',
    'avg_power_watts',
    'avg_utilization',
    'total_migrations',
    'total_vms_completed',
  ];

  readonly labels: Record<string, string> = {
    total_energy_kwh: 'Total Energy (kWh)',
    carbon_footprint_kg_baseline: 'Carbon (kg CO2e)',
    sla_violation_rate: 'SLA Violation Rate',
    avg_waiting_time: 'Avg Waiting Time',
    avg_turnaround_time: 'Avg Turnaround Time',
    avg_power_watts: 'Avg Power (W)',
    avg_utilization: 'Avg Utilization',
    total_migrations: 'Total Migrations',
    total_vms_completed: 'VMs Completed',
  };

  constructor(private readonly api: SimulationApiService) {}

  ngOnInit(): void {
    this.api.getOptions().subscribe({
      next: (options) => {
        this.challengers = options.comparison_challengers ?? options.schedulers.filter((s) => s !== 'RLPA');
        this.form.challenger_scheduler = this.challengers[0] ?? 'FCFS';
        this.form.workload_pattern = 'poisson';
        this.form.simulation_time = 2500;
        this.form.log_interval = 25;
        this.form.arrival_rate = options.defaults.arrival_rate;
        this.form.service_time_mean = options.defaults.service_time_mean;
        this.form.service_time_std = options.defaults.service_time_std;
        this.form.random_seed = options.defaults.random_seed;
      },
      error: () => {
        this.error = 'Unable to load comparison options from backend.';
      },
    });
  }

  compare(): void {
    this.error = '';
    this.running = true;
    this.result = undefined;

    this.api.compareAlgorithms(this.form).subscribe({
      next: (response) => {
        this.result = response;
        this.running = false;
      },
      error: () => {
        this.error = 'Comparison failed. Check backend status and try again.';
        this.running = false;
      },
    });
  }

  metric(key: string): { rlpa: number; challenger: number; delta: number; deltaPct: number } {
    const row = this.result?.metrics[key];
    if (!row) {
      return { rlpa: 0, challenger: 0, delta: 0, deltaPct: 0 };
    }
    return {
      rlpa: row.rlpa,
      challenger: row.challenger,
      delta: row.delta_challenger_minus_rlpa,
      deltaPct: row.delta_pct_vs_rlpa,
    };
  }

  isCostMetric(key: string): boolean {
    return ['total_energy_kwh', 'carbon_footprint_kg_baseline', 'sla_violation_rate', 'avg_waiting_time', 'avg_turnaround_time', 'avg_power_watts', 'total_migrations'].includes(key);
  }

  deltaClass(key: string): string {
    const delta = this.metric(key).delta;
    if (delta === 0) {
      return 'neutral';
    }
    // For cost metrics, positive delta means challenger is worse than RLPA.
    if (this.isCostMetric(key)) {
      return delta > 0 ? 'better-rlpa' : 'better-challenger';
    }
    // For benefit metrics, positive delta means challenger is better than RLPA.
    return delta > 0 ? 'better-challenger' : 'better-rlpa';
  }

  fmt(key: string, value: number): string {
    if (key.includes('rate') || key.includes('utilization')) {
      return `${(value * 100).toFixed(2)}%`;
    }
    if (Number.isInteger(value)) {
      return `${value}`;
    }
    return value.toFixed(2);
  }
}
