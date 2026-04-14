import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  SimulationApiService,
  ComparisonRequest,
  ComparisonResponse,
} from './simulation-api.service';

interface ChartPoint {
  label: string;
  rlpa: number;
  challenger: number;
}

@Component({
  selector: 'app-comparison-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './comparison-page.component.html',
  styleUrl: './comparison-page.component.css',
})
export class ComparisonPageComponent implements OnInit {
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

  /** Description of why lower/higher matters for each metric */
  readonly metricInsight: Record<string, string> = {
    total_energy_kwh: 'Lower is better - less energy consumed.',
    carbon_footprint_kg_baseline: 'Lower is better - less carbon emitted.',
    sla_violation_rate: 'Lower is better - fewer SLA breaches.',
    avg_waiting_time: 'Lower is better - tasks wait less in queue.',
    avg_turnaround_time: 'Lower is better - tasks complete faster.',
    avg_power_watts: 'Lower is better - less average power draw.',
    avg_utilization: 'Higher is better - resources used efficiently.',
    total_migrations: 'Lower is better - fewer disruptive migrations.',
    total_vms_completed: 'Higher is better - more work done.',
  };

  /** Bar-chart data for visual overview */
  get chartPoints(): ChartPoint[] {
    if (!this.result) return [];
    return this.metricOrder.map((key) => ({
      label: this.labels[key],
      rlpa: this.result!.metrics[key]?.rlpa ?? 0,
      challenger: this.result!.metrics[key]?.challenger ?? 0,
    }));
  }

  constructor(private readonly api: SimulationApiService) {}

  ngOnInit(): void {
    this.api.getOptions().subscribe({
      next: (options) => {
        this.challengers =
          options.comparison_challengers ??
          options.schedulers.filter((s) => s !== 'RLPA');
        this.form.challenger_scheduler = this.challengers[0] ?? 'FCFS';
        this.form.arrival_rate = options.defaults.arrival_rate;
        this.form.service_time_mean = options.defaults.service_time_mean;
        this.form.service_time_std = options.defaults.service_time_std;
        this.form.random_seed = options.defaults.random_seed;
      },
      error: () => {
        this.error = 'Unable to load options from backend.';
      },
    });
  }

  compare(): void {
    this.error = '';
    this.running = true;
    this.result = undefined;
    this.api.compareAlgorithms(this.form).subscribe({
      next: (res) => {
        this.result = res;
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
    if (!row) return { rlpa: 0, challenger: 0, delta: 0, deltaPct: 0 };
    return {
      rlpa: row.rlpa,
      challenger: row.challenger,
      delta: row.delta_challenger_minus_rlpa,
      deltaPct: row.delta_pct_vs_rlpa,
    };
  }

  isCostMetric(key: string): boolean {
    return [
      'total_energy_kwh',
      'carbon_footprint_kg_baseline',
      'sla_violation_rate',
      'avg_waiting_time',
      'avg_turnaround_time',
      'avg_power_watts',
      'total_migrations',
    ].includes(key);
  }

  /**
   * Returns 'rlpa-wins', 'challenger-wins', or 'tie' from RLPA's perspective.
   */
  winner(key: string): 'rlpa-wins' | 'challenger-wins' | 'tie' {
    const delta = this.metric(key).delta;
    if (delta === 0) return 'tie';
    if (this.isCostMetric(key)) {
      // positive delta means challenger has higher cost, so RLPA wins.
      return delta > 0 ? 'rlpa-wins' : 'challenger-wins';
    }
    // For benefit metrics, positive delta means challenger is better.
    return delta > 0 ? 'challenger-wins' : 'rlpa-wins';
  }

  winnerLabel(key: string): string {
    const w = this.winner(key);
    if (w === 'tie') return 'Tied';
    if (w === 'rlpa-wins') return 'RLPA better';
    return `${this.result?.challenger_scheduler ?? 'Challenger'} better`;
  }

  fmt(key: string, value: number): string {
    if (key.includes('rate') || key.includes('utilization')) return `${(value * 100).toFixed(2)}%`;
    if (Number.isInteger(value)) return `${value}`;
    return value.toFixed(2);
  }

  /** Derive a human-readable difference sentence */
  diffSentence(key: string): string {
    const m = this.metric(key);
    const w = this.winner(key);
    const absDelta = Math.abs(m.delta);
    const absPct = Math.abs(m.deltaPct).toFixed(1);
    const label = this.labels[key];
    const challenger = this.result?.challenger_scheduler ?? 'Challenger';

    if (w === 'tie') return `${label} is identical for both algorithms.`;

    const better = w === 'rlpa-wins' ? 'RLPA' : challenger;
    const worse = w === 'rlpa-wins' ? challenger : 'RLPA';
    const fmtted = this.fmt(key, absDelta);
    return `${better} outperforms ${worse} by ${fmtted} (${absPct}%) on ${label}.`;
  }

  /** CSS-based grouped bar chart data.
   *  Each metric is independently normalized with a min-height floor
   *  so every bar is always visible regardless of magnitude differences. */
  get chartGroups(): Array<{
    key: string; label: string;
    rlpa: number; challenger: number;
    rlpaPx: number; challengerPx: number;
    winner: string;
  }> {
    if (!this.result) return [];
    const CHART_H = 160; // usable bar area in px
    const MIN_H   = 24;  // minimum bar height in px
    const scale = (v: number, max: number) =>
      Math.round(MIN_H + (v / max) * (CHART_H - MIN_H));

    return this.metricOrder.map((key) => {
      const r = this.result!.metrics[key]?.rlpa ?? 0;
      const c = this.result!.metrics[key]?.challenger ?? 0;
      const localMax = Math.max(r, c, 1);
      return {
        key,
        label: this.labels[key],
        rlpa: r,
        challenger: c,
        rlpaPx: scale(r, localMax),
        challengerPx: scale(c, localMax),
        winner: this.winner(key),
      };
    });
  }

  get rlpaWinsCount(): number {
    return this.metricOrder.filter((k) => this.winner(k) === 'rlpa-wins').length;
  }

  get challengerWinsCount(): number {
    return this.metricOrder.filter((k) => this.winner(k) === 'challenger-wins').length;
  }
}
