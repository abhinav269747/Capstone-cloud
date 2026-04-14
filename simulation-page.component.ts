import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  SimulationApiService,
  SimulationJobSnapshot,
  SimulationOptionsResponse,
  SimulationStartRequest,
  SimulationStreamEvent,
} from './simulation-api.service';

@Component({
  selector: 'app-simulation-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './simulation-page.component.html',
  styleUrl: './simulation-page.component.css',
})
export class SimulationPageComponent implements OnInit, OnDestroy {
  readonly runPresets = [
    {
      key: 'quick',
      label: 'Quick Demo',
      values: {
        simulation_time: 800,
        log_interval: 20,
        arrival_rate: 4,
        service_time_mean: 90,
        service_time_std: 25,
        realtime_delay_seconds: 0.1,
      },
    },
    {
      key: 'balanced',
      label: 'Balanced Run',
      values: {
        simulation_time: 3000,
        log_interval: 25,
        arrival_rate: 5,
        service_time_mean: 100,
        service_time_std: 30,
        realtime_delay_seconds: 0.15,
      },
    },
    {
      key: 'stress',
      label: 'Stress Test',
      values: {
        simulation_time: 6000,
        log_interval: 20,
        arrival_rate: 8,
        service_time_mean: 120,
        service_time_std: 45,
        realtime_delay_seconds: 0.07,
      },
    },
  ] as const;

  options?: SimulationOptionsResponse;
  currentJob?: SimulationJobSnapshot;
  currentSnapshot?: SimulationStreamEvent['snapshot'];
  eventSource?: EventSource;
  events: SimulationStreamEvent[] = [];
  powerSeries: Array<{ time: number; value: number }> = [];
  queueSeries: Array<{ time: number; value: number }> = [];
  utilizationSeries: Array<{ time: number; value: number }> = [];
  isStarting = false;
  showAdvanced = false;
  selectedPresetKey: 'quick' | 'balanced' | 'stress' = 'balanced';
  connectionError = '';

  form: SimulationStartRequest = {
    scheduler: 'RLPA',
    workload_pattern: 'poisson',
    simulation_time: 1000,
    log_interval: 20,
    arrival_rate: 5,
    service_time_mean: 100,
    service_time_std: 30,
    random_seed: 42,
    realtime_delay_seconds: 0.15,
  };

  injection = {
    vm_profile: 'cpu_medium',
    vm_count: 2,
    priority: 1,
  };

  constructor(private readonly api: SimulationApiService) {}

  ngOnInit(): void {
    this.api.getOptions().subscribe({
      next: (options) => {
        this.options = options;
        this.form = {
          scheduler: 'RLPA',
          workload_pattern: 'poisson',
          simulation_time: options.defaults.simulation_time,
          log_interval: 25,
          arrival_rate: options.defaults.arrival_rate,
          service_time_mean: options.defaults.service_time_mean,
          service_time_std: options.defaults.service_time_std,
          random_seed: options.defaults.random_seed,
          realtime_delay_seconds: 0.15,
        };
        this.injection.vm_profile = Object.keys(options.vm_profiles)[0] ?? 'cpu_medium';
        this.applyPreset(this.selectedPresetKey);
      },
      error: () => {
        this.connectionError = 'Backend is not reachable. Start the FastAPI server on port 8000.';
      },
    });
  }

  ngOnDestroy(): void {
    this.eventSource?.close();
  }

  startSimulation(): void {
    this.isStarting = true;
    this.connectionError = '';
    this.events = [];
    this.powerSeries = [];
    this.queueSeries = [];
    this.utilizationSeries = [];
    this.currentSnapshot = undefined;
    this.currentJob = undefined;
    this.eventSource?.close();

    this.api.startSimulation(this.form).subscribe({
      next: (job) => {
        this.currentJob = job;
        this.isStarting = false;
        this.attachToJob(job.job_id);
      },
      error: () => {
        this.isStarting = false;
        this.connectionError = 'Failed to start simulation job.';
      },
    });
  }

  applyPreset(presetKey: 'quick' | 'balanced' | 'stress'): void {
    this.selectedPresetKey = presetKey;
    const preset = this.runPresets.find((item) => item.key === presetKey);
    if (!preset) return;
    this.form = { ...this.form, ...preset.values };
  }

  injectLoad(): void {
    if (!this.currentJob) return;
    this.api.injectLoad(this.currentJob.job_id, this.injection).subscribe({
      error: () => {
        this.connectionError = 'Manual load injection failed.';
      },
    });
  }

  attachToJob(jobId: string): void {
    this.eventSource?.close();
    this.eventSource = this.api.createEventSource(jobId);
    this.eventSource.onmessage = (message) => {
      const payload = JSON.parse(message.data) as SimulationStreamEvent;
      if (payload.event_type === 'stream_closed') {
        this.eventSource?.close();
        this.refreshJob(jobId);
        return;
      }
      this.events = [payload, ...this.events].slice(0, 60);
      if (this.currentJob) {
        this.currentJob = { ...this.currentJob, latest_payload: payload, recent_events: this.events };
      }
      if (payload.snapshot) {
        this.currentSnapshot = payload.snapshot;
        if (payload.snapshot.is_finished && this.currentJob) {
          this.currentJob = { ...this.currentJob, status: 'completed' };
        }
      }
      if (payload.event_type === 'metrics_tick' && payload.details && payload.time !== undefined) {
        const totalPower = Number(payload.details['total_power_watts'] ?? 0);
        const queueLength = Number(payload.details['queue_length'] ?? 0);
        const avgUtilization = Number(payload.details['avg_utilization'] ?? 0);
        this.pushSeriesPoint(this.powerSeries, payload.time, totalPower);
        this.pushSeriesPoint(this.queueSeries, payload.time, queueLength);
        this.pushSeriesPoint(this.utilizationSeries, payload.time, avgUtilization * 100);
      }
    };
    this.eventSource.onerror = () => {
      this.connectionError = 'Live event stream disconnected.';
    };
  }

  refreshJob(jobId: string): void {
    this.api.getSimulation(jobId).subscribe({
      next: (job) => {
        this.currentJob = job;
      },
    });
  }

  pushSeriesPoint(series: Array<{ time: number; value: number }>, time: number, value: number): void {
    series.push({ time, value });
    if (series.length > 40) series.shift();
  }

  sparkline(series: Array<{ time: number; value: number }>, width = 320, height = 110): string {
    if (series.length === 0) return '';
    const minValue = Math.min(...series.map((p) => p.value));
    const maxValue = Math.max(...series.map((p) => p.value));
    const spread = maxValue - minValue || 1;
    return series
      .map((p, i) => {
        const x = (i / Math.max(1, series.length - 1)) * width;
        const y = height - ((p.value - minValue) / spread) * height;
        return `${x},${y}`;
      })
      .join(' ');
  }

  get latestMetrics(): Record<string, number> {
    return this.currentSnapshot?.latest_metrics ?? this.currentJob?.latest_results ?? {};
  }

  get recentServers(): Array<Record<string, unknown>> {
    return this.currentSnapshot?.servers ?? [];
  }

  get hasRunningJob(): boolean {
    return this.currentJob?.status === 'running';
  }

  metricValue(key: string): number {
    const value = this.latestMetrics[key];
    return typeof value === 'number' ? value : 0;
  }

  serverNumber(server: Record<string, unknown>, key: string): number {
    const value = server[key];
    return typeof value === 'number' ? value : 0;
  }

  serverText(server: Record<string, unknown>, key: string): string {
    const value = server[key];
    return typeof value === 'string' ? value : '';
  }

  serverDisplayName(server: Record<string, unknown>): string {
    return this.serverText(server, 'name').replace(/_/g, ' ');
  }

  serverBool(server: Record<string, unknown>, key: string): boolean {
    return server[key] === true;
  }
}
