import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface SimulationOptionsResponse {
  schedulers: string[];
  comparison_challengers: string[];
  workload_patterns: string[];
  vm_profiles: Record<string, unknown>;
  defaults: {
    simulation_time: number;
    log_interval: number;
    arrival_rate: number;
    service_time_mean: number;
    service_time_std: number;
    random_seed: number;
  };
}

export interface SimulationStartRequest {
  scheduler: string;
  workload_pattern: 'poisson' | 'bursty' | 'time_varying';
  simulation_time: number;
  log_interval: number;
  arrival_rate: number;
  service_time_mean: number;
  service_time_std: number;
  random_seed: number;
  realtime_delay_seconds: number;
}

export interface LoadInjectionRequest {
  vm_profile: string;
  vm_count: number;
  priority: number;
}

export interface ComparisonRequest {
  challenger_scheduler: string;
  workload_pattern: 'poisson' | 'bursty' | 'time_varying';
  simulation_time: number;
  log_interval: number;
  arrival_rate: number;
  service_time_mean: number;
  service_time_std: number;
  random_seed: number;
}

export interface ComparisonMetricRow {
  rlpa: number;
  challenger: number;
  delta_challenger_minus_rlpa: number;
  delta_pct_vs_rlpa: number;
}

export interface ComparisonResponse {
  baseline_scheduler: 'RLPA';
  challenger_scheduler: string;
  rlpa: Record<string, number>;
  challenger: Record<string, number>;
  metrics: Record<string, ComparisonMetricRow>;
  settings: ComparisonRequest;
}

export interface SimulationJobSnapshot {
  job_id: string;
  status: string;
  created_at: number;
  updated_at: number;
  request: SimulationStartRequest;
  latest_payload?: SimulationStreamEvent;
  latest_results?: Record<string, number>;
  error?: string | null;
  recent_events: SimulationStreamEvent[];
}

export interface SimulationStreamEvent {
  job_id: string;
  event_type: string;
  time?: number;
  status?: string;
  details?: Record<string, unknown>;
  snapshot?: {
    time: number;
    pending_vm_count: number;
    running_vm_count: number;
    completed_vm_count: number;
    is_finished: boolean;
    servers: Array<Record<string, unknown>>;
    latest_metrics: Record<string, number>;
    recent_events: Array<Record<string, unknown>>;
  };
}

@Injectable({ providedIn: 'root' })
export class SimulationApiService {
  private readonly baseUrl = '/api';

  constructor(private readonly http: HttpClient) {}

  getOptions(): Observable<SimulationOptionsResponse> {
    return this.http.get<SimulationOptionsResponse>(`${this.baseUrl}/options`);
  }

  startSimulation(payload: SimulationStartRequest): Observable<SimulationJobSnapshot> {
    return this.http.post<SimulationJobSnapshot>(`${this.baseUrl}/simulations`, payload);
  }

  getSimulation(jobId: string): Observable<SimulationJobSnapshot> {
    return this.http.get<SimulationJobSnapshot>(`${this.baseUrl}/simulations/${jobId}`);
  }

  injectLoad(jobId: string, payload: LoadInjectionRequest): Observable<Record<string, unknown>> {
    return this.http.post<Record<string, unknown>>(`${this.baseUrl}/simulations/${jobId}/inject-load`, payload);
  }

  compareAlgorithms(payload: ComparisonRequest): Observable<ComparisonResponse> {
    return this.http.post<ComparisonResponse>(`${this.baseUrl}/compare`, payload);
  }

  createEventSource(jobId: string): EventSource {
    return new EventSource(`${this.baseUrl}/simulations/${jobId}/events`);
  }
}
