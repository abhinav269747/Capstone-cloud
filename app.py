from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from config.default_config import (
    DETAILED_LOGGING,
    ENABLE_SERVER_POWER_STATES,
    ENABLE_VM_MIGRATION,
    LOG_INTERVAL,
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
    RANDOM_SEED,
    SERVER_IDLE_SHUTDOWN_TIME,
    SERVER_MIGRATION_COOLDOWN,
    SERVER_POWER_STATE_CHECK_INTERVAL,
    SERVER_WAKEUP_DELAY,
    SERVER_WAKEUP_ENERGY_KWH,
    SERVICE_TIME_MEAN,
    SERVICE_TIME_STD,
    SIMULATION_TIME,
    VM_MIGRATION_COOLDOWN,
    VM_PROFILES,
    WORKLOAD_ARRIVAL_RATE,
)
from src.schedulers import (
    RLPA_WEIGHTS_PATH,
    aco_scheduler,
    best_fit_scheduler,
    energy_aware_scheduler,
    fcfs_scheduler,
    gwo_scheduler,
    rl_pa_scheduler,
    round_robin_scheduler,
    save_rlpa_weights,
    sjf_scheduler,
)
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector

SCHEDULERS = {
    "FCFS": fcfs_scheduler,
    "RoundRobin": round_robin_scheduler,
    "BestFit": best_fit_scheduler,
    "EnergyAware": energy_aware_scheduler,
    "SJF": sjf_scheduler,
    "ACO": aco_scheduler,
    "GWO": gwo_scheduler,
    "RLPA": rl_pa_scheduler,
}

WORKLOAD_PATTERNS = ["poisson", "bursty", "time_varying"]


class SimulationStartRequest(BaseModel):
    scheduler: str = Field(default="RLPA")
    workload_pattern: Literal["poisson", "bursty", "time_varying"] = "poisson"
    simulation_time: float = Field(default=SIMULATION_TIME, gt=0)
    log_interval: float = Field(default=25.0, gt=0)
    arrival_rate: float = Field(default=WORKLOAD_ARRIVAL_RATE, gt=0)
    service_time_mean: float = Field(default=SERVICE_TIME_MEAN, gt=0)
    service_time_std: float = Field(default=SERVICE_TIME_STD, ge=0)
    random_seed: int = RANDOM_SEED
    realtime_delay_seconds: float = Field(default=0.15, ge=0)


class LoadInjectionRequest(BaseModel):
    vm_profile: str
    vm_count: int = Field(default=1, ge=1, le=20)
    priority: int = Field(default=1, ge=1, le=10)


class ComparisonRequest(BaseModel):
    challenger_scheduler: str
    workload_pattern: Literal["poisson", "bursty", "time_varying"] = "poisson"
    simulation_time: float = Field(default=SIMULATION_TIME, gt=0)
    log_interval: float = Field(default=25.0, gt=0)
    arrival_rate: float = Field(default=WORKLOAD_ARRIVAL_RATE, gt=0)
    service_time_mean: float = Field(default=SERVICE_TIME_MEAN, gt=0)
    service_time_std: float = Field(default=SERVICE_TIME_STD, ge=0)
    random_seed: int = RANDOM_SEED


@dataclass
class SimulationJob:
    job_id: str
    request: SimulationStartRequest
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    latest_payload: dict = field(default_factory=dict)
    latest_results: Optional[dict] = None
    error: Optional[str] = None
    simulation: Optional[CloudDataCenterSimulation] = None
    metrics: Optional[MetricsCollector] = None
    events: List[dict] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def append_event(self, payload: dict):
        with self.lock:
            self.events.append(payload)
            self.latest_payload = payload
            self.updated_at = time.time()

    def snapshot(self) -> dict:
        with self.lock:
            recent_events = self.events[-20:]
            latest_payload = self.latest_payload
        return {
            "job_id": self.job_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "request": self.request.model_dump(),
            "latest_payload": latest_payload,
            "latest_results": self.latest_results,
            "error": self.error,
            "recent_events": recent_events,
        }


class SimulationJobManager:
    def __init__(self) -> None:
        self.jobs: Dict[str, SimulationJob] = {}
        self.lock = threading.Lock()

    def create_job(self, request: SimulationStartRequest) -> SimulationJob:
        if request.scheduler not in SCHEDULERS:
            raise HTTPException(status_code=400, detail=f"Unknown scheduler: {request.scheduler}")

        job = SimulationJob(job_id=str(uuid.uuid4()), request=request)
        with self.lock:
            self.jobs[job.job_id] = job

        thread = threading.Thread(target=self._run_job, args=(job,), daemon=True)
        thread.start()
        return job

    def get_job(self, job_id: str) -> SimulationJob:
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Simulation job not found")
        return job

    def _run_job(self, job: SimulationJob):
        job.status = "running"
        job.updated_at = time.time()

        request = job.request
        metrics = MetricsCollector(simulation_name=f"{request.scheduler}_interactive_{request.workload_pattern}")
        workload_gen = WorkloadGenerator(
            arrival_rate=request.arrival_rate,
            vm_profiles=VM_PROFILES,
            service_time_mean=request.service_time_mean,
            service_time_std=request.service_time_std,
            random_seed=request.random_seed,
        )

        def progress_callback(payload: dict):
            payload["job_id"] = job.job_id
            job.append_event(payload)

        simulation = CloudDataCenterSimulation(
            servers=PHYSICAL_SERVERS,
            vm_profiles=VM_PROFILES,
            workload_generator=workload_gen,
            scheduler=SCHEDULERS[request.scheduler],
            max_sla_time=MAX_WAIT_TIME,
            metrics_collector=metrics,
            random_seed=request.random_seed,
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
            progress_callback=progress_callback,
            realtime_delay_seconds=request.realtime_delay_seconds,
        )

        # Freeze RLPA policy during interactive runs for deterministic results.
        simulation._rl_pa_inference_mode = True

        job.simulation = simulation
        job.metrics = metrics

        try:
            simulation.run(
                simulation_time=request.simulation_time,
                workload_pattern=request.workload_pattern,
                log_interval=request.log_interval,
            )
            # Interactive runs do NOT save weights — only pretraining does.
            job.latest_results = metrics.get_statistics()
            job.status = "completed"
            job.updated_at = time.time()
        except Exception as exc:  # pragma: no cover - surfaced through API
            job.error = str(exc)
            job.status = "failed"
            job.updated_at = time.time()
            job.append_event({
                "job_id": job.job_id,
                "event_type": "simulation_failed",
                "time": 0.0,
                "details": {"error": str(exc)},
                "snapshot": simulation.get_snapshot() if simulation else {},
            })


def _run_single_simulation(request: SimulationStartRequest) -> dict:
    """Run a simulation synchronously and return final stats."""
    metrics = MetricsCollector(simulation_name=f"{request.scheduler}_comparison_{request.workload_pattern}")
    workload_gen = WorkloadGenerator(
        arrival_rate=request.arrival_rate,
        vm_profiles=VM_PROFILES,
        service_time_mean=request.service_time_mean,
        service_time_std=request.service_time_std,
        random_seed=request.random_seed,
    )

    simulation = CloudDataCenterSimulation(
        servers=PHYSICAL_SERVERS,
        vm_profiles=VM_PROFILES,
        workload_generator=workload_gen,
        scheduler=SCHEDULERS[request.scheduler],
        max_sla_time=MAX_WAIT_TIME,
        metrics_collector=metrics,
        random_seed=request.random_seed,
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

    # Freeze RLPA policy during comparison runs for deterministic results.
    simulation._rl_pa_inference_mode = True

    simulation.run(
        simulation_time=request.simulation_time,
        workload_pattern=request.workload_pattern,
        log_interval=request.log_interval,
    )
    # Comparison / interactive runs use frozen policy — do NOT save weights.
    return metrics.get_statistics()


def _build_comparison_payload(rlpa_stats: dict, challenger_stats: dict, challenger_name: str) -> dict:
    """Create comparable metric table and deltas where negative means RLPA is better for cost metrics."""
    keys = [
        "total_energy_kwh",
        "carbon_footprint_kg_baseline",
        "sla_violation_rate",
        "avg_waiting_time",
        "avg_turnaround_time",
        "avg_power_watts",
        "avg_utilization",
        "total_migrations",
        "total_vms_completed",
    ]

    metric_comparison = {}
    for key in keys:
        rlpa_value = float(rlpa_stats.get(key, 0.0))
        challenger_value = float(challenger_stats.get(key, 0.0))
        delta = challenger_value - rlpa_value
        delta_pct = (delta / rlpa_value * 100.0) if rlpa_value != 0 else 0.0
        metric_comparison[key] = {
            "rlpa": rlpa_value,
            "challenger": challenger_value,
            "delta_challenger_minus_rlpa": delta,
            "delta_pct_vs_rlpa": delta_pct,
        }

    return {
        "baseline_scheduler": "RLPA",
        "challenger_scheduler": challenger_name,
        "rlpa": rlpa_stats,
        "challenger": challenger_stats,
        "metrics": metric_comparison,
    }


job_manager = SimulationJobManager()
app = FastAPI(title="Green Cloud Simulation API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/options")
def get_options():
    challengers = sorted([name for name in SCHEDULERS.keys() if name != "RLPA"])
    return {
        "schedulers": sorted(SCHEDULERS.keys()),
        "comparison_challengers": challengers,
        "workload_patterns": WORKLOAD_PATTERNS,
        "vm_profiles": VM_PROFILES,
        "defaults": {
            "simulation_time": SIMULATION_TIME,
            "log_interval": LOG_INTERVAL,
            "arrival_rate": WORKLOAD_ARRIVAL_RATE,
            "service_time_mean": SERVICE_TIME_MEAN,
            "service_time_std": SERVICE_TIME_STD,
            "random_seed": RANDOM_SEED,
        },
    }


@app.post("/api/simulations")
def start_simulation(request: SimulationStartRequest):
    job = job_manager.create_job(request)
    return job.snapshot()


@app.get("/api/simulations/{job_id}")
def get_simulation(job_id: str):
    return job_manager.get_job(job_id).snapshot()


@app.get("/api/simulations/{job_id}/results")
def get_simulation_results(job_id: str):
    job = job_manager.get_job(job_id)
    if job.latest_results is None:
        raise HTTPException(status_code=409, detail="Simulation results are not available yet")
    return {
        "job_id": job.job_id,
        "status": job.status,
        "results": job.latest_results,
        "snapshot": job.simulation.get_snapshot() if job.simulation else {},
    }


@app.post("/api/simulations/{job_id}/inject-load")
def inject_load(job_id: str, request: LoadInjectionRequest):
    job = job_manager.get_job(job_id)
    if job.status != "running" or job.simulation is None:
        raise HTTPException(status_code=409, detail="Simulation is not running")
    if request.vm_profile not in VM_PROFILES:
        raise HTTPException(status_code=400, detail=f"Unknown VM profile: {request.vm_profile}")

    workload_request = job.simulation.inject_request(
        vm_profile=request.vm_profile,
        vm_count=request.vm_count,
        priority=request.priority,
    )
    return {
        "job_id": job.job_id,
        "status": job.status,
        "request_id": workload_request.request_id,
        "simulation_time": job.simulation.env.now,
    }


@app.get("/api/simulations/{job_id}/events")
def stream_simulation_events(job_id: str):
    job = job_manager.get_job(job_id)

    def event_generator():
        last_index = 0
        while True:
            with job.lock:
                new_events = job.events[last_index:]
                last_index = len(job.events)
                status = job.status
            for event in new_events:
                yield f"data: {json.dumps(event)}\n\n"
            if status in {"completed", "failed"} and not new_events:
                terminal_payload = {"job_id": job.job_id, "event_type": "stream_closed", "status": status}
                yield f"data: {json.dumps(terminal_payload)}\n\n"
                break
            time.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/compare")
def compare_algorithms(request: ComparisonRequest):
    if request.challenger_scheduler == "RLPA":
        raise HTTPException(status_code=400, detail="Challenger must be different from RLPA")
    if request.challenger_scheduler not in SCHEDULERS:
        raise HTTPException(status_code=400, detail=f"Unknown scheduler: {request.challenger_scheduler}")

    rlpa_request = SimulationStartRequest(
        scheduler="RLPA",
        workload_pattern=request.workload_pattern,
        simulation_time=request.simulation_time,
        log_interval=request.log_interval,
        arrival_rate=request.arrival_rate,
        service_time_mean=request.service_time_mean,
        service_time_std=request.service_time_std,
        random_seed=request.random_seed,
        realtime_delay_seconds=0.0,
    )
    challenger_request = SimulationStartRequest(
        scheduler=request.challenger_scheduler,
        workload_pattern=request.workload_pattern,
        simulation_time=request.simulation_time,
        log_interval=request.log_interval,
        arrival_rate=request.arrival_rate,
        service_time_mean=request.service_time_mean,
        service_time_std=request.service_time_std,
        random_seed=request.random_seed,
        realtime_delay_seconds=0.0,
    )

    rlpa_stats = _run_single_simulation(rlpa_request)
    challenger_stats = _run_single_simulation(challenger_request)
    payload = _build_comparison_payload(rlpa_stats, challenger_stats, request.challenger_scheduler)
    payload["settings"] = request.model_dump()
    return payload


# ── RLPA Pre-training ────────────────────────────────────────────────────────

def _run_pretrain_background() -> None:
    """Background worker: run RLPA pre-training from batch_task trace data."""
    try:
        from src.rl_pretrain import pretrain_from_trace
        pretrain_from_trace(verbose=True)
    except Exception as exc:
        print(f"[RLPA Pretrain] Background pre-training failed: {exc}")


@app.on_event("startup")
def startup_pretrain() -> None:
    """
    Auto-run RLPA pre-training in the background when the server starts,
    but only if no weights file exists yet.  This seeds the agent with
    batch_task trace experience before any live comparison is made.
    """
    if not os.path.exists(RLPA_WEIGHTS_PATH):
        print("[RLPA Pretrain] No weights found — starting background pre-training …")
        t = threading.Thread(target=_run_pretrain_background, daemon=True)
        t.start()
    else:
        print(f"[RLPA Pretrain] Loaded existing weights from {RLPA_WEIGHTS_PATH}")


@app.post("/api/pretrain")
def trigger_pretrain():
    """
    Manually trigger RLPA pre-training from batch_task trace data.
    Runs in the background; weights accumulate across calls.
    """
    t = threading.Thread(target=_run_pretrain_background, daemon=True)
    t.start()
    return {
        "status": "pretraining_started",
        "message": "RLPA pre-training running in background. Weights will be ready in ~1 minute.",
        "weights_path": RLPA_WEIGHTS_PATH,
        "existing_weights": os.path.exists(RLPA_WEIGHTS_PATH),
    }


@app.get("/api/pretrain/status")
def pretrain_status():
    """Return current RLPA pre-training / weights status."""
    exists = os.path.exists(RLPA_WEIGHTS_PATH)
    info: dict = {"weights_exist": exists, "weights_path": RLPA_WEIGHTS_PATH}
    if exists:
        try:
            import json as _json
            with open(RLPA_WEIGHTS_PATH) as fh:
                data = _json.load(fh)
            info["total_updates"] = data.get("total_updates", 0)
            info["epsilon"] = data.get("epsilon", None)
        except Exception:
            pass
    return info
