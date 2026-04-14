# Green Cloud Data Center with AI Optimization

A SimPy-based simulator for evaluating resource scheduling algorithms in energy-aware cloud data centers.

## Project Overview

This project compares various scheduling algorithms to optimize virtual machine (VM) placement in cloud data centers while minimizing energy consumption and maintaining Quality of Service (QoS).

**Algorithms Evaluated:**
- FCFS (First-Come-First-Served)
- Best-Fit Placement
- Energy-Aware Scheduling
- SJF (Shortest-Job-First)
- ACO (Ant Colony Optimization)
- GWO (Grey Wolf Optimization)
- RL + Predictive Analysis (RL+PA)

## Project Structure

```
capstone_project/
├── backend/
│   ├── app.py                     # FastAPI REST API and SSE event stream
│   └── run.py                     # Backend launcher
├── config/
│   └── default_config.py          # Configuration parameters
├── frontend/                      # Angular dashboard for interactive control
├── src/
│   ├── models.py                  # Core data models (Server, VM, Request)
│   ├── workload_generator.py      # Workload and request generation
│   ├── simulation_engine.py       # Main SimPy simulation engine
│   └── schedulers.py              # Placement algorithms
├── utils/
│   └── metrics.py                 # Metrics collection and tracking
├── output/                        # Simulation results (JSON)
├── tests/                         # Unit tests
├── main.py                        # Entry point: run comparisons
└── README.md                      # This file
```

## Key Concepts

### Data Models

**PhysicalServer**: Heterogeneous physical machines with:
- Unique CPU, GPU, RAM capacity
- Individual power models (idle and peak power)
- Energy efficiency factor

**VirtualMachine**: Job/request with:
- Resource requirements (CPU, GPU, RAM)
- Service time (execution duration)
- SLA deadline
- Lifecycle states (pending, running, completed)

**WorkloadRequest**: Incoming request that may spawn multiple VMs

### Simulation Flow

1. **Arrival**: Requests arrive according to a workload pattern (Poisson, bursty, time-varying)
2. **Queuing**: VMs wait in queue until scheduled
3. **Placement**: Scheduler algorithm assigns VMs to physical servers
4. **Execution**: VMs run for their service time
5. **Completion**: VMs finish; server resources freed
6. **Metrics**: Energy, SLA violations, waiting time tracked throughout

## Getting Started

### Installation

```bash
pip install -r requirements.txt
```

### Running Simulations

```bash
python main.py
```

This runs all baseline algorithms on the same scenario and prints a comparison table.

### Running the Interactive Stack

Backend API:

```bash
python -m backend.run
```

Frontend dashboard:

```bash
cd frontend
npm install
npm start
```

Open `http://localhost:4200` to start simulations, watch live power/queue/utilization charts,
inspect per-server state, and inject manual VM load while a run is active.

### REST API

- `GET /api/options`: available schedulers, workload patterns, VM profiles, and defaults
- `POST /api/simulations`: start a background simulation job
- `GET /api/simulations/{job_id}`: current job state and recent events
- `GET /api/simulations/{job_id}/results`: final results after completion
- `POST /api/simulations/{job_id}/inject-load`: inject manual VM load into a running simulation
- `GET /api/simulations/{job_id}/events`: Server-Sent Events stream for live frontend updates

### Configuration

Edit `config/default_config.py` to customize:
- Data center servers and capacities
- Workload arrival rate and patterns
- VM resource profiles
- SLA and QoS targets
- Simulation duration

## Metrics Collected

- **Energy**: Total kWh, average power, peak power
- **Carbon**: Grid-only and renewable-adjusted CO2e estimates
- **Performance**: Average waiting time, turnaround time
- **Availability**: VMs completed, success rate
- **SLA**: Violation count and rate
- **Utilization**: Server CPU/GPU/RAM utilization

## Frontend Features

- Interactive simulation launch form with scheduler and workload controls
- Real-time power, queue, and utilization sparklines from the event stream
- Per-server rack view with power state and VM counts
- Live event feed showing placements, migrations, wakeups, and completions
- Manual load injection into a running simulation through REST calls

## Next Steps

1. Add trace-replay mode to the interactive API using the Alibaba-style trace reader
2. Add comparison dashboards with side-by-side scheduler runs
3. Persist simulation history in a lightweight database for replay and reporting
4. Add richer charts and filtering for event streams and server timelines
5. Expand automated tests for API endpoints and frontend behaviors

## References

- SimPy Documentation: https://simpy.readthedocs.io/
- Green Data Center Research: Energy-aware scheduling literature
- Reinforcement Learning: Multi-agent scheduling in cloud environments

## License

MIT

## Author

Capstone Project Team
