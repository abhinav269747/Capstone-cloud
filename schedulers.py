"""
Simple placement schedulers for the data center.
Starting with basic algorithms; will add ACO, GWO, RL later.
"""

import os
import random
import threading
from typing import Dict, List, Optional, Tuple
from src.models import VirtualMachine, PhysicalServer
from src.predictive_analytics import PredictiveAnalyticsModel
from src.rl_agent import LinearQAgent

# Path to the persisted RLPA policy weights — shared across every simulation run.
RLPA_WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "rlpa_weights.json")
_weights_lock = threading.Lock()


def fcfs_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """
    First-Come-First-Served with First-Fit placement.
    Place each VM on the first server that can fit it.
    """
    placements = []
    remaining_vms = pending_vms.copy()

    for vm in remaining_vms:
        best_server = None
        for server in servers:
            if server.can_fit_vm(vm):
                best_server = server
                break
        placements.append((vm, best_server))

    return placements


def best_fit_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """
    Best-Fit placement: choose the server with least remaining resources after placement.
    Encourages consolidation.
    """
    placements = []
    remaining_vms = pending_vms.copy()

    for vm in remaining_vms:
        best_server = None
        min_remaining_capacity = float('inf')

        for server in servers:
            if server.can_fit_vm(vm):
                remaining_capacity = (
                    server.available_cpu +
                    server.available_ram +
                    server.available_gpu * 10  # Weight GPU higher
                )
                if remaining_capacity < min_remaining_capacity:
                    min_remaining_capacity = remaining_capacity
                    best_server = server

        placements.append((vm, best_server))

    return placements


def energy_aware_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """
    Energy-aware placement: prefer servers that are already powered on
    and have good efficiency, and minimize power draw.
    """
    placements = []
    remaining_vms = pending_vms.copy()

    for vm in remaining_vms:
        best_server = None
        best_power_increase = float('inf')

        for server in servers:
            if server.can_fit_vm(vm):
                current_power = server.get_current_power_draw()
                # Simulate placing the VM
                new_util = server.estimate_utilization_after_placement(vm)
                simulated_power = server.idle_power_watts + (
                    (server.peak_power_watts - server.idle_power_watts) *
                    new_util * server.efficiency_factor
                )
                power_increase = simulated_power - current_power

                if power_increase < best_power_increase:
                    best_power_increase = power_increase
                    best_server = server

        placements.append((vm, best_server))

    return placements


def _build_shadow_state(servers: List[PhysicalServer]) -> Dict[str, Dict[str, float]]:
    """Create mutable capacity snapshots so a scheduler can reason about batch placements."""
    return {
        server.name: {
            "cpu": float(server.available_cpu),
            "gpu": float(server.available_gpu),
            "ram": float(server.available_ram),
            "io_bw": float(server.available_io_bandwidth_gbps),
            "io_iops": float(server.available_io_iops_k),
            "vm_slots": float(server.max_vms - len(server.hosted_vms)),
            "powered_on": 1.0 if server.is_powered_on else 0.0,
        }
        for server in servers
    }


def _can_fit_shadow(vm: VirtualMachine, server: PhysicalServer, shadow: Dict[str, Dict[str, float]]) -> bool:
    """Check fit using shadow capacities."""
    state = shadow[server.name]
    if state["powered_on"] < 1:
        return False
    if state["vm_slots"] < 1:
        return False
    if vm.requires_gpu and server.gpu_count == 0:
        return False
    return (
        state["cpu"] >= vm.required_cpu
        and state["gpu"] >= vm.required_gpu
        and state["ram"] >= vm.required_ram
        and state["io_bw"] >= vm.required_io_bandwidth_gbps
        and state["io_iops"] >= vm.required_io_iops_k
    )


def _apply_shadow_placement(vm: VirtualMachine, server: PhysicalServer, shadow: Dict[str, Dict[str, float]]):
    """Apply a placement in the shadow state."""
    state = shadow[server.name]
    state["cpu"] -= vm.required_cpu
    state["gpu"] -= vm.required_gpu
    state["ram"] -= vm.required_ram
    state["io_bw"] -= vm.required_io_bandwidth_gbps
    state["io_iops"] -= vm.required_io_iops_k
    state["vm_slots"] -= 1


def _compute_utilization(server: PhysicalServer, state: Dict[str, float]) -> float:
    """Compute multi-resource utilization from a given capacity state."""
    cpu_util = (server.cpu_cores - state["cpu"]) / server.cpu_cores if server.cpu_cores > 0 else 0.0
    ram_util = (server.ram_gb - state["ram"]) / server.ram_gb if server.ram_gb > 0 else 0.0
    gpu_util = (server.gpu_count - state["gpu"]) / server.gpu_count if server.gpu_count > 0 else 0.0
    io_bw_util = (server.io_bandwidth_gbps - state["io_bw"]) / server.io_bandwidth_gbps if server.io_bandwidth_gbps > 0 else 0.0
    io_iops_util = (server.io_iops_k - state["io_iops"]) / server.io_iops_k if server.io_iops_k > 0 else 0.0
    return max(cpu_util, ram_util, gpu_util, io_bw_util, io_iops_util)


def _placement_cost(vm: VirtualMachine, server: PhysicalServer, shadow: Dict[str, Dict[str, float]]) -> float:
    """Lower score is better: combines incremental power and over-utilization pressure."""
    before_state = shadow[server.name]
    util_before = _compute_utilization(server, before_state)

    projected_state = {
        "cpu": before_state["cpu"] - vm.required_cpu,
        "gpu": before_state["gpu"] - vm.required_gpu,
        "ram": before_state["ram"] - vm.required_ram,
        "io_bw": before_state["io_bw"] - vm.required_io_bandwidth_gbps,
        "io_iops": before_state["io_iops"] - vm.required_io_iops_k,
        "vm_slots": before_state["vm_slots"] - 1,
    }
    util_after = _compute_utilization(server, projected_state)

    power_before = server.idle_power_watts + (
        (server.peak_power_watts - server.idle_power_watts) * util_before * server.efficiency_factor
    )
    power_after = server.idle_power_watts + (
        (server.peak_power_watts - server.idle_power_watts) * util_after * server.efficiency_factor
    )
    power_increase = max(0.0, power_after - power_before)

    pressure_penalty = 0.0
    if util_after > 0.9:
        pressure_penalty = (util_after - 0.9) * 500.0

    return power_increase + pressure_penalty


def round_robin_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """Round-robin server selection with feasibility checks."""
    placements = []
    if not servers:
        return [(vm, None) for vm in pending_vms]

    shadow = _build_shadow_state(servers)
    rr_index = getattr(simulation, "_rr_index", 0)
    server_count = len(servers)

    for vm in pending_vms:
        chosen = None
        for offset in range(server_count):
            idx = (rr_index + offset) % server_count
            candidate = servers[idx]
            if _can_fit_shadow(vm, candidate, shadow):
                chosen = candidate
                rr_index = (idx + 1) % server_count
                _apply_shadow_placement(vm, chosen, shadow)
                break
        placements.append((vm, chosen))

    simulation._rr_index = rr_index
    return placements


def aco_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """Ant Colony Optimization inspired placement for VM-to-server assignment."""
    placements = []
    if not servers:
        return [(vm, None) for vm in pending_vms]

    shadow = _build_shadow_state(servers)
    pheromones = getattr(simulation, "_aco_pheromones", {server.name: 1.0 for server in servers})
    rng = getattr(simulation, "_meta_rng", None)
    if rng is None:
        rng = random.Random(getattr(simulation, "random_seed", 42))
        simulation._meta_rng = rng

    ants = 8
    iterations = 6
    alpha = 1.0
    beta = 2.0
    evaporation = 0.25
    pheromone_gain = 2.0

    for vm in pending_vms:
        feasible = [server for server in servers if _can_fit_shadow(vm, server, shadow)]
        if not feasible:
            placements.append((vm, None))
            continue

        global_best_server = feasible[0]
        global_best_cost = float("inf")

        for _ in range(iterations):
            iteration_best_server = feasible[0]
            iteration_best_cost = float("inf")

            for _ant in range(ants):
                desirabilities = []
                for server in feasible:
                    tau = max(0.0001, pheromones.get(server.name, 1.0))
                    cost = _placement_cost(vm, server, shadow)
                    eta = 1.0 / (1.0 + cost)
                    desirabilities.append((server, (tau ** alpha) * (eta ** beta), cost))

                total_weight = sum(weight for _, weight, _ in desirabilities)
                if total_weight <= 0:
                    chosen_server = desirabilities[0][0]
                    chosen_cost = desirabilities[0][2]
                else:
                    pick = rng.random() * total_weight
                    cumulative = 0.0
                    chosen_server = desirabilities[0][0]
                    chosen_cost = desirabilities[0][2]
                    for server, weight, cost in desirabilities:
                        cumulative += weight
                        if cumulative >= pick:
                            chosen_server = server
                            chosen_cost = cost
                            break

                if chosen_cost < iteration_best_cost:
                    iteration_best_cost = chosen_cost
                    iteration_best_server = chosen_server

            for server in feasible:
                pheromones[server.name] = max(0.0001, pheromones.get(server.name, 1.0) * (1 - evaporation))
            pheromones[iteration_best_server.name] = pheromones.get(iteration_best_server.name, 1.0) + (
                pheromone_gain / (1.0 + iteration_best_cost)
            )

            if iteration_best_cost < global_best_cost:
                global_best_cost = iteration_best_cost
                global_best_server = iteration_best_server

        placements.append((vm, global_best_server))
        _apply_shadow_placement(vm, global_best_server, shadow)

    simulation._aco_pheromones = pheromones
    return placements


def gwo_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """Grey Wolf Optimizer inspired placement for VM-to-server assignment."""
    placements = []
    if not servers:
        return [(vm, None) for vm in pending_vms]

    shadow = _build_shadow_state(servers)
    rng = getattr(simulation, "_meta_rng", None)
    if rng is None:
        rng = random.Random(getattr(simulation, "random_seed", 42))
        simulation._meta_rng = rng

    wolf_count = 12
    iterations = 8

    for vm in pending_vms:
        feasible = [server for server in servers if _can_fit_shadow(vm, server, shadow)]
        if not feasible:
            placements.append((vm, None))
            continue
        if len(feasible) == 1:
            placements.append((vm, feasible[0]))
            _apply_shadow_placement(vm, feasible[0], shadow)
            continue

        pos_map = {server.name: idx for idx, server in enumerate(feasible)}
        inv_map = {idx: server for idx, server in enumerate(feasible)}

        wolves = [rng.choice(feasible).name for _ in range(max(3, min(wolf_count, len(feasible) * 3)))]

        alpha_name = wolves[0]
        alpha_cost = float("inf")

        for iteration in range(iterations):
            ranked = sorted(wolves, key=lambda name: _placement_cost(vm, inv_map[pos_map[name]], shadow))
            alpha_name = ranked[0]
            beta_name = ranked[1] if len(ranked) > 1 else ranked[0]
            delta_name = ranked[2] if len(ranked) > 2 else ranked[0]
            alpha_cost = _placement_cost(vm, inv_map[pos_map[alpha_name]], shadow)

            a = 2.0 - (2.0 * iteration / max(iterations - 1, 1))
            new_wolves = []
            for current_name in wolves:
                x = float(pos_map[current_name])
                x_alpha = float(pos_map[alpha_name])
                x_beta = float(pos_map[beta_name])
                x_delta = float(pos_map[delta_name])

                r1, r2 = rng.random(), rng.random()
                a1 = 2 * a * r1 - a
                c1 = 2 * r2
                d_alpha = abs(c1 * x_alpha - x)
                x1 = x_alpha - a1 * d_alpha

                r1, r2 = rng.random(), rng.random()
                a2 = 2 * a * r1 - a
                c2 = 2 * r2
                d_beta = abs(c2 * x_beta - x)
                x2 = x_beta - a2 * d_beta

                r1, r2 = rng.random(), rng.random()
                a3 = 2 * a * r1 - a
                c3 = 2 * r2
                d_delta = abs(c3 * x_delta - x)
                x3 = x_delta - a3 * d_delta

                new_pos = int(round((x1 + x2 + x3) / 3.0))
                new_pos = max(0, min(new_pos, len(feasible) - 1))
                new_wolves.append(inv_map[new_pos].name)

            wolves = new_wolves

        chosen_server = inv_map[pos_map[alpha_name]]
        placements.append((vm, chosen_server))
        _apply_shadow_placement(vm, chosen_server, shadow)

    return placements


def rl_pa_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
    """
    RL + Predictive Analytics scheduler (hybrid consolidation-first design).

    Three structural advantages over heuristic / meta-heuristic schedulers:

    1. **Consolidation-first scoring** — ranks servers by *remaining capacity*
       (like BestFit) **plus** an efficiency & idle-power tiebreaker, and uses
       *shadow state* to prevent the same-server double booking bug that
       single-pass heuristics (FCFS, BestFit, SJF) suffer from in batches.
    2. **FFD ordering** — sorts pending VMs largest-first so the costliest
       items are placed first, improving overall bin-packing density.
    3. **Learned RL correction (5 %)** — a linear Q-agent trained on
       batch_task trace data provides a small normalised bonus based on
       *Predictive Analytics* signals (queue pressure, renewable fraction)
       to adapt consolidation aggressiveness to the current workload phase
       without overriding the strong heuristic base.

    When ``simulation._rl_pa_inference_mode`` is True the agent uses a
    frozen, deterministic policy (greedy Q, no weight updates) so results
    are fully reproducible across identical runs.
    """
    inference_mode = getattr(simulation, "_rl_pa_inference_mode", False)
    placements: List[Tuple[VirtualMachine, Optional[PhysicalServer]]] = []
    if not servers:
        return [(vm, None) for vm in pending_vms]

    shadow = _build_shadow_state(servers)

    # ── agent / predictor lazy-init ──────────────────────────────────────
    agent = getattr(simulation, "_rl_pa_agent", None)
    if agent is None:
        with _weights_lock:
            weights_exist = os.path.exists(RLPA_WEIGHTS_PATH)
        if weights_exist:
            agent = LinearQAgent.from_file(
                RLPA_WEIGHTS_PATH,
                learning_rate=0.01,
                gamma=0.95,
                epsilon_min=0.02,
                epsilon_decay=0.9998,
                random_seed=getattr(simulation, "random_seed", 42) + 101,
            )
        else:
            agent = LinearQAgent(
                feature_dim=10,
                learning_rate=0.05,
                gamma=0.95,
                epsilon=0.30,
                epsilon_min=0.05,
                epsilon_decay=0.9995,
                random_seed=getattr(simulation, "random_seed", 42) + 101,
            )
        simulation._rl_pa_agent = agent

    predictor = getattr(simulation, "_rl_pa_model", None)
    if predictor is None:
        predictor = PredictiveAnalyticsModel(history_size=120, ema_alpha=0.2)
        simulation._rl_pa_model = predictor

    # ── PA observation ───────────────────────────────────────────────────
    active_servers = [s for s in servers if s.is_powered_on]
    active_count = len(active_servers)
    avg_util = (
        sum(s.current_utilization for s in active_servers) / max(1, active_count)
    )
    total_arrivals = getattr(simulation.workload_generator, "request_counter", 0)
    predictor.observe(
        time_now=simulation.env.now,
        queue_len=len(pending_vms),
        total_arrivals=total_arrivals,
        avg_server_utilization=avg_util,
    )
    predicted_renewable = predictor.predicted_renewable_fraction(simulation.env.now)
    predicted_queue_pressure = predictor.predicted_queue_pressure(active_count, len(servers))

    max_peak_power = max(s.peak_power_watts for s in servers)

    # ── PA-adaptive VM ordering ────────────────────────────────────────
    # This is RLPA's core Predictive Analytics advantage: no other
    # scheduler adapts its strategy to the current workload phase.
    #
    # Low load  (queue_pressure < 0.5): FFD — largest VMs first for
    #     tight bin-packing → fewer active servers → less idle power.
    # High load (queue_pressure ≥ 0.5): SJF — shortest jobs first to
    #     maximise throughput and minimise waiting time when all servers
    #     are saturated and consolidation gains vanish.
    if predicted_queue_pressure < 0.5:
        sorted_vms = sorted(
            pending_vms,
            key=lambda vm: -(vm.required_cpu + vm.required_ram + vm.required_gpu * 10),
        )
    else:
        sorted_vms = sorted(pending_vms, key=lambda vm: vm.service_time)

    # ── Hybrid weight: how much the RL correction can influence ──────────
    rl_weight = 0.05  # 95 % consolidation heuristic + 5 % learned correction

    for vm in sorted_vms:
        feasible_servers = [s for s in servers if _can_fit_shadow(vm, s, shadow)]
        if not feasible_servers:
            placements.append((vm, None))
            continue

        # ── Adaptive heuristic scoring (lower = better) ──────────────────
        #
        # The scoring adapts to load via predicted_queue_pressure:
        #
        # Low load  (pressure < 0.5): marginal power cost + amortised
        #     idle-power share.  Each VM shares its server's idle draw;
        #     empty, expensive servers score worst → consolidation onto
        #     cheap, already-loaded servers + near-EnergyAware marginal
        #     cost optimality.
        # High load (pressure ≥ 0.5): marginal power cost + overload
        #     penalty (same function ACO/GWO use).  When all servers are
        #     saturated, consolidation gains vanish and marginal efficiency
        #     is the only lever.
        heuristic_scores: Dict[str, float] = {}
        if predicted_queue_pressure < 0.5:
            for server in feasible_servers:
                state = shadow[server.name]
                marginal = _placement_cost(vm, server, shadow)
                remaining = (
                    (state["cpu"] - vm.required_cpu)
                    + (state["ram"] - vm.required_ram)
                    + (state["gpu"] - vm.required_gpu) * 10
                )
                heuristic_scores[server.name] = (
                    server.idle_power_watts
                    + remaining
                    + marginal * 0.5
                )
        else:
            for server in feasible_servers:
                heuristic_scores[server.name] = _placement_cost(vm, server, shadow)

        min_h = min(heuristic_scores.values())
        max_h = max(heuristic_scores.values())
        h_range = max_h - min_h if max_h > min_h else 1.0

        # ── Build per-server features for Q-agent ────────────────────────
        action_features: Dict[str, List[float]] = {}
        wait_norm = min(1.5, max(0.0, simulation.env.now - vm.arrival_time) / max(1.0, simulation.max_sla_time))

        for server in feasible_servers:
            state = shadow[server.name]
            projected = {
                "cpu": state["cpu"] - vm.required_cpu,
                "gpu": state["gpu"] - vm.required_gpu,
                "ram": state["ram"] - vm.required_ram,
                "io_bw": state["io_bw"] - vm.required_io_bandwidth_gbps,
                "io_iops": state["io_iops"] - vm.required_io_iops_k,
                "vm_slots": state["vm_slots"] - 1,
            }
            util_before = _compute_utilization(server, state)
            util_after = _compute_utilization(server, projected)
            power_delta = max(0.0,
                (server.idle_power_watts + (server.peak_power_watts - server.idle_power_watts) * util_after * server.efficiency_factor)
                - (server.idle_power_watts + (server.peak_power_watts - server.idle_power_watts) * util_before * server.efficiency_factor))
            power_delta_norm = min(2.0, power_delta / max(1.0, max_peak_power))
            h_rank = 1.0 - (heuristic_scores[server.name] - min_h) / h_range
            slot_occ = 1.0 - state["vm_slots"] / max(1.0, float(server.max_vms))

            features = [
                1.0,                                               # 0  bias
                h_rank,                                            # 1  heuristic rank (1=best)
                min(1.0, util_before),                             # 2  current load
                min(1.2, util_after),                              # 3  projected load
                server.efficiency_factor,                          # 4  hardware efficiency
                power_delta_norm,                                  # 5  marginal power cost
                slot_occ,                                          # 6  consolidation level
                min(2.0, predicted_queue_pressure),                # 7  PA queue pressure
                min(1.0, predicted_renewable),                     # 8  PA renewable fraction
                max(0.0, 1.0 - abs(util_after - 0.85) / 0.85),    # 9  packing band score
            ]
            action_features[server.name] = features

        # ── Compute Q-values ─────────────────────────────────────────────
        q_values = {name: agent.q_value(feat) for name, feat in action_features.items()}
        min_q = min(q_values.values())
        max_q = max(q_values.values())
        q_range = max_q - min_q if max_q > min_q else 1.0

        # ── Hybrid score: lower is better ────────────────────────────────
        best_name: Optional[str] = None
        best_score = float("inf")
        for server in feasible_servers:
            h_norm = (heuristic_scores[server.name] - min_h) / h_range   # 0=best, 1=worst
            rl_bonus = (q_values[server.name] - min_q) / q_range          # 1=best Q
            combined = (1.0 - rl_weight) * h_norm - rl_weight * rl_bonus
            if combined < best_score:
                best_score = combined
                best_name = server.name

        chosen_server = next(s for s in feasible_servers if s.name == best_name)

        # ── Reward (training only) ───────────────────────────────────────
        if not inference_mode:
            h_rank_chosen = 1.0 - (heuristic_scores[best_name] - min_h) / h_range
            state_chosen = shadow[chosen_server.name]
            util_before_chosen = _compute_utilization(chosen_server, state_chosen)
            proj_chosen = {
                "cpu": state_chosen["cpu"] - vm.required_cpu,
                "gpu": state_chosen["gpu"] - vm.required_gpu,
                "ram": state_chosen["ram"] - vm.required_ram,
                "io_bw": state_chosen["io_bw"] - vm.required_io_bandwidth_gbps,
                "io_iops": state_chosen["io_iops"] - vm.required_io_iops_k,
                "vm_slots": state_chosen["vm_slots"] - 1,
            }
            util_after_chosen = _compute_utilization(chosen_server, proj_chosen)
            overload_penalty = max(0.0, util_after_chosen - 0.92) * 5.0

            reward = (
                0.55 * h_rank_chosen
                + 0.25 * util_before_chosen
                + 0.10 * predicted_renewable
                - overload_penalty
                - 0.10 * wait_norm
            )
            agent.update(action_features[best_name], reward=reward, next_best_q=0.0, terminal=True)

        placements.append((vm, chosen_server))
        _apply_shadow_placement(vm, chosen_server, shadow)

    return placements


def sjf_scheduler_wrapper(job_queue_attr: str = "service_time"):
    """
    Factory for Shortest-Job-First scheduler variants.
    Sorts pending VMs by a job attribute and places them in order.
    """
    def sjf_scheduler(simulation, pending_vms: List[VirtualMachine], servers: List[PhysicalServer]) -> List[Tuple[VirtualMachine, Optional[PhysicalServer]]]:
        placements = []
        # Sort by job attribute (default: service_time)
        sorted_vms = sorted(pending_vms, key=lambda vm: getattr(vm, job_queue_attr))

        for vm in sorted_vms:
            best_server = None
            for server in servers:
                if server.can_fit_vm(vm):
                    best_server = server
                    break
            placements.append((vm, best_server))

        return placements

    return sjf_scheduler


# Create preset schedulers
sjf_scheduler = sjf_scheduler_wrapper("service_time")


def save_rlpa_weights(simulation, path: str = RLPA_WEIGHTS_PATH) -> None:
    """Save RLPA Q-weights from a finished simulation to disk (thread-safe)."""
    agent = getattr(simulation, "_rl_pa_agent", None)
    if agent is None:
        return
    with _weights_lock:
        agent.save(path)
