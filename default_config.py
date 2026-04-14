"""
Default configuration for the Green Cloud Data Center simulator.
All parameters can be overridden at runtime.
"""

# Simulation parameters
SIMULATION_TIME = 10000  # Total simulation time (time units)
RANDOM_SEED = 42

# Data center configuration
PHYSICAL_SERVERS = [
    # Server type 1: CPU-optimized, energy-efficient
    {
        "name": "CPU_Server_1",
        "cpu_cores": 32,
        "gpu_count": 0,
        "ram_gb": 128,
        "io_bandwidth_gbps": 12,
        "io_iops_k": 140,
        "max_vms": 16,
        "idle_power_watts": 150,
        "peak_power_watts": 600,
        "efficiency_factor": 0.85,  # Better efficiency at partial load
    },
    # Server type 2: GPU-heavy, power-hungry
    {
        "name": "GPU_Server_1",
        "cpu_cores": 64,
        "gpu_count": 4,
        "ram_gb": 256,
        "io_bandwidth_gbps": 16,
        "io_iops_k": 180,
        "max_vms": 8,
        "idle_power_watts": 400,
        "peak_power_watts": 2000,
        "efficiency_factor": 0.70,
    },
    # Server type 3: Balanced
    {
        "name": "Balanced_Server_1",
        "cpu_cores": 48,
        "gpu_count": 2,
        "ram_gb": 192,
        "io_bandwidth_gbps": 14,
        "io_iops_k": 160,
        "max_vms": 12,
        "idle_power_watts": 250,
        "peak_power_watts": 1200,
        "efficiency_factor": 0.78,
    },
    # Duplicate a few more for larger scales
    {
        "name": "CPU_Server_2",
        "cpu_cores": 32,
        "gpu_count": 0,
        "ram_gb": 128,
        "io_bandwidth_gbps": 12,
        "io_iops_k": 140,
        "max_vms": 16,
        "idle_power_watts": 150,
        "peak_power_watts": 600,
        "efficiency_factor": 0.85,
    },
    {
        "name": "GPU_Server_2",
        "cpu_cores": 64,
        "gpu_count": 4,
        "ram_gb": 256,
        "io_bandwidth_gbps": 16,
        "io_iops_k": 180,
        "max_vms": 8,
        "idle_power_watts": 400,
        "peak_power_watts": 2000,
        "efficiency_factor": 0.70,
    },
]

# Workload configuration
WORKLOAD_ARRIVAL_RATE = 5  # Requests per 100 time units (Poisson)

# VM/Request profiles
VM_PROFILES = {
    "cpu_light": {"cpu": 2, "gpu": 0, "ram": 4, "requires_gpu": False, "io_bandwidth_gbps": 0.5, "io_iops_k": 5},
    "cpu_medium": {"cpu": 4, "gpu": 0, "ram": 8, "requires_gpu": False, "io_bandwidth_gbps": 0.8, "io_iops_k": 8},
    "cpu_heavy": {"cpu": 8, "gpu": 0, "ram": 16, "requires_gpu": False, "io_bandwidth_gbps": 1.2, "io_iops_k": 12},
    "gpu_light": {"cpu": 4, "gpu": 1, "ram": 8, "requires_gpu": True, "io_bandwidth_gbps": 1.0, "io_iops_k": 10},
    "gpu_heavy": {"cpu": 8, "gpu": 2, "ram": 32, "requires_gpu": True, "io_bandwidth_gbps": 1.8, "io_iops_k": 20},
    "io_light": {"cpu": 2, "gpu": 0, "ram": 6, "requires_gpu": False, "io_bandwidth_gbps": 2.5, "io_iops_k": 35},
    "io_heavy": {"cpu": 4, "gpu": 0, "ram": 12, "requires_gpu": False, "io_bandwidth_gbps": 4.5, "io_iops_k": 65},
}

# Service time distribution (in time units)
SERVICE_TIME_MEAN = 100
SERVICE_TIME_STD = 30

# Renewable energy simulation
RENEWABLE_AVAILABILITY_PERCENTAGE = 40  # % of time renewable is available
RENEWABLE_POWER_FRACTION = 0.5  # At most 50% of total can be renewable

# SLA/QoS targets
MAX_WAIT_TIME = 50  # time units before SLA violation
MAX_DELAY_ACCEPTABLE = 20  # time units

# Metrics and logging
LOG_EVENTS = True
LOG_INTERVAL = 100  # Log metrics every N time units
DETAILED_LOGGING = False  # Set to True for per-VM tracking

# VM migration settings
ENABLE_VM_MIGRATION = True
MIGRATION_CHECK_INTERVAL = 25
MIGRATION_SOURCE_MAX_UTIL = 0.35
MIGRATION_DEST_MAX_UTIL = 0.85
MAX_MIGRATIONS_PER_VM = 2
VM_MIGRATION_COOLDOWN = 120
SERVER_MIGRATION_COOLDOWN = 80
MIGRATION_BASE_DOWNTIME = 0.5
MIGRATION_BANDWIDTH_GB_PER_TIME = 8.0
MIGRATION_ENERGY_OVERHEAD_KWH = 0.002
MIGRATION_ENERGY_GAIN_HORIZON = 180
MIGRATION_MIN_NET_ENERGY_GAIN_KWH = 0.01

# Server power state settings
ENABLE_SERVER_POWER_STATES = True
SERVER_IDLE_SHUTDOWN_TIME = 120
SERVER_POWER_STATE_CHECK_INTERVAL = 10
SERVER_WAKEUP_DELAY = 8
SERVER_WAKEUP_ENERGY_KWH = 0.01
