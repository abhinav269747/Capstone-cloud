"""
Unit tests for core simulation components.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from src.models import PhysicalServer, VirtualMachine, VMState, WorkloadRequest
from src.workload_generator import WorkloadGenerator
from config.default_config import VM_PROFILES, SERVICE_TIME_MEAN, SERVICE_TIME_STD


class TestPhysicalServer(unittest.TestCase):
    """Test PhysicalServer model."""

    def setUp(self):
        self.server = PhysicalServer(
            name="TestServer",
            cpu_cores=32,
            gpu_count=2,
            ram_gb=128,
            max_vms=10,
            idle_power_watts=100,
            peak_power_watts=500,
            efficiency_factor=0.8,
        )

    def test_initialization(self):
        """Test server initializes with correct capacity."""
        self.assertEqual(self.server.available_cpu, 32)
        self.assertEqual(self.server.available_gpu, 2)
        self.assertEqual(self.server.available_ram, 128)

    def test_vm_placement(self):
        """Test placing a VM on the server."""
        vm = VirtualMachine(
            vm_id="vm1",
            required_cpu=8,
            required_gpu=1,
            required_ram=16,
            requires_gpu=False,
            service_time=100,
            arrival_time=0,
        )
        success = self.server.place_vm(vm)
        self.assertTrue(success)
        self.assertEqual(self.server.available_cpu, 24)
        self.assertEqual(self.server.available_gpu, 1)
        self.assertEqual(len(self.server.hosted_vms), 1)

    def test_cannot_fit_large_vm(self):
        """Test that VMs too large cannot be placed."""
        vm = VirtualMachine(
            vm_id="vm_big",
            required_cpu=64,  # More than server has
            required_gpu=0,
            required_ram=32,
            requires_gpu=False,
            service_time=100,
            arrival_time=0,
        )
        success = self.server.place_vm(vm)
        self.assertFalse(success)

    def test_power_draw(self):
        """Test power calculation at different utilization levels."""
        # Idle
        power_idle = self.server.get_current_power_draw()
        self.assertEqual(power_idle, 100)

        # 50% utilized
        vm = VirtualMachine(
            vm_id="vm_half",
            required_cpu=16,
            required_gpu=0,
            required_ram=64,
            requires_gpu=False,
            service_time=100,
            arrival_time=0,
        )
        self.server.place_vm(vm)
        power_half = self.server.get_current_power_draw()
        self.assertGreater(power_half, power_idle)
        self.assertLess(power_half, 500)

    def test_io_capacity_constraint(self):
        """Test that I/O-heavy VMs are rejected when server I/O capacity is insufficient."""
        self.server.available_io_bandwidth_gbps = 1.0
        self.server.available_io_iops_k = 10.0

        vm = VirtualMachine(
            vm_id="vm_io_heavy",
            required_cpu=2,
            required_gpu=0,
            required_ram=4,
            requires_gpu=False,
            service_time=80,
            arrival_time=0,
            required_io_bandwidth_gbps=2.0,
            required_io_iops_k=20.0,
        )

        success = self.server.place_vm(vm)
        self.assertFalse(success)


class TestVirtualMachine(unittest.TestCase):
    """Test VirtualMachine model."""

    def setUp(self):
        self.vm = VirtualMachine(
            vm_id="vm1",
            required_cpu=4,
            required_gpu=0,
            required_ram=8,
            requires_gpu=False,
            service_time=100,
            arrival_time=10,
            sla_deadline=110,
        )

    def test_initialization(self):
        """Test VM initializes with correct state."""
        self.assertEqual(self.vm.state, VMState.PENDING)
        self.assertIsNone(self.vm.start_time)
        self.assertIsNone(self.vm.completion_time)

    def test_lifecycle(self):
        """Test VM state transitions."""
        self.vm.mark_started(15)
        self.assertEqual(self.vm.state, VMState.RUNNING)
        self.assertEqual(self.vm.start_time, 15)
        self.assertEqual(self.vm.waiting_time, 5)

        self.vm.mark_completed(115)
        self.assertEqual(self.vm.state, VMState.COMPLETED)
        self.assertTrue(self.vm.sla_violated)  # Deadline was 110
        self.assertEqual(self.vm.get_turnaround_time(), 105)


class TestWorkloadGenerator(unittest.TestCase):
    """Test WorkloadGenerator."""

    def setUp(self):
        self.gen = WorkloadGenerator(
            arrival_rate=5,
            vm_profiles=VM_PROFILES,
            service_time_mean=SERVICE_TIME_MEAN,
            service_time_std=SERVICE_TIME_STD,
            random_seed=42,
        )

    def test_arrivals_generation(self):
        """Test that arrivals are generated."""
        arrivals = list(self.gen.generate_arrivals(simulation_time=1000, pattern="poisson"))
        self.assertGreater(len(arrivals), 0)
        # Arrivals should be in ascending order
        for i in range(len(arrivals) - 1):
            self.assertLess(arrivals[i][0], arrivals[i + 1][0])

    def test_vm_creation_from_request(self):
        """Test creating VMs from a request."""
        request = WorkloadRequest(
            request_id="req1",
            arrival_time=0,
            vm_profile="cpu_light",
            vm_count=2,
        )
        vms = self.gen.create_vms_from_request(request, max_sla_time=50)
        self.assertEqual(len(vms), 2)
        self.assertEqual(vms[0].arrival_time, 0)
        self.assertGreater(vms[0].service_time, 0)


if __name__ == "__main__":
    unittest.main()
