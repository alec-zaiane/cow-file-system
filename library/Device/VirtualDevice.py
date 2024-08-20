from typing import Literal, Optional
from logging import Logger

from .Device import Device

from .DeviceState import *


# supported aggregation types
# stripe: all devices are striped together, maximum space, no redundancy, requires all devices to have the same block size
# mirror: all devices are mirrored, minimum space, max redundancy, requires all devices to be of the same size        
type_aggregation_options = Literal["stripe", "mirror"]

class VirtualDeviceFactory:
    @staticmethod
    def create_virtual_device(devices: list[Device], aggregation_type: type_aggregation_options, logger:Optional[Logger]) -> Device:
        """Create a virtual device from a list of physical devices.

        Args:
            devices (list[PhysicalDevice]): the physical devices to aggregate
            aggregation_type (type_aggregation_options): the aggregation type to use

        Returns:
            Device: a virtual device that aggregates the physical devices
        """
        if aggregation_type == "stripe":
            return VirtualDeviceStripe(devices, logger)
        elif aggregation_type == "mirror":
            return VirtualDeviceMirror(devices, logger)
        else:
            raise ValueError(f"Unknown aggregation type {aggregation_type}.")

class VirtualDevice(Device):
    def __init__(self, logger:Optional[Logger]):
        self.logger:Logger = logger if logger is not None else Logger("VirtualDevice")
        self._state:DeviceState = VirtualDeviceOnline()
        self._write_intents:list[tuple[int, bytes]] = [] # writes that have not been committed yet, only populated if a device is unreachable 
        self._devices:list[Device] = []
        self._skip_write_intents:bool = False # gets set to true during the attempt_bring_online process to prevent multiple write intents
        
    def get_state(self) -> DeviceState:
        return self._state
    
    def attempt_bring_online(self) -> bool:
        for d in self._devices:
            d.attempt_bring_online()
        self.self_check_state()
        if len(self._write_intents) > 0:
            self._skip_write_intents = True
            self.logger.info(f"Attempting to bring virtual device online, {len(self._write_intents)} write intents to commit.")
            for block_number, data in self._write_intents:
                success = self.write_block(block_number, data)
                if not success:
                    self.logger.error(f"Failed to commit write intent for block {block_number}.")
                    break
                else:
                    self.logger.info(f"Successfully committed write intent for block {block_number}.")
                    self._write_intents.pop(0)
            self._skip_write_intents = False
        return isinstance(self._state, VirtualDeviceOnline)
                
        
    def _attempt_state_update(self, desired_state:DeviceState):
        if self._state == desired_state:
            return
        success = self._state.transition_to(desired_state)
        if success:
            self.logger.info(f"Virtual device state successfully transitioned to {desired_state}.")
        else:
            self.logger.error(f"Failed to transition virtual device state to {desired_state}, current state: {self._state}.")
                
    def self_check_state(self):
        """Self-check the state of the virtual device, may update the state depending on the state of the contained devices."""
        if all(isinstance(d.get_state(), DeviceOnlineMixin) for d in self._devices) and not any(isinstance(d.get_state(), DeviceFaultedMixin) for d in self._devices) and len(self._write_intents) == 0:
            self._attempt_state_update(VirtualDeviceOnline())
        elif all(isinstance(d.get_state(), DeviceOfflineMixin) for d in self._devices) and not any(isinstance(d.get_state(), DeviceFaultedMixin) for d in self._devices):
            self._attempt_state_update(VirtualDeviceOffline())
        elif any(isinstance(d.get_state(), DeviceFaultedMixin) for d in self._devices) or len(self._write_intents) > 0:
            if all(isinstance(d.get_state(), DeviceOfflineMixin) for d in self._devices):
                self._attempt_state_update(VirtualDeviceFaultedOffline())
            else:
                self._attempt_state_update(VirtualDeviceFaulted())
        else:
            self._state = VirtualDeviceDegraded()
            
        
    
class VirtualDeviceStripe(VirtualDevice):
    def __init__(self, devices: list[Device], logger:Optional[Logger]):
        """Create a virtual device that stripes multiple physical devices together.
        All devices must be the same block size.

        Args:
            devices (list[PhysicalDevice]): the physical devices to stripe together
            
        Raises:
            ValueError: if the devices have different block sizes
        """
        # validation:
        if len(set(d.get_block_size() for d in devices)) != 1:
            raise ValueError("All devices must have the same block size.")
        
        super().__init__(logger)
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = sum(d.get_size() for d in devices)
        
        # block_number_lookup to accelerate block number -> device/block_number conversion
        self._block_number_lookup:list[int] = []
        rolling_sum = 0
        for d in devices:
            self._block_number_lookup.append(rolling_sum)
            rolling_sum += d.get_size() // self._block_size
            
    def _find_device_and_local_block_number(self, block_number:int) -> tuple[Device, int]:
        """Find the device and local block number for a given block number.

        Args:
            block_number (int): global block number

        Returns:
            tuple[PhysicalDevice, int]: the device and local block number
            
        Raises:
            ValueError: if the block number is out of range
        """        
        if block_number < 0 or block_number >= self._size // self._block_size:
            raise ValueError(f"Block number {block_number} out of range.")
        
        for i, b_number in enumerate(self._block_number_lookup):
            if block_number < b_number:
                return self._devices[i-1], block_number - self._block_number_lookup[i-1]
        return self._devices[-1], block_number - self._block_number_lookup[-1]

    
    def write_block(self, block_number:int, data:bytes) -> bool:
        """Write a block to the virtual device.

        Args:
            block_number (int): the block number to write
            data (bytes): the data to write
        Raises:
            ValueError: if the data length is not the same as the block size
        Returns: 
            bool: True if the write was successful, False otherwise
        """
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        self.self_check_state()
        device, local_block_number = self._find_device_and_local_block_number(block_number)
        if isinstance(device.get_state(), DeviceOfflineMixin):
            success = device.attempt_bring_online()
            self.logger.info(f"Attempted to bring device {device} online, success: {success}.")
        if isinstance(device.get_state(), DeviceOnlineMixin):
            return device.write_block(local_block_number, data)
        else:
            if not self._skip_write_intents:
                self._write_intents.append((block_number, data))
            self.logger.error(f"Failed to write block {block_number}, device {device} is not online.")
            self._attempt_state_update(VirtualDeviceFaulted()) # because we can't write to the device, data is lost until we actually can write it
            return False
    def read_block(self, block_number:int) -> bytes:
        """Read a block from the virtual device.

        Args:
            block_number (int): the block number to read

        Returns:
            bytes: the block data, will be the length of the block size
        """
        device, local_block_number = self._find_device_and_local_block_number(block_number)
        return device.read_block(local_block_number)
    
    def get_block_size(self) -> int:
        return self._block_size
    
    def get_size(self) -> int:
        return self._size
    
class VirtualDeviceMirror(VirtualDevice):
    def __init__(self, devices:list[Device], logger:Optional[Logger]):
        """Create a virtual device that mirrors multiple physical devices together.
        All devices must be the same size and block size.
        
        Args:
            devices (list[Device]): the physical devices to mirror
        Raises:
            ValueError: if the devices have different sizes or block sizes
        """
        # validation:
        if len(set(d.get_size() for d in devices)) != 1:
            raise ValueError("All devices must have the same size.")
        if len(set(d.get_block_size() for d in devices)) != 1:
            raise ValueError("All devices must have the same block size.")
        
        super().__init__(logger)
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = devices[0].get_size()
        
    def get_block_size(self) -> int:
        return self._block_size
    
    def get_size(self) -> int:
        return self._size
    
    def write_block(self, block_number:int, data:bytes) -> bool:
        """Write a block to the virtual device.

        Args:
            block_number (int): the block number to write
            data (bytes): the data to write
        Raises:
            ValueError: if the data length is not the same as the block size
        """
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        
        successes = [d.write_block(block_number, data) for d in self._devices]
        if not all(successes):
            self.logger.error(f"Failed to write block {block_number} to all devices, {sum(successes)} out of {len(self._devices)} succeeded (failing: {[d for i,d in enumerate(self._devices) if not successes[i]]}), vdev is faulted.")
            self._attempt_state_update(VirtualDeviceFaulted())
            self._write_intents.append((block_number, data))
            return False
        return True
            
    def read_block(self, block_number:int) -> bytes:
        """Read a block from the virtual device.

        Args:
            block_number (int): the block number to read

        Returns:
            bytes: the block data, will be the length of the block size
        """
        return self._devices[0].read_block(block_number)
    
    def check_integrity(self) -> bool:
        """Check the integrity of the virtual device.

        Returns:
            bool: True if all devices have the same data, False otherwise
        """
        for block in range(self._size // self._block_size):
            data_0 = self._devices[0].read_block(block)
            for d in self._devices[1:]:
                if d.read_block(block) != data_0:
                    return False
        return True