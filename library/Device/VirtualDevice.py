from typing import Literal, Optional
from logging import Logger

from .Device import Device

from .DeviceState import *


# supported aggregation types
# stripe: all devices are striped together, maximum space, no redundancy, requires all devices to have the same block size
# mirror: all devices are mirrored, minimum space, max redundancy, requires all devices to be of the same size        
type_aggregation_options = Literal["stripe", "mirror"]


class VirtualDevice(Device):
    def __init__(self, name:str, logger:Optional[Logger]):
        super().__init__(name)
        
        self.logger:Logger = logger if logger is not None else Logger("VirtualDevice")
        self._state:DeviceState = VirtualDeviceOffline()
        self._write_intents:list[tuple[int, bytes]] = [] # writes that have not been committed yet, only populated if a device is unreachable 
        self._devices:list[Device] = []
        self._skip_write_intents:bool = False # gets set to true during the attempt_bring_online process to prevent multiple write intents
        
        
    def get_state(self) -> DeviceState:
        return self._state
    
    def attempt_bring_online(self) -> bool:
        self.logger.debug(f"{self.name} - Attempting to bring self virtual device online.")
        for d in self._devices:
            d.attempt_bring_online()
        self.self_check_state()
        # do any pending writes
        if len(self._write_intents) > 0:
            self._skip_write_intents = True
            self.logger.info(f"{self.name} - while attempting to bring virtual device online, found {len(self._write_intents)} write intents to commit.")
            for block_number, data in self._write_intents:
                success = self.write_block(block_number, data)
                if not success:
                    self.logger.error(f"{self.name} - Failed to commit write intent for block {block_number}.")
                    break
                else:
                    self.logger.info(f"{self.name} - Successfully committed write intent for block {block_number}.")
                    self._write_intents.pop(0)
            self._skip_write_intents = False
        self.self_check_state()
        return isinstance(self._state, VirtualDeviceOnline)
    
    def __str__(self):
        return f"VirtualDevice {self.name} with state {self._state} and devices {[str(d) for d in self._devices]}"
                
        
    def _attempt_state_update(self, desired_state:DeviceState) -> bool:
        """Update the state of the virtual device if possible.

        Args:
            desired_state (DeviceState): desired state to transition to

        Returns:
            bool: True if the state was successfully transitioned, False otherwise
        """        
        if self._state == desired_state:
            self.logger.debug(f"{self.name} - Virtual device state already in desired state.")
            return True
        oldstate = self._state
        self._state = self._state.transition_to(desired_state)
        if desired_state == self._state:
            self.logger.debug(f"{self.name} - Virtual device state successfully transitioned {oldstate} -> {desired_state}.")
            return True
        else:
            self.logger.error(f"{self.name} - Virtual device state FAILED TRANSITION {oldstate} -x> {desired_state}.")
            return False
                
    def self_check_state(self):
        """Self-check the state of the virtual device, may update the state depending on the state of the contained devices."""
        self.logger.debug(f"{self.name} - Self-checking virtual device state.")
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
            
    def mark_faulted(self) -> bool:
        if isinstance(self._state, DeviceOnlineMixin):
            return self._attempt_state_update(VirtualDeviceFaulted())
        elif isinstance(self._state, DeviceOfflineMixin):
            return self._attempt_state_update(VirtualDeviceFaultedOffline())
        return False
            
        
            
        
class VirtualDeviceFactory:
    @staticmethod
    def create_virtual_device(name:str, devices: list[Device], aggregation_type: type_aggregation_options, logger:Optional[Logger]) -> VirtualDevice:
        """Create a virtual device from a list of physical devices.

        Args:
            devices (list[PhysicalDevice]): the physical devices to aggregate
            aggregation_type (type_aggregation_options): the aggregation type to use

        Returns:
            Device: a virtual device that aggregates the physical devices
        """
        if aggregation_type == "stripe":
            return VirtualDeviceStripe(name, devices, logger)
        elif aggregation_type == "mirror":
            return VirtualDeviceMirror(name, devices, logger)
        else:
            raise ValueError(f"Unknown aggregation type {aggregation_type}.")
    
class VirtualDeviceStripe(VirtualDevice):
    def __init__(self, name:str, devices: list[Device], logger:Optional[Logger]):
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
        
        super().__init__(name, logger)
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = sum(d.get_size() for d in devices)
        
        self.self_check_state()
        
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
            self.logger.info(f"{self.name} - Attempted to bring device {device} online, success: {success}.")
        if isinstance(device.get_state(), DeviceOnlineMixin):
            return device.write_block(local_block_number, data)
        else:
            if not self._skip_write_intents:
                self._write_intents.append((block_number, data))
            self.logger.error(f"{self.name} - Failed to write block {block_number}, device {device} is not online.")
            self._attempt_state_update(VirtualDeviceFaulted()) # because we can't write to the device, data is lost until we actually can write it
            return False
    def read_block(self, block_number:int) -> bytes:
        """Read a block from the virtual device.

        Args:
            block_number (int): the block number to read
            
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the device is offline and cannot be brought online

        Returns:
            bytes: the block data, will be the length of the block size
        """
        device, local_block_number = self._find_device_and_local_block_number(block_number)
        if isinstance(device.get_state(), DeviceOfflineMixin):
            success = device.attempt_bring_online()
            self.logger.info(f"{self.name} - Attempted to bring device {device} online, success: {success}.")
        if isinstance(device.get_state(), DeviceOnlineMixin):
            return device.read_block(local_block_number)
        raise ValueError(f"Device {device} is not online.")

    def get_block_size(self) -> int:
        return self._block_size
    
    def get_size(self) -> int:
        return self._size
    
class VirtualDeviceMirror(VirtualDevice):
    def __init__(self, name:str, devices:list[Device], logger:Optional[Logger]):
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
        
        super().__init__(name, logger)
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = devices[0].get_size()
        self.self_check_state()
        
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
        Returns:
            bool: True if the write was successful, False otherwise
        """
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        for d in self._devices:
            if not isinstance(d.get_state(), DeviceOnlineMixin):
                success = d.attempt_bring_online()
                self.logger.info(f"{self.name} - Attempted to bring device {d} online, success: {success}.")
        self.self_check_state()
        
        # write to as many devices as possible
        successes:list[bool] = []
        for d in self._devices:
            if isinstance(d.get_state(), DeviceOnlineMixin):
                successes.append(d.write_block(block_number, data))
            else:
                successes.append(False)
        if not all(successes):
            self.logger.error(f"{self.name} - Failed to write block {block_number} to all devices, {sum(successes)} out of {len(self._devices)} succeeded (failing: {[d for i,d in enumerate(self._devices) if not successes[i]]}), vdev is faulted.")
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
        if not self.check_integrity(block_number):
            # get what data we can and return it, mark any device returning different data as faulted
            self.logger.error(f"{self.name} - Data corruption detected in block {block_number}.")
            self._attempt_state_update(VirtualDeviceFaulted())
            all_data = [d.read_block(block_number) for d in self._devices]
            freqs:dict[bytes, int] = {}
            for d in all_data:
                freqs[d] = freqs.get(d, 0) + 1
                
            highest_freq = max(freqs.values())
            most_common_data = [d for d in freqs if freqs[d] == highest_freq]
            if len(most_common_data) == 1:
                final_data = most_common_data[0]
            else:
                self.logger.error(f"{self.name} - Data corruption detected in block {block_number}, no common data found.")
                final_data = None
            # mark all devices with different data as faulted
            for i, d in enumerate(all_data):
                if d != final_data:
                    self.logger.error(f"{self.name} - Device {self._devices[i]} returned different data than majority, marking as faulted.")
                    success = self._devices[i].mark_faulted()
                    if not success:
                        self.logger.error(f"{self.name} - Failed to mark device {self._devices[i]} as faulted.")
                        raise ValueError(f"Failed to mark device {self._devices[i]} as faulted.")
            if final_data is not None:
                return final_data
            raise ValueError("Data corruption detected and no majority data found.")
        return self._devices[0].read_block(block_number)
    
    def check_integrity(self, block_number:int, repair:bool=False) -> bool:
        """Check the integrity of the virtual device for a given block.
        If there are issues, marks self as faulted
        
        Args:
            block_number (int): the block number to check
            repair (bool): if True, attempt to repair the block if it is corrupted

        Returns:
            bool: True if all devices have the same data (no issue, or repair was successful), False otherwise
        """
        self.logger.info(f"{self.name} - Checking integrity of block {block_number}.")
        data_dict:dict[bytes,int] = {}
        for d in self._devices:
            data = d.read_block(block_number)
            data_dict[data] = data_dict.get(data, 0) + 1
        if len(data_dict) == 1:
            return True
        self.mark_faulted()


        self.logger.error(f"{self.name} - Data corruption detected in block {block_number}{', attempting repair.' if repair else '.'}")
        highest = max(data_dict.values())
        most_common_data = [d for d in data_dict if data_dict[d] == highest]
        if len(most_common_data) == 1:
            repair_successful = True
            for d in self._devices:
                if d.read_block(block_number) != most_common_data[0]:
                    d.mark_faulted()
                    if repair:
                        d.write_block(block_number, most_common_data[0])
                if repair and d.read_block(block_number) != most_common_data[0]:
                    # if we can't re-write the correct data, repair is impossible
                    repair_successful = False
            if repair and repair_successful:
                self.logger.info(f"{self.name} - Repair successful for block {block_number}.")
                return True
            elif repair:
                self.logger.error(f"{self.name} - Repair failed for block {block_number}.")
            return False
        # else, no majority data, can't repair
        self.logger.critical(f"{self.name} - Data corruption detected in block {block_number}, no majority data found, repair{" would be" if not repair else ""} impossible.")
        return False
    
    def check_all_integrity(self, repair:bool=False) -> bool:
        """Check the integrity of the entire virtual device.

        Args:
            repair (bool, optional): attempt to repair the block if it is corrupted. Defaults to False.

        Returns:
            bool: True if all blocks had no errors, False otherwise
        """        
        for block in range(self._size // self._block_size):
            if not self.check_integrity(block, repair):
                return False
        return True
    