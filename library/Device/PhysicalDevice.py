from .Device import Device
from .DeviceState import *

class PhysicalDevice(Device):
    """A physical device represents a physical disk that holds data with no redundancy."""
    def __init__(self, name:str, size:int, block_size:int):
        """Create a physical disk with a given size and block size.

        Args:
            size (int): disk size in bytes
            block_size (int): block size in bytes
        """
        # validate
        assert size % block_size == 0, "Disk size must be a multiple of block size."
        assert size > 0, "Disk size must be positive."
        assert block_size > 0, "Block size must be positive."
        assert size >= block_size, "Disk size must be at least the block size."
        
        super().__init__(name)
                
        self._state:DeviceState = PhysicalDeviceOffline()
        self._size = size
        self._block_size = block_size
        self._data = bytearray(size)
        
    def transition_state(self, desired_state:PhysicalDeviceState) -> bool:
        """Attempts to transition the device to a new state, returns True if the state post-transition is the desired state.

        Args:
            desired_state (DeviceState): the desired state to transition to

        Returns:
            bool: True if the state post-transition is the desired state, False otherwise
        """        
        self._state = self._state.transition_to(desired_state)
        return self._state == desired_state
        
    def read_block(self, block_number:int) -> bytes:
        """Read a block from the disk.

        Args:
            block_number (int): the block number to read

        Returns:
            bytes: the block data, will be the length of the block size
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the device is offline
        """
        if block_number < 0 or block_number >= self._size // self._block_size:
            raise ValueError(f"Block number {block_number} out of range.")
        if self._state == PhysicalDeviceOffline():
            raise ValueError("Device is offline.")
        return bytes(self._data[block_number * self._block_size:(block_number + 1) * self._block_size])
    
    def write_block(self, block_number:int, data:bytes) -> bool:
        """Write a block to the disk.

        Args:
            block_number (int): the block number to write
            data (bytes): the data to write
        Raises:
            ValueError: if the data length is not the same as the block size
            ValueError: if the block number is out of range
            ValueError: if the device is offline
        Returns:
            bool: True if the write was successful, False otherwise
        """
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        if block_number < 0 or block_number >= self._size // self._block_size:
            raise ValueError(f"Block number {block_number} out of range.")
        if self._state == PhysicalDeviceOffline():
            raise ValueError("Device is offline.")
        self._data[block_number * self._block_size:(block_number + 1) * self._block_size] = data
        return True
        
    def get_block_size(self) -> int:
        return self._block_size
    
    def get_size(self) -> int:
        return self._size
    
    def get_state(self) -> DeviceState:
        return self._state
    
    def attempt_bring_online(self) -> bool:
        """Attempt to bring the device to the fully online state.

        Returns:
            bool: True if the device is now online, False otherwise
        """
        return self.transition_state(PhysicalDeviceOnline())
    
    def mark_faulted(self) -> bool:
        if isinstance(self._state, DeviceOnlineMixin):
            return self.transition_state(PhysicalDeviceFaulted())
        else:
            return self.transition_state(PhysicalDeviceFaultedOffline())