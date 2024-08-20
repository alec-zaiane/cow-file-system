# A device stores data at the block level, can be read from/written to, dumb storage
# a physical device is like a hard drive
# a virtual device can join multiple physical or virtual devices together

from abc import ABC, abstractmethod
from .DeviceState import DeviceState

class Device(ABC):
    def __init__(self, name:str):
        self.name = name

    def __str__(self):
        return self.name
    
    @abstractmethod
    def read_block(self, block_number:int) -> bytes:
        """Read a block from the device

        Args:
            block_number (int): the block number to read

        Returns:
            bytes: the block data, will be the length of the block size
        Raises:
            ValueError: if the block number is out of range
        """        
        ...
    @abstractmethod
    def write_block(self, block_number:int, data:bytes) -> bool:
        """Write a block to the device

        Args:
            block_number (int): the block number to write
            data (bytes): the data to write, must be the same length as the block size
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the data length is not the same as the block size
        Returns:
            bool: True if the write was successful, False otherwise
        """        
        ...
    @abstractmethod
    def get_block_size(self) -> int:
        """Get the block size of the device.

        Returns:
            int: the block size in bytes
        """
        ...
        
    @abstractmethod
    def get_size(self) -> int:
        """Get the size of the device.

        Returns:
            int: the size of this device in bytes
        """
        ...
        
    @abstractmethod
    def get_state(self) -> DeviceState:
        """Get the state of the device.

        Returns:
            DeviceState: the state of the device
        """
        ...
        
    @abstractmethod
    def attempt_bring_online(self) -> bool:
        """Attempt to bring the device to the fully online state.

        Returns:
            bool: True if the device is now online, False otherwise
        """
        ...
    @abstractmethod
    def mark_faulted(self) -> bool:
        """Mark the device as faulted
        Returns:
            bool: True if the device is now faulted, False otherwise
        """        
        
        


