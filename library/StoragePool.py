import logging
from typing import Optional

from library.Device.VirtualDevice import VirtualDevice
from library.BlockUser import BlockUser

class StoragePool:
    """A storage pool is a collection of virtual devices that can be used to store data, no redundancy is available at this level.
    The pool level is where the CoW functionality is implemented."""
    
    def __init__(self, name:str, devices:list[VirtualDevice], logger:Optional[logging.Logger]):
        """Create a storage pool from a list of virtual devices.
        
        Args:
            devices (list[VirtualDevice]): the devices in the storage pool
        Raises:
            ValueError: if the devices have different sizes or block sizes
        """
        
        if len(set(d.get_block_size() for d in devices)) != 1:
            raise ValueError("All devices must have the same block size.")
        self.name = name
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = sum(d.get_size() for d in devices)
        
        self._virtual_to_physical_block_map:dict[int, tuple[VirtualDevice, int]] = {} # maps virtual block numbers to Real blocks on a device
        self._physical_to_virtual_block_map:dict[tuple[VirtualDevice, int], int] = {} # maps real blocks to virtual block numbers
        
        self._physical_blocks_used:dict[VirtualDevice, dict[int, list[BlockUser]]] = {} # device -> (physical block -> users)
            
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        
        self._new_block_number = [0 for _ in range(len(devices))] # where to allocate the next block for each device
        self._device_blocks_used:dict[VirtualDevice, int] = {d:0 for d in devices} # how many blocks are in use on each device, used for CoW
        
    def _add_virtual_block_link(self, virtual_block:int, device:VirtualDevice, physical_block:int):
        self._virtual_to_physical_block_map[virtual_block] = (device, physical_block)
        self._physical_to_virtual_block_map[(device, physical_block)] = virtual_block
        
    def _remove_virtual_block_link(self, virtual_block:int):
        device, physical_block = self._virtual_to_physical_block_map.pop(virtual_block)
        self._physical_to_virtual_block_map.pop((device, physical_block))
        
    def _allocate_new_physical_block(self) -> tuple[VirtualDevice, int]:
        """allocate a physical block from the devices in the storage pool

        Returns:
            tuple[VirtualDevice, int]: a tuple of the device and physical block number that was allocated
        """     
        min_device = min(self._device_blocks_used.values())
        device = [d for d, blocks_used in self._device_blocks_used.items() if blocks_used == min_device][0]
        new_block_number = self._new_block_number[self._devices.index(device)]
        # increment until we find an unused one
        traversed_count = 0
        while new_block_number in self._physical_blocks_used.get(device, {}):
            new_block_number += 1
            traversed_count += 1
            if new_block_number >= device.get_size() // self._block_size:
                new_block_number = 0
            if traversed_count > device.get_size() // self._block_size:
                raise ValueError("All blocks in use.")
        self._device_blocks_used[device] += 1
        self._new_block_number[self._devices.index(device)] = new_block_number + 1
        if self._new_block_number[self._devices.index(device)] >= device.get_size() // self._block_size:
            self._new_block_number[self._devices.index(device)] = 0
        return device, new_block_number
    
    def _free_physical_block(self, device:VirtualDevice, physical_block_number:int):
        """Free a physical block on a device

        Args:
            device (VirtualDevice): the device to free the block on
            physical_block_number (int): the physical block number to free
        """        
        self._device_blocks_used[device] -= 1
        self._physical_blocks_used[device].pop(physical_block_number)
        self._remove_virtual_block_link(self._physical_to_virtual_block_map[(device, physical_block_number)])
        
    def read_physical_block(self, device:VirtualDevice, physical_block:int) -> bytes:
        """Read a block from a physical device

        Args:
            device (VirtualDevice): the device to read from
            physical_block (int): the block number to read

        Returns:
            bytes: the block data
        """
        return device.read_block(physical_block)
        
    def read_virtual_block(self, block_number:int) -> bytes:
        """Read a block from the storage pool.

        Args:
            block_number (int): the block number to read
            
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the block is not in use
            

        Returns:
            bytes: the block data
        """
        if block_number not in self._virtual_to_physical_block_map:
            raise ValueError(f"Block {block_number} not in use.")
        device, physical_block = self._virtual_to_physical_block_map[block_number]
        return self.read_physical_block(device, physical_block)
    
    def write_virtual_block(self, block_number:int, data:bytes, user: BlockUser) -> bool:
        """Write a block to the storage pool, this will allocate a new block for copy-on-write functionality.
        
        Args:
            block_number (int): the block number to write
            data (bytes): the data to write
            user (BlockUser): the user writing the block
            
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the data length is not the same as the block size
            ValueError: if the user does not have ownership of the block
            
        Returns:
            bool: True if the write was successful, False otherwise
        """
        if block_number < 0 or block_number >= self._size // self._block_size:
            raise ValueError(f"Block number {block_number} out of range.")
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        
        if block_number in self._virtual_to_physical_block_map:
            device, physical_block = self._virtual_to_physical_block_map[block_number]
            # check if the block is in use
            if user not in self._physical_blocks_used[device][physical_block]:
                raise ValueError(f"{self.name} Unallowed access: User {user} does not use block {block_number}")
            # remove the user from the block
            self._physical_blocks_used[device][physical_block].remove(user)
            # check if the block is in use by anyone else
            if len(self._physical_blocks_used[device][physical_block]) == 0:
                self._free_physical_block(device, physical_block)
        # allocate a new block
        new_device, new_physical_block = self._allocate_new_physical_block()
        success = new_device.write_block(new_physical_block, data)
        if not success:
            return False
        self._add_virtual_block_link(block_number, new_device, new_physical_block)
        if new_device not in self._physical_blocks_used:
            self._physical_blocks_used[new_device] = {}
        self._physical_blocks_used[new_device][new_physical_block] = [user]
        return True
    
    def release_ownership(self, block_number:int, user:BlockUser):
        """Release ownership of a block

        Args:
            block_number (int): the block number to release
            user (BlockUser): the user releasing the block
        """        
        if block_number not in self._virtual_to_physical_block_map:
            raise ValueError(f"Block {block_number} not in use.")
        device, physical_block = self._virtual_to_physical_block_map[block_number]
        if user not in self._physical_blocks_used[device][physical_block]:
            raise ValueError(f"User {user} does not own block {block_number}.")
        self._physical_blocks_used[device][physical_block].remove(user)
        if len(self._physical_blocks_used[device][physical_block]) == 0:
            self._free_physical_block(device, physical_block)
            
    def get_fullness(self) -> float:
        """Get the fullness of the storage pool

        Returns:
            float: the fullness as a percentage
        """        
        total_blocks = self._size // self._block_size
        used_space = sum(len(blocks) for blocks in self._physical_blocks_used.values())
        return used_space / total_blocks