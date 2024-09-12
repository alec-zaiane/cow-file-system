import logging
from typing import Optional

from library.Device.VirtualDevice import VirtualDevice
from library.PhysicalVirtualBlockMapping import PhysicalVirtualBlockMapping
from library.Snapshot import Snapshot

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
        
        self._mapping:PhysicalVirtualBlockMapping = PhysicalVirtualBlockMapping()
        
        self._snapshots:list[Snapshot] = []
            
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        
        self._new_block_number = [0 for _ in range(len(devices))] # where to start looking for each device's new block allocations
        
    def _get_physical_blocks_used(self, filter_device:Optional[VirtualDevice]=None) -> dict[VirtualDevice, set[int]]:
        """Get the physical blocks used by each device, whether in snapshots or the active mapping

        Args:
            filter_device (Optional[VirtualDevice], optional): if set, filter to this device only. Defaults to None.

        Returns:
            dict[VirtualDevice, set[int]]: device -> {physical block numbers in use}
        """           
        physical_blocks_used:dict[VirtualDevice,set[int]] = {}
        for device in self._devices:
            physical_blocks_used[device] = set()
        for mapping in [self._mapping] + [snapshot.get_mapping() for snapshot in self._snapshots]:
            for device, blocks in mapping.get_physical_block_usage_sets().items():
                if filter_device is not None and device != filter_device:
                    continue
                if not isinstance(device, VirtualDevice):
                    raise SyntaxError("Device is not a virtual device, this should never happen and is here for type checking.")
                physical_blocks_used[device].update(blocks)
        return physical_blocks_used
        
    def _allocate_new_physical_block(self) -> tuple[VirtualDevice, int]:
        """allocate a physical block from the devices in the storage pool
        
        Raises:
            ValueError: if all blocks are in use

        Returns:
            tuple[VirtualDevice, int]: a tuple of the device and physical block number that was allocated
        """
        physical_blocks_used = self._get_physical_blocks_used()
        min_device = min({d: len(blocks) for d, blocks in physical_blocks_used.items()}.items(), key=lambda x: x[1])[0] 
        new_block_number = self._new_block_number[self._devices.index(min_device)]
        # increment until we find an unused one
        traversed_count = 0
        while new_block_number in physical_blocks_used.get(min_device, set()):
            new_block_number += 1
            traversed_count += 1
            if new_block_number >= min_device.get_size() // self._block_size:
                new_block_number = 0
            if traversed_count > min_device.get_size() // self._block_size:
                raise ValueError("All blocks in use.")
        self._new_block_number[self._devices.index(min_device)] = new_block_number + 1
        if self._new_block_number[self._devices.index(min_device)] >= min_device.get_size() // self._block_size:
            self._new_block_number[self._devices.index(min_device)] = 0
        return min_device, new_block_number
    
    def _free_physical_block(self, device:VirtualDevice, physical_block_number:int):
        """Free a physical block on a device

        Args:
            device (VirtualDevice): the device to free the block on
            physical_block_number (int): the physical block number to free
        """
        self._mapping.unenroll_mapping_physical(device, physical_block_number)
        
    def bytes2block_count(self, bytes:int) -> int:
        """Given a number of bytes, return the number of blocks needed to store that many bytes

        Args:
            bytes (int): the number of bytes to convert

        Returns:
            int: the number of blocks needed to store that data
        """
        return (bytes + self._block_size - 1) // self._block_size        
        
    def read_physical_block(self, device:VirtualDevice, physical_block:int) -> bytes:
        """Read a block from a physical device

        Args:
            device (VirtualDevice): the device to read from
            physical_block (int): the block number to read

        Returns:
            bytes: the block data
        """
        return device.read_block(physical_block)
        
    def read_virtual_block(self, block_number:int, snapshot:Optional[Snapshot] = None) -> bytes:
        """Read a block from the storage pool.

        Args:
            block_number (int): the block number to read
            snapshot (Optional[None], optional): If set, this snapshot will be used for the reading. Defaults to None.
            
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the block is not in use
            
        Returns:
            bytes: the block data
        """
        mapping = snapshot.get_mapping() if snapshot is not None else self._mapping
        if not mapping.check_virtual_block(block_number):
            raise ValueError(f"Block {block_number} not in use.")
        device, physical_block = mapping.get_physical_block(block_number)
        if not isinstance(device, VirtualDevice):
            raise SyntaxError("Device is not a virtual device, this should never happen and is here for type checking.")
        return self.read_physical_block(device, physical_block)
    
    def read_virtual_blocks(self, start_block:int, end_block:int, snapshot:Optional[None] = None) -> bytes:
        """Read multiple blocks from the storage pool, starting at the given block number

        Args:
            start_block (int): block number to start reading at
            end_block (int): block number to stop reading at (inclusive)
            snapshot (Optional[None], optional): If set, this snapshot will be used for the reading. Defaults to None.

        Returns:
            bytes: the data read
        """
        data = b""
        for i in range(start_block, end_block+1):
            data += self.read_virtual_block(i, snapshot)
        return data
    
    def read_virtual_blocks_byte_count(self, start_block:int, bytes_count:int, snapshot:Optional[None] = None) -> bytes:
        """Read a number of bytes from the storage pool, starting at the given block number

        Args:
            start_block (int): block number to start reading at
            bytes_count (int): number of bytes to read
            snapshot (Optional[None], optional): If set, this snapshot will be used for the reading. Defaults to None.

        Returns:
            bytes: the data read
        """
        end_block = start_block + self.bytes2block_count(bytes_count)
        return self.read_virtual_blocks(start_block, end_block, snapshot)[:bytes_count]        
    
    def write_virtual_block(self, block_number:int, data:bytes) -> bool:
        """Write a block to the storage pool, this will allocate a new block for copy-on-write functionality.
        
        Args:
            block_number (int): the block number to write
            data (bytes): the data to write
            
        Raises:
            ValueError: if the block number is out of range
            ValueError: if the data length is not the same as the block size
            
        Returns:
            bool: True if the write was successful, False otherwise
        """
        if block_number < 0 or block_number >= self._size // self._block_size:
            raise ValueError(f"Block number {block_number} out of range.")
        if len(data) != self._block_size:
            raise ValueError(f"Tried to write {len(data)} bytes to a block of size {self._block_size}.")
        
        # Write to a new block
        new_device, new_physical_block = self._allocate_new_physical_block()
        success = new_device.write_block(new_physical_block, data)
        if not success:
            return False
        if not self._mapping.check_virtual_block(block_number): # if its a fresh block, no need to update mapping
            self._mapping.enroll_mapping(new_device, new_physical_block, block_number)
            return True
        
        # this block was in use, check if we need to remove the old block
        self._mapping.update_mapping(block_number, new_device, new_physical_block)
        # if not isinstance(old_device, VirtualDevice):
        #     raise SyntaxError("Device is not a virtual device, this should never happen and is here for type checking.")
        # # remove user from old block
        # if old_block not in self._get_physical_blocks_used(old_device)[old_device]:
        #     self._free_physical_block(old_device, old_block) # TODO this is the current source of the keyerror bug
        return True
    
    def write_virtual_blocks(self, start_block:int, data:bytes) -> bool:
        """Write multiple blocks to the storage, starting at the given block number

        Args:
            start_block (int): block number to start writing at
            data (bytes): data to write, will be padded with zeroes if not a multiple of the block size
            
        Raises:
            ValueError: if the start block is out of range
            ValueError: if the calculated end block is out of range
            
        Returns:
            bool: True if all writes were successful, False otherwise
        """
        pad_length = self._block_size - (len(data) % self._block_size)
        data += b"\x00" * pad_length
        end_block = start_block + len(data) // self._block_size
        if start_block < 0 or start_block > self._size // self._block_size:
            raise ValueError(f"Start block number {start_block} out of range.")
        if end_block < 0 or end_block > self._size // self._block_size:
            raise ValueError(f"End block number {end_block} out of range.")
        all_successful = True
        for i, block_to_write in enumerate(range(start_block, end_block)):
            data_fragment = data[i*self._block_size:(i+1)*self._block_size]
            all_successful = all_successful and self.write_virtual_block(block_to_write, data_fragment)
        return all_successful
                
    
    def free_virtual_block(self, block_number:int):
        """Free a block from the current mapping

        Args:
            block_number (int): block number to free

        Raises:
            ValueError: if the block was not in use
        """
        if not self._mapping.check_virtual_block(block_number):
            raise ValueError(f"Block {block_number} not in use.")
        device, physical_block = self._mapping.get_physical_block(block_number)
        if not isinstance(device, VirtualDevice):
            raise SyntaxError("Device is not a virtual device, this should never happen and is here for type checking.")
        
        self._free_physical_block(device, physical_block)
            
    def get_fullness(self) -> float:
        """Get the fullness of the storage pool

        Returns:
            float: the fullness between 0 and 1
        """        
        total_blocks = self._size // self._block_size
        used_space = sum(len(blocks) for blocks in self._get_physical_blocks_used().values())
        return used_space / total_blocks
    
    def get_free_block_count(self) -> int:
        """Get the number of free blocks in the storage pool

        Returns:
            int: number of blocks available
        """
        total_blocks = self._size // self._block_size
        used_space = sum(len(blocks) for blocks in self._get_physical_blocks_used().values())
        return total_blocks - used_space
    
    def get_usage_stats(self) -> tuple[int,int,int]:
        """Get the usage statistics for the storage pool

        Returns:
            tuple[int,int,int]: actively used blocks, snapshot used blocks, free blocks
        """
        total_blocks = self._size // self._block_size
        current_blocks = self._mapping.get_virtual_block_usage_set()
        snapshot_blocks:set[int] = set()
        for snapshot in self._snapshots:
            usage_set = snapshot.get_mapping().get_virtual_block_usage_set()
            snapshot_blocks.update(usage_set)
        exclusive_snapshot_blocks = snapshot_blocks - current_blocks
        return len(current_blocks), len(exclusive_snapshot_blocks), total_blocks - len(current_blocks) - len(exclusive_snapshot_blocks)
    
    def capture_snapshot(self) -> Snapshot:
        """Capture a snapshot of the storage pool"""
        snapshot = Snapshot(self._mapping)
        self._snapshots.append(snapshot)
        return snapshot
    
    def get_snapshots(self) -> list[Snapshot]:
        """Get a list of snapshots

        Returns:
            list[Snapshot]: list of snapshots
        """
        return self._snapshots
    
    def delete_snapshot(self, snapshot:Snapshot):
        """Delete a snapshot from the storage pool
        Remember to delete the reference to the snapshot as well, as it is no longer useful
        
        Args:
            snapshot (Snapshot): the snapshot to delete
        """
        self._snapshots.remove(snapshot)
        
    def get_num_blocks(self) -> int:
        """
        Returns:
            int: The number of blocks in the pool
        """       
        return self._size // self._block_size 
    
    def get_block_size(self) -> int: 
        """
        Returns:
            int: The block size in bytes
        """        
        return self._block_size
    
    def get_virtual_blocks_used(self) -> set[int]:
        """
        Returns:
            set[int]: The set of virtual blocks in use
        """        
        return self._mapping.get_virtual_block_usage_set()
    
        