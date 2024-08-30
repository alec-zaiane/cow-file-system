from __future__ import annotations

from library.Device.Device import Device

class PhysicalVirtualBlockMapping:
    def __init__(self):
        self._physical_to_virtual_map: dict[Device, dict[int, int]] = {}
        self._virtual_to_physical_map: dict[int, tuple[Device, int]] = {}
        
    def enroll_mapping(self, device:Device, physical_block:int, virtual_block:int):
        """Enroll a mapping in the mapping table.

        Args:
            device (PhysicalDevice): physical device to map
            physical_block (int): block on that device to map
            virtual_block (int): virtual block to link to
            
        Raises:
            ValueError: if the virtual block is already in use
            ValueError: if the physical block is already in use
        """
        if self.check_virtual_block(virtual_block):
            raise ValueError(f"Virtual block {virtual_block} already in use.")
        if self.check_physical_block(device, physical_block):
            raise ValueError(f"Physical block {physical_block} on device {device.name} already in use.")
        if device not in self._physical_to_virtual_map:
            self._physical_to_virtual_map[device] = {}
        self._physical_to_virtual_map[device][physical_block] = virtual_block
        self._virtual_to_physical_map[virtual_block] = (device, physical_block)
        
    def update_mapping(self, virtual_block:int, new_device:Device, new_physical_block:int) -> tuple[Device, int]:
        """Update a mapping in the mapping table.

        Args:
            virtual_block (int): the virtual block to update
            new_device (Device): the new physical device for that block
            new_physical_block (int): the new physical block for that block
            
        Raises:
            ValueError: if the virtual block is not in use
            ValueError: if the physical block is already in use
            
        Returns:
            tuple[Device, int]: the old device and physical block
        """
        if not self.check_virtual_block(virtual_block):
            raise ValueError(f"Virtual block {virtual_block} not in use.")
        if self.check_physical_block(new_device, new_physical_block):
            raise ValueError(f"Physical block {new_physical_block} on device {new_device.name} already in use, cannot update mapping.")
        old_device, old_physical_block = self.get_physical_block(virtual_block)
        self._virtual_to_physical_map[virtual_block] = (new_device, new_physical_block)
        self._physical_to_virtual_map[new_device][new_physical_block] = virtual_block
        self._physical_to_virtual_map[old_device].pop(old_physical_block)
        return old_device, old_physical_block
        
        
    def unenroll_mapping(self, virtual_block:int):
        """Remove a mapping from the mapping table given the virtual block.
        
        Args:
            virtual_block (int): virtual block to remove
            
        Raises:
            ValueError: if the virtual block is not in use
        """
        if not self.check_virtual_block(virtual_block):
            raise ValueError(f"Virtual block {virtual_block} not in use.")
        device, physical_block = self._virtual_to_physical_map.pop(virtual_block)
        self._physical_to_virtual_map[device].pop(physical_block)
        
    def unenroll_mapping_physical(self, device:Device, physical_block:int):
        """Remove a mapping from the mapping table given the physical block.

        Args:
            device (PhysicalDevice): physical device to remove the mapping from
            physical_block (int): block on that device to remove the mapping from
        """        
        self.unenroll_mapping(self.get_virtual_block(device, physical_block))
        
    def get_physical_block(self, virtual_block:int) -> tuple[Device, int]:
        """Get the physical block mapped to a virtual block.
        
        Args:
            virtual_block (int): block to get the physical block for
            
        Returns:
            tuple[PhysicalDevice, int]: device and block number on that device
        
        Raises:
            KeyError: if the virtual block is not mapped
        """
        return self._virtual_to_physical_map[virtual_block]
    
    def get_virtual_block(self, device:Device, physical_block:int) -> int:
        """Get the virtual block mapped to a physical block.

        Args:
            device (PhysicalDevice): device that the block is on
            physical_block (int): block on that device

        Returns:
            int: virtual block linked to the physical block
            
        Raises:
            KeyError: if the physical block is not mapped
        """        
        return self._physical_to_virtual_map[device][physical_block]
    
    def check_virtual_block(self, virtual_block:int) -> bool:
        """Check if a virtual block is mapped.

        Args:
            virtual_block (int): block to check

        Returns:
            bool: True if the block is mapped, False otherwise
        """        
        return virtual_block in self._virtual_to_physical_map
    
    def check_physical_block(self, device:Device, physical_block:int) -> bool:
        """Check if a physical block is mapped.

        Args:
            device (PhysicalDevice): device that the block is on
            physical_block (int): block on that device

        Returns:
            bool: True if the block is mapped, False otherwise
        """        
        return physical_block in self._physical_to_virtual_map.get(device, {})
    
    def get_virtual_block_usage_set(self) -> set[int]:
        """Get the currently mapped virtual blocks.
        Useful for calculating usage statistice.
        
        Returns:
            set[int]: set of virtual blocks
        """
        return set(self._virtual_to_physical_map.keys())
    
    def get_physical_block_usage_sets(self) -> dict[Device, set[int]]:
        """Get the currently mapped physical blocks for each device.

        Returns:
            dict[Device, set[int]]: Device -> {physical blocks mapped}
        """
        output:dict[Device,set[int]] = {}
        for device, mapping in self._physical_to_virtual_map.items():
            output[device] = set(mapping.keys())
        return output
    
    def get_snapshot(self) -> PhysicalVirtualBlockMapping:
        new_map = PhysicalVirtualBlockMapping()
        for device in self._physical_to_virtual_map:
            for physical_block, virtual_block in self._physical_to_virtual_map[device].items():
                new_map.enroll_mapping(device, physical_block, virtual_block)
        return new_map
    
    