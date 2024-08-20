from library.Device.VirtualDevice import VirtualDevice

class StoragePool:
    """A storage pool is a collection of virtual devices that can be used to store data, no redundancy is available at this level.
    The pool level is where the CoW functionality is implemented."""
    
    def __init__(self, devices:list[VirtualDevice]):
        """Create a storage pool from a list of virtual devices.
        
        Args:
            devices (list[VirtualDevice]): the devices in the storage pool
        Raises:
            ValueError: if the devices have different sizes or block sizes
        """
        self._devices = devices
        self._block_size = devices[0].get_block_size()
        self._size = sum(d.get_size() for d in devices)
        
    