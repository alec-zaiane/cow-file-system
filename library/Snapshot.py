from library.PhysicalVirtualBlockMapping import PhysicalVirtualBlockMapping

class Snapshot:
    """A snapshot holds a snapshot of the data in a storage pool at the time it was taken.
    """
    def __init__(self, mapping:PhysicalVirtualBlockMapping):
        self._mapping = mapping.get_snapshot()
        
    def get_mapping(self) -> PhysicalVirtualBlockMapping:
        return self._mapping