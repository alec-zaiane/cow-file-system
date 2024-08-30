from library.StoragePool import StoragePool

class FileSystem:
    """The Filesystem interacts with a StoragePool to provide an easier to use interface
    """    
    def __init__(self, storage_pool:StoragePool):
        self._storage_pool = storage_pool
        
    def _read_file_table(self) -> dict[str, list[int]]:
        """Read the file table from the storage pool

        Returns:
            dict[str, list[int]]: filename -> list of block numbers
        """
        ...
        
    def _write_tile_table(self, file_table:dict[str, list[int]]):
        """Write the file table to the storage pool

        Args:
            file_table (dict[str, list[int]]): filename -> list of block numbers
        """
        ...
        
    def _update_file_table(self, filename:str, block_numbers:list[int]):
        """Update the file table with the given block numbers

        Args:
            filename (str): filename to update
            block_numbers (list[int]): list of block numbers
        """
        ...
        
    def write_file(self, filename:str, data:bytes):
        """Write a file to the storage pool

        Args:
            filename (str): filename to write
            data (bytes): data to write
            
        Raises:
            ValueError: if the filesystem is full
        """        
        ...
        
    def read_file(self, filename:str) -> bytes:
        """Read a file from the storage pool

        Args:
            filename (str): filename to read

        Raises:
            ValueError: if the file does not exist
            
        Returns:
            bytes: the data read
        """        
        ...
    
    def get_pool(self) -> StoragePool:
        """Get the storage pool

        Returns:
            StoragePool: the storage pool
        """
        return self._storage_pool