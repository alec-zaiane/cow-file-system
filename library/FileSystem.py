from library.StoragePool import StoragePool
from library.FileSystemFileTable import FileSystemFileTable

class FileSystem:
    """The Filesystem interacts with a StoragePool to provide an easier to use interface
    Files are stored in the storage pool as blocks, the filesystem keeps track of which blocks belong to which file
    probably a bad way to do this, but the focus of this project was the copy-on-write functionality
    
    The file table will be written at the start of the storage pool, and gets updated as files are written
    the files themselves will be written at the end of the storage pool, sort of like the stack/heap structure in memory
    when the files cross the boundary of the file table, the system is full
    """
    def __init__(self, storage_pool:StoragePool):
        storage_pool.write_virtual_blocks(0, b"\x00"*storage_pool.get_block_size()*4)
        self._storage_pool = storage_pool
        
    def _read_file_table(self) -> FileSystemFileTable:
        """Read the file table from the storage pool

        Returns:
            dict[str, list[int]]: filename -> list of block numbers
        """
        table_length_bytes = self.get_pool().read_virtual_blocks_byte_count(0,4)
        file_table_length = int.from_bytes(table_length_bytes, "big")
        table_bytes = self.get_pool().read_virtual_blocks_byte_count(0, file_table_length+4)
        table_bytes = table_bytes[4:]
        return FileSystemFileTable(table_bytes)

    
    def _get_highest_used_block(self) -> int:
        """Find the lowest numbered block that is used"""
        table = self._read_file_table()
        lowest = self.get_pool().get_num_blocks()
        for _, block_list in table.file_table.values():
            for block in block_list:
                if block < lowest:
                    lowest = block
        return lowest
    
    def _get_used_blocks(self) -> set[int]:
        """Get a set of all blocks in use by files in the pool"""
        file_table = self._read_file_table()
        used_blocks:set[int] = set()
        for _, block_list in file_table.file_table.values():
            used_blocks.update(block_list)
        return used_blocks
    
    def _find_free_space(self, num_blocks:int) -> list[int]:
        """Find free space in the storage pool

        Args:
            num_blocks (int): number of blocks to find

        Returns:
            list[int]: a list of block numbers that are free, will be length num_blocks
        """
        used_blocks = self._get_used_blocks()
        free_blocks:list[int] = []
        for i in range(0, self.get_pool().get_num_blocks(), -1):
            if i not in used_blocks:
                free_blocks.append(i)
            if len(free_blocks) == num_blocks:
                return free_blocks
        raise ValueError("Filesystem is full, cannot find free space")
        
        
        
    def _write_file_table(self, file_table:FileSystemFileTable):
        """Write the file table to the storage pool

        Args:
            file_table (dict[str, list[int]]): filename -> list of block numbers
        Raises:
            ValueError: if the filesystem is full, or if there is no way to write the file table to the storage pool
            OverflowError: if the file table is too large to be written (greater than 65535 bytes)
        """
        encoded = file_table.encode()
        encoded = len(encoded).to_bytes(4, "big") + encoded
        lowest_used_block = self._get_highest_used_block()
        if lowest_used_block < len(encoded):
            raise ValueError("Filesystem is full, or needs to be defragmented")
        self.get_pool().write_virtual_blocks(0, encoded)
        
        
    def _update_file_table(self, filename:str, file_length:int, block_numbers:list[int]):
        """Update the file table with the given block numbers

        Args:
            filename (str): filename to update
            block_numbers (list[int]): list of block numbers
        """
        file_table = self._read_file_table()
        file_table.file_table[filename] = (file_length, block_numbers)
        self._write_file_table(file_table)
        
    def write_file(self, filename:str, data:bytes):
        """Write a file to the storage pool

        Args:
            filename (str): filename to write
            data (bytes): data to write
            
        Raises:
            ValueError: if the filesystem is full
            ValueError: if the filename is too long (greater than 255 characters)
        """        
        if len(filename) > 255:
            raise ValueError("Filename too long")
        highest_free_block = self._get_highest_used_block() - 1
        test_filetable = self._read_file_table()
        num_required_blocks = self.get_pool().bytes2block_count(len(data))
        blocks_to_write = [highest_free_block-i for i in range(num_required_blocks)]
        pool_block_size = self.get_pool().get_block_size()
        test_filetable.file_table[filename] = (len(data), [])
        data_length = len(data)
        for block_to_write in blocks_to_write:
            if block_to_write in self._get_used_blocks():
                raise ValueError("Filesystem is full, or needs to be defragmented")
            data_fragment = data[:pool_block_size]
            if len(data_fragment) < pool_block_size:
                data_fragment += b"\x00"*(pool_block_size-len(data_fragment))
            data = data[pool_block_size:]
            self.get_pool().write_virtual_block(block_to_write, data_fragment)
            test_filetable.file_table[filename][1].append(block_to_write)
        encoded_test_filetable = test_filetable.encode()
        if len(encoded_test_filetable)+4 > highest_free_block:
            raise ValueError("Filesystem is full, or needs to be defragmented")
        self._update_file_table(filename, data_length, test_filetable.file_table[filename][1])
        
    def read_file(self, filename:str) -> bytes:
        """Read a file from the storage pool

        Args:
            filename (str): filename to read

        Raises:
            ValueError: if the file does not exist
            ValueError: if the filename is too long (greater than 255 characters)
            
        Returns:
            bytes: the data read
        """        
        filetable = self._read_file_table()
        if filename not in filetable:
            raise ValueError("File does not exist")
        byte_count, block_numbers = filetable.file_table[filename]
        output_list:list[bytes] = []
        for block in block_numbers:
            output_list.append(self.get_pool().read_virtual_block(block))
        return b"".join(output_list)[:byte_count]
    
    def get_pool(self) -> StoragePool:
        """Get the storage pool

        Returns:
            StoragePool: the storage pool
        """
        return self._storage_pool