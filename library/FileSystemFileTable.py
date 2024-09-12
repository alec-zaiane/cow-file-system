class FileSystemFileTable:
    def __init__(self, data:bytes):
        """Create a new file table from the given data, or an empty file table if no data is given.

        Args:
            data (bytes): the encoded file table, or an empty bytes object (for a new file table)
        """
        # file table is {filename -> (size in bytes, ordered list of block numbers)}
        if data:
            self.file_table:dict[str, tuple[int,list[int]]] = self._decode_file_table(data)
        else:
            self.file_table:dict[str, tuple[int,list[int]]] = {}
            
    def encode(self) -> bytes:
        """
        Raises:
            OverflowError: if items in the file table exceed the maximum size: 65535 bytes for filename, 4GB for file size, 65535 blocks per file
        
        Returns:
            bytes: byte encoded representation of the file table
        """
        list_output:list[bytes] = []
        for filename, (size, block_numbers) in self.file_table.items():
            list_output.append(len(filename).to_bytes(2, "big")) # path length can be at most 65535 bytes
            list_output.append(filename.encode())
            list_output.append(size.to_bytes(4, "big")) # files can be at most 4GB
            list_output.append(len(block_numbers).to_bytes(2, "big")) # files can have at most 65535 blocks
            list_output.append(bytes(block_numbers))
        return b"".join(list_output)
    
    def _decode_file_table(self, data:bytes):
        """decode the file table from the given data

        Args:
            data (bytes): data to decode
        """
        file_table:dict[str, tuple[int, list[int]]] = {}
        i = 0
        while i < len(data):
            filename_length = int.from_bytes(data[i:i+2], "big")
            i += 2
            filename = data[i:i+filename_length].decode()
            i += filename_length
            size = int.from_bytes(data[i:i+4], "big")
            i += 4
            block_count = int.from_bytes(data[i:i+2], "big")
            i += 2
            block_numbers = list(data[i:i+block_count])
            i += block_count
            file_table[filename] = (size, block_numbers)
        return file_table        
    
    def __contains__(self, filename:object) -> bool:
        return filename in self.file_table
    
    def __getitem__(self, filename:object) -> tuple[int, list[int]]:
        if not isinstance(filename, str):
            raise TypeError("filename must be a string")
        return self.file_table[filename]