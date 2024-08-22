from abc import ABC, abstractmethod

class BlockUser(ABC):
    """A block user is an object that holds references to blocks of data within a storage pool.
    eg: A dataset or snapshot.
    """    
    ...