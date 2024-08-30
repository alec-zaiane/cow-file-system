# cow-file-system
*"We have ZFS at home", the ZFS at home:*


My own implementation of the logic behind a copy-on-write file system

This project simulates structure from the physical disks up to the filesystem level

core structure somewhat like [ZFS](https://arstechnica.com/information-technology/2020/05/zfs-101-understanding-zfs-storage-and-performance/)

PhysicalDevice -> VirtualDevice -> StoragePool -> StorageDataset -> FileSystem -> File

For simplicity, all devices up to the StoragePool must have the same block size for now

While physical redundancy is part of the structure of the project, time-based failures are not protected, for example crashes during the middle of a write.