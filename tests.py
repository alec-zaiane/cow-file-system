import logging


from library.Device.PhysicalDevice import PhysicalDevice
from library.Device.VirtualDevice import *
from library.StoragePool import StoragePool
import library.Device.DeviceState as States
from library.FileSystem import FileSystem


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
testLogger = logging.getLogger("test")
testLogger.setLevel(logging.ERROR)


def pretty_print_usage_stats(pool:StoragePool):
    active, snapshot, free = pool.get_usage_stats()
    total = active + snapshot + free
    print(f"Usage stats: {active} block(s) active ({active/total*100:.2f}%), {snapshot} block(s) in snapshot(s) ({snapshot/total*100:.2f}%), {free} block(s) free ({free/total*100:.2f}%)")

def test1():
    pd = PhysicalDevice("pd1", 100, 10)
    pd2 = PhysicalDevice("pd2", 100, 10)
    pd3 = PhysicalDevice("pd3", 100, 10)
    vd = VirtualDeviceFactory.create_virtual_device("vdev1", [pd, pd2, pd3], "mirror", testLogger)


    def print_all_details():
        important_blocks = [0,1]
        output:list[str] = []
        output.append(f"VDEV 1 state: {vd.get_state()}")
        for block in important_blocks:
            output.append(f"\tVDEV 1 block {block}: {vd.read_block(block) if isinstance(vd.get_state(), States.DeviceOnlineMixin) else 'N/A'}")
        output.append("==Physical Devices==")
        for device in [pd, pd2, pd3]:
            output.append(f"{device.name} state: {device.get_state()}")
            for block in important_blocks:
                output.append(f"\t{device.name} block {block}: {device.read_block(block) if isinstance(device.get_state(), States.DeviceOnlineMixin) else "N/A"}")
        
        print("\n".join(output))

    print_all_details()
    input("Created all devices, press enter to write to vdev1")

    testLogger.info("writing to vdev1")
    vd.write_block(0, b"HelloHello")
    vd.write_block(1, b"WorldWorld")

    input("Wrote to vdev1, press enter to print all details")
    print_all_details()
    input("Press enter to manually edit pd2's data to simulate a bad sector")
    testLogger.info("Manually editing pd2's data")
    pd2._data[10:20] = b"BadDataBadD" # type: ignore
    input("Edited pd2's data, press enter to discover error")
    assert isinstance(vd, VirtualDeviceMirror)
    vd.check_all_integrity()
    input("press enter to print all details")
    print_all_details()
    input("press enter to attempt repair")
    vd.check_all_integrity(repair=True)
    input("press enter to print all details")
    print_all_details()

def test2():
    
    pd1 = PhysicalDevice("pd1",100, 10)
    pd2 = PhysicalDevice("pd2",100, 10)
    pd3 = PhysicalDevice("pd3",100, 10)
    pd4 = PhysicalDevice("pd4",100, 10)
    vd1 = VirtualDeviceFactory.create_virtual_device("vdev1", [pd1, pd2], "mirror", testLogger)
    vd2 = VirtualDeviceFactory.create_virtual_device("vdev2", [pd3, pd4], "mirror", testLogger)
    sp = StoragePool("sp1", [vd1,vd2], testLogger)
    
    vd1.attempt_bring_online()
    vd2.attempt_bring_online()
    
    for i in range(20):
        string_to_write = f"Hellohel{i+1:02d}".encode()
        sp.write_virtual_block(i, string_to_write)
        pretty_print_usage_stats(sp)
        
    for i in range(20):
        expected_string = f"Hellohel{i+1:02d}".encode()
        assert sp.read_virtual_block(i) == expected_string
    print("All reads successful")
    
    snap = sp.capture_snapshot()
    print("Snapshot taken")
    
    for i in range(10):
        sp.free_virtual_block(i)
    print("Freed first 10 blocks, but snapshot still has them")
    pretty_print_usage_stats(sp)
    
    print("Attempting to write to snapshot...",end=" ")
    try:
        sp.write_virtual_block(0, b"Hellohello") # this currently steals snapshot blocks, bad
    except ValueError as e:
        assert str(e) == "All blocks in use."
        print("Caught expected error")
    else:
        print("Error not caught")
        raise Exception("Should have failed")
    
    pretty_print_usage_stats(sp)
    
    sp.delete_snapshot(snap)
    print("Snapshot deleted")
    try:
        sp.write_virtual_block(0, b"Hellohello")
    except ValueError as e:
        raise Exception("Should not have failed")
    else:
        print("Write successful")
    pretty_print_usage_stats(sp)
    
    # sp.write_virtual_block(5, b"Helloworld", b)
    # print(sp.get_fullness())
    
    
def test3():
    pd1 = PhysicalDevice("pd1",100, 10)
    vd1 = VirtualDeviceFactory.create_virtual_device("vdev1", [pd1], "stripe", testLogger)
    sp = StoragePool("sp1", [vd1], testLogger)
    sp.write_virtual_block(9, b"HelloHello")
    assert sp.read_virtual_block(9) == b"HelloHello"
    sp.write_virtual_block(9, b"WorldWorld")
    assert sp.read_virtual_block(9) == b"WorldWorld"
    usage = sp.get_usage_stats()
    assert usage == (1, 0, 9)
    
    snap = sp.capture_snapshot()
    sp.free_virtual_block(9)
    assert sp.read_virtual_block(9, snap) == b"WorldWorld"
    assert sp.get_usage_stats() == (0, 1, 9)
    print("Test 3 passed (snapshot read)")
    
test3()


def debug_print_storage_pool(sp:StoragePool):
    print(f"Storage pool {sp.name}:")
    used_blocks = sp.get_virtual_blocks_used()
    skipped_blocks:set[int] = set()
    for i in range(max(used_blocks)+1):
        closest_used = min(used_blocks, key=lambda x: abs(x-i))
        if abs(closest_used-i) > 1:
            skipped_blocks.add(i)
            
    for i in range(sp.get_num_blocks()):
        if i in skipped_blocks:
            if i-1 not in skipped_blocks:
                print("...")
            continue
        print(f"Block {i}: ".ljust(11), end="| ")
        if i not in used_blocks:
            print("__ "*sp.get_block_size(), end="")
            print(f"| {'_'*sp.get_block_size()} |")
            continue
        data = sp.read_virtual_block(i)
        for byte in data:
            print(f"{byte:02x}", end=" ")
        print("|", end=" ")
        for byte in data:
            print(chr(byte) if byte > 31 and byte < 127 else ".", end="")
        print(" |")
    print()

def test4():
    pd1 = PhysicalDevice("pd1",2048, 16)
    vd1 = VirtualDeviceFactory.create_virtual_device("vdev1", [pd1], "stripe", testLogger)
    sp = StoragePool("sp1", [vd1], testLogger)
    filesystem = FileSystem(sp)
    
    filesystem.write_file("file1", b"Hello World!")
    assert filesystem.read_file("file1").decode() == "Hello World!"
    filesystem.write_file("file2", b"Hello World again!")
    assert filesystem.read_file("file2").decode() == "Hello World again!"
    filesystem.write_file("file3", b"Hello World a third time!")
    assert filesystem.read_file("file3").decode() == "Hello World a third time!"
    print("Test 4 passed (filesystem read/write)")
test4()
    
    
    