import logging
from typing import Optional


from library.Device.PhysicalDevice import PhysicalDevice
from library.Device.VirtualDevice import *
from library.StoragePool import StoragePool
import library.Device.DeviceState as States


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
testLogger = logging.getLogger("test")
testLogger.setLevel(logging.ERROR)


def pretty_print_usage_stats(pool:StoragePool):
    active, snapshot, free = pool.get_usage_stats()
    total = active + snapshot + free
    print(f"Usage stats: {active} block(s) active ({active/total*100:.2f}%), {snapshot} block(s) in snapshot ({snapshot/total*100:.2f}%), {free} block(s) free ({free/total*100:.2f}%)")

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
        
    # sp.read_virtual_block(5)
    # sp.release_ownership(5, b)
    # print(sp.get_fullness())
    # sp.write_virtual_block(5, b"Helloworld", b)
    # print(sp.get_fullness())
    
    
test2()
    