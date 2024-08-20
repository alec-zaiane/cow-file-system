from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Type


class DeviceState(ABC):
    @abstractmethod
    def transition_to(self, state:DeviceState) -> DeviceState:
        ...
        
    def _check_transition(self, target:DeviceState, acceptable:list[Type[DeviceState]]) -> bool:
        for a in acceptable:
            if isinstance(target, a):
                return True
        return False
    
    def __eq__(self, other:object):
        return self.__class__ == other.__class__
    
    def __str__(self):
        return self.__class__.__name__
    
class DeviceOfflineMixin(DeviceState):
    ...
    
class DeviceOnlineMixin(DeviceState):
    ...
    
class DeviceFaultedMixin(DeviceState):
    ...
    

# ============================= Physical Device States =============================
# physical devices can be in one of three states: online, offline, faulted, faultedOffline, disconnected
# online devices are fully operational, can become offline or faulted
# offline devices are not operational (eg powered off), can become online, disconnected
# faulted devices have encountered an error involving data loss (eg bad sector), can become faulted offline
# faulted offline devices are faulted and offline, can become faulted
# disconnected devices are not connected to the system, can become offline, offline faulted


class PhysicalDeviceState(DeviceState):
    ...
    
class PhysicalDeviceOnline(PhysicalDeviceState, DeviceOnlineMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [PhysicalDeviceOffline, PhysicalDeviceFaulted]):
            return state
        return self
    
class PhysicalDeviceOffline(PhysicalDeviceState, DeviceOfflineMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [PhysicalDeviceOnline, PhysicalDeviceDisconnected]):
            return state
        return self
    
class PhysicalDeviceFaulted(PhysicalDeviceState, DeviceFaultedMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [PhysicalDeviceFaultedOffline, PhysicalDeviceOnline]):
            return state
        return self
    
class PhysicalDeviceFaultedOffline(PhysicalDeviceState, DeviceOfflineMixin, DeviceFaultedMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [PhysicalDeviceFaulted]):
            return state
        return self    
    
class PhysicalDeviceDisconnected(PhysicalDeviceState, DeviceOfflineMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [PhysicalDeviceOffline, PhysicalDeviceFaultedOffline]):
            return state
        return self
    
# ============================= Virtual Device States =============================
# virtual devices can be in one of three states: online, offline, faulted, faultedOffline, degraded
# online devices are fully operational, can become offline, faulted, degraded
# offline devices are not operational, can become online, faulted, degraded
# faulted devices have encountered an error involving data loss (eg bad sector), can become faulted offline, online, degraded
# faulted offline devices are faulted and offline, can become faulted
# degraded devices are operational, but not at full capacity, (eg: one of the physical devices is offline/faulted, but no data loss), can become offline, online, faulted


class VirtualDeviceState(DeviceState):
    ...
    
    
class VirtualDeviceOnline(VirtualDeviceState, DeviceOnlineMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [VirtualDeviceOffline, VirtualDeviceFaulted, VirtualDeviceDegraded]):
            return state
        return self
    
class VirtualDeviceOffline(VirtualDeviceState, DeviceOfflineMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [VirtualDeviceOnline, VirtualDeviceFaulted, VirtualDeviceDegraded]):
            return state
        return self
    
class VirtualDeviceFaulted(VirtualDeviceState, DeviceFaultedMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [VirtualDeviceFaultedOffline, VirtualDeviceOnline, VirtualDeviceDegraded]):
            return state
        return self
    
class VirtualDeviceFaultedOffline(VirtualDeviceState, DeviceOfflineMixin, DeviceFaultedMixin):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [VirtualDeviceFaulted]):
            return state
        return self
    
class VirtualDeviceDegraded(VirtualDeviceState):
    def transition_to(self, state:DeviceState):
        if self._check_transition(state, [VirtualDeviceOffline, VirtualDeviceOnline, VirtualDeviceFaulted]):
            return state
        return self