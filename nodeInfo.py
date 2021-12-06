import numpy as np
from configs import Configs
from device import Device

Configs = Configs()

class NodeInfo():
    
    def __init__(self, node, dev_index):
        self.gpuCount = self.getGPUCount(node)
        self.processes = self.getProcesses(node)
        self.gpuUtil = self.getGPUUtil(node)
        self.memoryCapacity = self.getMemoryCapacity(node)
        self.memoryUsed = self.getMemoryUsed(node)
        self.memUtil = self.getMemUtil(node)
        self.deviceInfo = self.getDeviceInfo(node, dev_index)

    # get GPU count of node
    def getGPUCount(self, node):
        try:
            if node.status.capacity[Configs.GPU_COUNT] is not None:
                return  int(node.status.capacity[Configs.GPU_COUNT])   
        except Exception as e:
            print("Get GPU count of node %s error!", node.metadata.name)
            print(e)
        return 0

    # get processe of node
    def getProcesses(self, node):
        try:
            if node.metadata.annotations[Configs.GPU_PROCESS] is not None:
                process = node.metadata.annotations[Configs.GPU_PROCESS]
                if process != "null":
                    processes = np.fromstring(process[1: -2], dtype=str, sep=',')
                    return processes
        except Exception as e:
            print("Get processes of node " + node.metadata.name + " error!")
            print(e)
        return []

    # get gpu memory capacity of node 
    def getMemoryCapacity(self, node):
        try:
            tmp = node.metadata.annotations[Configs.MEM_CAPACITY]
            mem_capacity = np.fromstring(tmp[1: -1], dtype=int, sep=',')
            return mem_capacity
        except Exception as e:
            print("Get memory capacity of node " + node.metadata.name + " error!")
            print(e)
        return []

    # get gpu memory used of node 
    def getMemoryUsed(self, node):
        try:
            tmp = node.metadata.annotations[Configs.MEM_USED]
            mem_used = np.fromstring(tmp[1: -1], dtype=int, sep=',')
            return mem_used
        except Exception as e:
            print("Get memory used of node " + node.metadata.name + " error!")
            print(e)
        return []

    # get gpu util of node 
    def getGPUUtil(self, node):
        try:
            tmp = node.metadata.annotations[Configs.GPU_UTIL]
            gpu_util = np.fromstring(tmp[1: -1], dtype=int, sep=',')
            return gpu_util
        except Exception as e:
            print("Get GPU utilization of node " + node.metadata.name + " error!")
            print(e)
        return []        

    # get gpu memory util of node 
    def getMemUtil(self, node):
        try:
            tmp = node.metadata.annotations[Configs.MEM_UTIL]
            mem_util = np.fromstring(tmp[1: -1], dtype=int, sep=',')
            return mem_util
        except Exception as e:
            print("Get GPU memory utilization of node " + node.metadata.name + " error!")
            print(e)
        return [] 

    # get device x info of node
    def getDeviceInfo(self, node, dev_index):
        try:
            if len(self.processes) == 0: processes = [] 
            elif len(self.processes) == 1 and dev_index == 1: processes = []
            else: processes = self.processes[dev_index]    
            gpu_util = self.gpuUtil[dev_index]
            mem_capacity = self.memoryCapacity[dev_index]
            mem_used = self.memoryUsed[dev_index]
            mem_util = self.memUtil[dev_index]
            return Device(processes, gpu_util, mem_capacity, mem_used, mem_util)

        except Exception as e:
            print("Get device " + str(dev_index) + " of node " + node.metadata.name + " error!")
            print(e)
        return None        
    



