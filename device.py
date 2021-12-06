
class Device():

    def __init__(self, processes, gpu_util, mem_capacity, mem_used, mem_util):
        self.processes = processes
        self.gpu_util = gpu_util
        self.mem_capacity = mem_capacity
        self.mem_used = mem_used
        self.mem_util = mem_util

    # get processe of device x
    def getProcesses(self):
        try:
            return self.processes
        except Exception:
            print("Get processes of device %d error!")
        return []

    # get gpu memory capacity of device x
    def getMemoryCapacity(self):
        try:
            return self.mem_capacity
        except Exception:
            print("Get memory capacity of device %d error!")
        return 0

    # get gpu memory used of device x
    def getMemoryUsed(self):
        try:
            return self.mem_used
        except Exception:
            print("Get memory used of device %d error!")
        return 0

    # get gpu util of device x
    def getGPUUtil(self):
        try:
            return self.gpu_util
        except Exception:
            print("Get GPU utilization of device %d error!")
        return 0        

    # get gpu memory util of device x
    def getMemUtil(self):
        try:
            return self.mem_util
        except Exception:
            print("Get memory utilization of device %d error!")
        return 0       

        