# coding:utf-8
from kubernetes import client
import kubernetes as k8s
import numpy as np
 
# refer: https://blog.csdn.net/qq_31152023/article/details/114286448

# k8s自定义调度器
# python : 3.8
# kubernetes : V17.14.0a1
# numpy  : V1.20.1

gpu_request = 0

# get avaliable nodes
def getUseNode(k8sCoreV1api, namespace):
    # get all nodes
    nodeInstance = k8sCoreV1api.list_node()
    podInstance = k8sCoreV1api.list_namespaced_pod(namespace)
    
    useNodeName = []
    for i in podInstance.items:
        if i.status.phase == 'Pending' and i.spec.node_name is None and i.spec.scheduler_name is not None:
            pod_requests = i.spec.containers[0].resources.requests
            global gpu_request
            print(pod_requests)
            if "nvidia.com/gpu" in pod_requests.keys():   
                gpu_request = int(pod_requests["nvidia.com/gpu"])
            if "memory" in pod_requests.keys():
                memory_request = int(pod_requests["memory"][:-1])
            if "cpu" in pod_requests.keys():
                cpu_request = int(pod_requests["cpu"])
            for j in nodeInstance.items:
                if j.status.conditions[-1].status == "True" and j.status.conditions[-1].type == "Ready":
                    if "nvidia.com/gpu" in j.status.capacity.keys():
                        gpu = int(j.status.capacity["nvidia.com/gpu"])
                        memory = str(j.status.capacity['memory'])[:-2]
                        memory = float(int(memory)/1024/1024)
                        cpu = int(j.status.capacity["cpu"])
                        if cpu >= cpu_request and gpu >= gpu_request and memory >= memory_request:
                            useNodeName.append(j.metadata.name) 
    print(useNodeName)        
    return useNodeName

# get pods that need be scheduled
def getScheduledPod(k8sCoreV1api,namespace):
    podInstance = k8sCoreV1api.list_namespaced_pod(namespace)
    # get pods which status are Pending
    scheduledPodName = []
    for i in podInstance.items:
        if i.status.phase == 'Pending' and i.spec.node_name is None and i.spec.scheduler_name is not None:
            pod_requests = i.spec.containers[0].resources.requests
            print(pod_requests)
            if "nvidia.com/gpu" in pod_requests.keys():
                gpu_request = pod_requests["nvidia.com/gpu"]
 
            if i.spec.scheduler_name == scheduler_name:
                scheduledPodName.append(i.metadata.name)
    print(scheduledPodName)
    return scheduledPodName

# binding pod on some node
def podBinding(k8sCoreV1api, podName, nodeName, namespace):
    target = client.V1ObjectReference()
    target.kind = "Node"
    target.api_version = "v1"
    target.name = nodeName

    meta = client.V1ObjectMeta()
    meta.name = podName
    body = client.V1Binding(target=target)
    body.target = target
    body.metadata = meta
    # 
    try:
        k8sCoreV1api.create_namespaced_binding(namespace, body)
        return True
    except Exception as e:
        """
        Notice!
        create_namespaced_binding() throws exception:
        Invalid value for `target`, must not be `None`
        or
        despite the fact this exception is being thrown,
        Pod is bound to a Node and Pod is running
        """
        print('exception' + str(e))
        return False


# schedule the pod on specific node
def podScheduling(k8sCoreV1api,useNodeName,scheduledPodName,namespace):
    print(useNodeName)
    nodeName = np.random.choice(useNodeName)
    print("select node: ", nodeName)
    for podNAame in scheduledPodName:
        print("start schedule...")
        re = podBinding(k8sCoreV1api, podNAame, nodeName, namespace)
        print("finish scheduling this pod!")

if __name__ == '__main__':                                                         
    namespace = "gpu-share"
    global scheduler_name
    scheduler_name = "gpu_scheduler"
    # 1、加载配置文件 这里的配置文件是集群中的.kube/config 文件，直接去集群中粘贴过来即可
    k8s.config.load_kube_config(config_file="/home/wychen/gpu_share/admin.conf")
    k8sCoreV1api = client.CoreV1Api()
    useNodeName = getUseNode(k8sCoreV1api, namespace)
    scheduledPodName = getScheduledPod(k8sCoreV1api,namespace)
    podScheduling(k8sCoreV1api,useNodeName,scheduledPodName,namespace)
