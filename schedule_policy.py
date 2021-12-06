# coding:utf-8
from nodeInfo import NodeInfo
from interference import Interference
from logging import basicConfig, getLogger, INFO
from kubernetes.client.rest import ApiException, RESTClientObject
from kubernetes import client, config, watch
from json import loads as json_loads
import kubernetes as k8s
import numpy as np
from configs import Configs
Configs = Configs()

formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
            " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
basicConfig(level=INFO, format=formatter)
logger = getLogger("gpu_scheduler")


# get avaliable nodes
def getUseNode(k8sCoreV1api, namespace):
    # podInstance = k8sCoreV1api.list_namespaced_pod(namespace)
    podInstance = getScheduledPod(k8sCoreV1api, namespace)
    # print(podInstance)
    useNodeName = []
    newPodInstance = []

    # 1. if there are free node, return these nodes to schedule
    # nodeInstance = k8sCoreV1api.list_node(label_selector="gpushare/has-processes=false")
    # if nodeInstance is not None:
    #     for item in nodeInstance.items:
    #         useNodeName.append(item.metadata.name)
    #     return useNodeName

    # 2. select nodes with one free GPUs
    # 3. select nodes with the minimum interference
    nodeInstance = k8sCoreV1api.list_node(label_selector="gpushare=true")
    for i in podInstance:
        if i.status.phase == 'Pending' and i.spec.node_name is None and i.spec.scheduler_name is not None:
            print("wychen")
            pod_requests = i.spec.containers[0].resources.requests
            job_name = i.metadata.name

            gpu_request, gpu_mem_request = 0, 0
            # print(pod_requests)
            if "gpushare/gpu-count" in pod_requests.keys():
                gpu_request = int(pod_requests["gpushare/gpu-count"])
            if "gpushare/gpu-mem" in pod_requests.keys():
                gpu_mem_request = int(pod_requests["gpushare/gpu-mem"])
            if "memory" in pod_requests.keys():
                memory_request = int(pod_requests["memory"][:-1])
            if "cpu" in pod_requests.keys():
                cpu_request = int(pod_requests["cpu"])
            for j in nodeInstance.items:
                # print(j.metadata.annotations)
                nodeInfo = NodeInfo(j, 0)
                # print(nodeInfo)
                if j.status.conditions[-1].status == "True" and j.status.conditions[-1].type == "Ready":
                    envs = client.V1EnvVar(
                        name='CUDA_VISIBLE_DEVICES', value="1")
                    # print(i.spec.containers[0])
                    i.spec.containers[0].env = envs
                        # memory = str(j.status.capacity['memory'])[:-2]
                        # memory = float(int(memory)/1024/1024)
                        # cpu = int(j.status.capacity["cpu"])
                        # gpu_count = nodeInfo.gpuCount
                        # print(nodeInfo.processes)
                        # print(nodeInfo.memoryCapacity)
                        # print(nodeInfo.memoryUsed)
                        # print(nodeInfo.gpuUtil)
                        # print(nodeInfo.memUtil)
                        # # print(nodeInfo.getDeviceInfo(j, 0))
                        # if gpu_count > 0:
                        #     for k in range(gpu_count):
                        #        deviceInfo = nodeInfo.getDeviceInfo(j, k)
                        #        if deviceInfo.gpu_util < Configs.GPU_UTIL_THRESHOLD and deviceInfo.mem_util < Configs.MEM_UTIL_THRESHOLD and cpu >= cpu_request and memory >= memory_request and gpu_mem_request < deviceInfo.mem_capacity - deviceInfo.mem_used:
                        #             process = deviceInfo.processes
                        #             interference_score = Interference.getInterferenceScore(process, job_name)
                        #             if interference_score < Configs.INTERFERENCE_THRESHOLD:
                        #                 print(j.metadata.name)
                        #                 print(k)
                        #                 i.spec.containers[0].env['CUDA_VISIBLE_DEVICES'] = k
                        #                 useNodeName.append(j.metadata.name)
                    useNodeName.append(j.metadata.name)
            # newPodInstance(i)
    print(useNodeName)
    return useNodeName

# get pods that need be scheduled


def getScheduledPod(k8sCoreV1api, namespace):
    podInstance = k8sCoreV1api.list_namespaced_pod(namespace)
    # get pods which status are Pending
    scheduledPods = []
    for i in podInstance.items:
        if i.status.phase == 'Pending' and i.spec.node_name is None and i.spec.scheduler_name == scheduler_name:
            scheduledPods.append(i)
    print(len(scheduledPods))
    print(scheduledPods)
    return scheduledPods

# binding pod on some node


def podBinding(k8sCoreV1api, pod, nodeName, namespace):
    print(nodeName)
    target = client.V1ObjectReference()
    target.kind = "Node"
    target.api_version = "v1"
    target.name = nodeName
    # print(target)

    meta = client.V1ObjectMeta()
    meta.name = pod.metadata.name
    body = client.V1Binding(target=target)
    body.target = target
    body.metadata = meta
    body.spec = pod.spec

    #
    try:
        logger.info("Binding Pod: %s  to  Node: %s",
                    pod.metadata.name, nodeName)
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
        print('exception: ' + str(e))
        return False


# schedule the pod on specific node
def podScheduling(k8sCoreV1api, useNodeName, scheduledPods, namespace):
    print(useNodeName)
    nodeName = np.random.choice(useNodeName)
    # nodeName = useNodeName
    print("select node: ", nodeName)
    for pod in scheduledPods:
        envs = client.V1EnvVar(name='CUDA_VISIBLE_DEVICES', value="1")
        # print(i.spec.containers[0])
        pod.spec.containers[0].env = envs
        
        print("start schedule..." + str(pod.metadata.name))
        re = podBinding(k8sCoreV1api, pod, nodeName, namespace)
        if re:
            print("schedule success!")
        print("finish scheduling this pod!")


def watchPodEvents(k8sCoreV1api, namespace):
    while True:
        try:
            logger.info("Check pod event...")
            try:
                watcher = watch.Watch()
                for event in watcher.stream(k8sCoreV1api.list_namespaced_pod, namespace=namespace, timeout_seconds=20):
                    logger.info(f"Event: {event['type']} {event['object'].kind}, {event['object'].metadata.namespace}, {event['object'].metadata.name}, {event['object'].status.phase}")
                    if event["object"].status.phase == "Pending":
                        try:
                            logger.info(f'{event["object"].metadata.name} needs scheduling...')
                            pod_name = event["object"].metadata.name
                            logger.info("Processing for Pod: %s/%s",
                                        namespace, pod_name)
                            node_name = getUseNode(k8sCoreV1api, namespace)
                            if node_name:
                                logger.info("Namespace %s, PodName %s , Node Name: %s",
                                            namespace, pod_name, node_name)
                                scheduledPods = getScheduledPod(k8sCoreV1api, namespace)            
                                res = podScheduling(
                                    k8sCoreV1api, node_name, scheduledPods, namespace)
                                logger.info("Response %s ", res)
                            else:
                                logger.error(f"Found no valid node to schedule {pod_name} in {namespace}")
                        except ApiException as e:
                            logger.error(json_loads(e.body)["message"])
                        except ValueError as e:
                            logger.error("Value Error %s", e)
                        except:
                            logger.exception("Ignoring Exception")
                logger.info("Resetting k8s watcher...")
            except:
                logger.exception("Ignoring Exception")
            finally:
                del watcher
        except:
            logger.exception("Ignoring Exception & listening for pod events")

if __name__ == '__main__':                                                         
    namespace = "gpu-share"
    scheduler_name = "gpu_scheduler"
    # 加载配置文件 这里的配置文件是集群中的.kube/config 文件，直接去集群中粘贴过来即可
    k8s.config.load_kube_config(config_file="./admin.conf")
    k8sCoreV1api = client.CoreV1Api()
    logger.info("Initializing gpushare-scheduler...")
    logger.info("Watching for pod events...")
    # config.load_incluster_config()
    watchPodEvents(k8sCoreV1api, namespace)
