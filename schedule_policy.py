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
from prometheus_api_client import PrometheusConnect,MetricRangeDataFrame, metric_range_df
from prometheus_api_client.utils import parse_datetime
from datetime import timedelta
import pandas as pd

Configs = Configs()

formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
            " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
basicConfig(level=INFO, format=formatter)
query_metrics = [
    'cgroup_monitor_monitored_cpu_psi', 'cgroup_monitor_monitored_io_psi',
    'cgroup_monitor_monitored_memory_psi'
]
logger = getLogger("gpu_scheduler")

# init prometheus api
prom_client = PrometheusConnect(url="172.169.8.5:9090")


# get prometheus metrics
def GetMetrics(query_metrics=query_metrics,chunck_size="10s", query_range="1d"):
    start_time = parse_datetime(query_range)
    end_time = parse_datetime("now")
    chunk_size = timedelta(seconds=chunck_size)
    result = pd.DataFrame()
    for metric in query_metrics:
        metric_data = prom_client.get_metric_range_data(
            metric,  # this is the metric name and label config
            start_time=start_time,
            end_time=end_time,
            chunk_size=chunk_size,
        )
        metric_data_df = MetricRangeDataFrame(metric_data).reset_index()
        result.append(metric_range_df)
    return result


'''
kernel scheduling function.
1. first stage: get all scheduled pods and deploy them to the node with minimum average 60s pod some cpu psi value last day.
'''
def getUseNode(k8sCoreV1api, namespace):
    podInstance = getScheduledPod(k8sCoreV1api, namespace)
    # print(podInstance)
    useNodeName = []
    newPodInstance = []
    node_psi_metrics = GetMetrics() 
    node_avg_metrics = node_psi_metrics[(node_psi_metrics['type']=='some')&(node_psi_metrics['window']=='60s')].groupby(['instance']).agg({'value':np.mean, 'timestamp':np.max}).reset_index()[['instance','value']]
    node_avg_metrics.sort_values(by='value', ascending=False, inplace=True)
    nodeInstance = k8sCoreV1api.list_node()
    for i in podInstance:
        if i.status.phase == 'Pending' and i.spec.node_name is None and i.spec.scheduler_name is not None:
            for j in nodeInstance.items:
                
    print(useNodeName)
    return useNodeName


# get node available cpu and memory
def getFreeResource(k8sCoreV1api, nodeName):
    nodeInfo = k8sCoreV1api.read_node(nodeName)
    nodeInfo = NodeInfo(nodeInfo)
    return nodeInfo.getFreeResource()


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
        logger.info("Binding Pod: %s  to  Node: %s", pod.metadata.name,
                    nodeName)
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
                for event in watcher.stream(k8sCoreV1api.list_namespaced_pod,
                                            namespace=namespace,
                                            timeout_seconds=20):
                    logger.info(
                        f"Event: {event['type']} {event['object'].kind}, {event['object'].metadata.namespace}, {event['object'].metadata.name}, {event['object'].status.phase}"
                    )
                    if event["object"].status.phase == "Pending":
                        try:
                            logger.info(
                                f'{event["object"].metadata.name} needs scheduling...'
                            )
                            pod_name = event["object"].metadata.name
                            logger.info("Processing for Pod: %s/%s", namespace,
                                        pod_name)
                            node_name = getUseNode(k8sCoreV1api, namespace)
                            if node_name:
                                logger.info(
                                    "Namespace %s, PodName %s , Node Name: %s",
                                    namespace, pod_name, node_name)
                                scheduledPods = getScheduledPod(
                                    k8sCoreV1api, namespace)
                                res = podScheduling(k8sCoreV1api, node_name,
                                                    scheduledPods, namespace)
                                logger.info("Response %s ", res)
                            else:
                                logger.error(
                                    f"Found no valid node to schedule {pod_name} in {namespace}"
                                )
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
