from kubernetes import config, client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import sys
import time
import logging

def setup_kubernetes(kubeconfig_path):
    """
    Sets up the Kubernetes client
    """

    if kubeconfig_path is None:
        kubeconfig_path = config.KUBE_CONFIG_DEFAULT_LOCATION
    kubeconfig = config.kube_config.KubeConfigMerger(kubeconfig_path)

    if kubeconfig.config is None:
        raise Exception(
            "Invalid kube-config file: %s. " "No configuration found." % kubeconfig_path
        )
    loader = config.kube_config.KubeConfigLoader(
        config_dict=kubeconfig.config,
    )
    client_config = client.Configuration()
    loader.load_and_set(client_config)
    #cli = client.ApiClient(configuration=client_config)
    cli = client.CoreV1Api()
    batch_cli = client.BatchV1Api()
    return cli, batch_cli

def create_job(batch_cli, body, namespace="default"):
    try:
        api_response = batch_cli.create_namespaced_job(body=body, namespace=namespace)
        return api_response
    except ApiException as api:
        logging.warn(
            "Exception when calling \
                       BatchV1Api->create_job: %s"
            % api
        )
        if api.status == 409:
            logging.warn("Job already present")
    except Exception as e:
        logging.error(
            "Exception when calling \
                       BatchV1Api->create_namespaced_job: %s"
            % e
        )
        raise

def delete_pod(cli, name, namespace):
    try:
        print(type(cli))
        cli.delete_namespaced_pod(name=name, namespace=namespace)
        while cli.read_namespaced_pod(name=name, namespace=namespace):
            time.sleep(1)
    except ApiException as e:
        if e.status == 404:
            logging.info("Pod already deleted")
        else:
            logging.error("Failed to delete pod %s" % e)
            raise e

def create_pod(cli, body, namespace, timeout=120):
    try:
        pod_stat = None
        pod_stat = cli.create_namespaced_pod(body=body, namespace=namespace)
        end_time = time.time() + timeout
        while True:
            pod_stat = cli.read_namespaced_pod(name=body["metadata"]["name"], namespace=namespace)
            if pod_stat.status.phase == "Running":
                break
            if time.time() > end_time:
                raise Exception("Starting pod failed")
            time.sleep(1)
    except Exception as e:
        logging.error("Pod creation failed %s" % e)
        if pod_stat:
            logging.error(pod_stat.status.container_statuses)
        delete_pod(cli, body["metadata"]["name"], namespace)
        sys.exit(1)

def exec_cmd_in_pod(cli, command, pod_name, namespace, container=None, base_command="bash"):
    exec_command = [base_command, "-c", command]
    try:
        if container:
            ret = stream(
                cli.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                container=container,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        else:
            ret = stream(
                cli.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
    except Exception:
        return False
    return ret

def list_pods(cli, namespace, label_selector=None):
    pods = []
    try:
        if label_selector:
            ret = cli.list_namespaced_pod(namespace, pretty=True, label_selector=label_selector)
        else:
            ret = cli.list_namespaced_pod(namespace, pretty=True)
    except ApiException as e:
        logging.error(
            "Exception when calling \
                       CoreV1Api->list_namespaced_pod: %s\n"
            % e
        )
        raise e
    for pod in ret.items:
        pods.append(pod.metadata.name)
    return pods

def get_job_status(batch_cli, name, namespace="default"):
    try:
        return batch_cli.read_namespaced_job_status(name=name, namespace=namespace)
    except Exception as e:
        logging.error(
            "Exception when calling \
                       BatchV1Api->read_namespaced_job_status: %s"
            % e
        )
        raise

def get_pod_log(cli, name, namespace="default"):
    return cli.read_namespaced_pod_log(
        name=name, namespace=namespace, _return_http_data_only=True, _preload_content=False
    )

def read_pod(cli, name, namespace="default"):
    return cli.read_namespaced_pod(name=name, namespace=namespace)


def delete_job(batch_cli, name, namespace="default"):
    try:
        api_response = batch_cli.delete_namespaced_job(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=0),
        )
        logging.debug("Job deleted. status='%s'" % str(api_response.status))
        return api_response
    except ApiException as api:
        logging.warn(
            "Exception when calling \
                       BatchV1Api->create_namespaced_job: %s"
            % api
        )
        logging.warn("Job already deleted\n")
    except Exception as e:
        logging.error(
            "Exception when calling \
                       BatchV1Api->delete_namespaced_job: %s\n"
            % e
        )
        sys.exit(1)