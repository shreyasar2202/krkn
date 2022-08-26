from code import interact
from dataclasses import dataclass, field
import yaml
import logging
import time
import sys
import os
import random
from traceback import format_exc
from jinja2 import Environment, FileSystemLoader
#import kraken.cerberus.setup as cerberus
from kraken.plugins.network import kubernetes_functions as kube_helper
import kraken.node_actions.common_node_functions as common_node_functions
import typing
from arcaflow_plugin_sdk import validation, plugin


@dataclass
class NetworkScenarioConfig:
    node_interface_name: typing.Annotated[typing.Dict[str, typing.List[str]], validation.required_if_not("label_selector")] = field(
        default=None,
        metadata={
            "name": "Name",
            "description": "Name(s) for target nodes. Required if label_selector is not set.",
        },
    )

    # node_interface_name: typing.Annotated[
    #     typing.Optional[typing.Dict[str, typing.List[str]]],
    #     validation.required_if_not("label_selector"),
    # ] = field(
    #     default=None,
    #     metadata={
    #         "name": "Name",
    #         "description": "Name(s) for target nodes. Required if label_selector is not set.",
    #     },
    # )
    # node_interface_name: typing.Annotated[
    #     typing.Optional[str],
    #     validation.required_if_not("label_selector"),
    # ] = field(
    #     default=None,
    #     metadata={
    #         "name": "Name",
    #         "description": "Name(s) for target nodes. Required if label_selector is not set.",
    #     },
    # )

    label_selector: typing.Annotated[
        typing.Optional[str], validation.min(1), validation.required_if_not("node_interface_name")
    ] = field(
        default=None,
        metadata={
            "name": "Label selector",
            "description": "Kubernetes label selector for the target nodes. Required if name is not set.\n"
            "See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ for details.",
        },
    )

    test_duration: typing.Annotated[typing.Optional[int], validation.min(1)] = field(
        default=300,
        metadata={
            "name": "Timeout",
            "description": "Timeout to wait for the target pod(s) to be removed in seconds.",
        },
    )

    wait_duration: typing.Annotated[typing.Optional[int], validation.min(1)] = field(
        default=300,
        metadata={
            "name": "Timeout",
            "description": "Timeout to wait for the target pod(s) to be removed in seconds.",
        },
    )

    instance_count: typing.Annotated[typing.Optional[int], validation.min(1)] = field(
        default=1,
        metadata={
            "name": "Instance Count",
            "description": "Number of nodes to perform action/select that match the label selector.",
        },
    )


    kubeconfig_path: typing.Optional[str] = field(
        default=None,
        metadata={
            "name": "Kubeconfig path",
            "description": "Path to your Kubeconfig file. Defaults to ~/.kube/config.\n"
            "See https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/ for "
            "details.",
        },
    )

    execution_type: typing.Optional[str] = field(
        default='serial',
        metadata={
            "name": "Execution Type",
            "description": "The order in which the ingress filters are applied .Execution type can be 'serial' or 'parallel'"  
        }
    )

    network_params : typing.Dict[str,str] = field(
        default=None,
        metadata={
            "name": "Network Parameters",
            "description": "Name(s) for target nodes. Required if label_selector is not set.",
        },
    )
    

@dataclass
class NetworkScenarioSuccessOutput:
    traffic_direction: str
    #node_interfaces : typing.Dict[str, typing.List[str]]
    network_paramaters: typing.Dict[str, str]
    execution_type: str

@dataclass
class NetworkScenarioErrorOutput:
    error: str
    

def get_random_interface(node, pod_template):

    pod_body = yaml.safe_load(pod_template.render(nodename=node))
    logging.info("Creating pod to query interface on node %s" % node)
    kube_helper.create_pod(pod_body, "default", 300)
    try:
        cmd = "ip r | grep default | awk '/default/ {print $5}'"
        output = kube_helper.exec_cmd_in_pod(cmd, "fedtools", "default")
        interfaces = [output.replace("\n", "")]
        random_interface = random.choice(interfaces)        
    finally:
        logging.info("Deleteing pod to query interface on node")
        kube_helper.delete_pod("fedtools", "default")
    return random_interface

def verify_interface(input_interface_list, node, template, cli):
    
    pod_body = yaml.safe_load(template.render(nodename=node))
    logging.info("Creating pod to query interface on node %s" % node)
    kube_helper.create_pod(cli, pod_body, "default", 300)
    try:
        if input_interface_list == []:
            cmd = "ip r | grep default | awk '/default/ {print $5}'"
            output = kube_helper.exec_cmd_in_pod(cmd, "fedtools", "default")
            input_interface_list = [output.replace("\n", "")]
        else:
            cmd = "ip -br addr show|awk -v ORS=',' '{print $1}'"
            output = kube_helper.exec_cmd_in_pod(cli, cmd, "fedtools", "default")
            node_interface_list = output[:-1].split(",")
            
            for interface in input_interface_list:
                if interface not in node_interface_list:
                    logging.error("Interface %s not found in node %s interface list %s" % (interface, node, input_interface_list))
                    sys.exit(1)
    finally:
        logging.info("Deleteing pod to query interface on node")
        kube_helper.delete_pod(cli, "fedtools", "default")
    return input_interface_list

def get_node_interfaces(node_interface_dict, label_selector, instance_count, pod_template, cli):    

    if not node_interface_dict:
        if not label_selector:
            raise Exception("If node names and interfaces aren't provided, then the label selector must be provided")
        nodes = common_node_functions.get_node(None, label_selector, instance_count)
        
        for node in nodes:
            node_interface_dict[node] = [get_random_interface(node, pod_template)]
    else:
        node_name_list = node_interface_dict.keys()
        filtered_node_list = []        
        #for node in node_name_list:
        #    filtered_node_list.extend(common_node_functions.get_node(node, label_selector, instance_count))
        filtered_node_list = ['kind-control-plane']
        for node in filtered_node_list:
            node_interface_dict[node] = verify_interface(node_interface_dict[node], node, pod_template, cli)

    return node_interface_dict

def apply_ingress_filter(cfg, interface_list, node, job_template, batch_cli, param_selector='all'):
    
    network_params = cfg.network_params
    if param_selector != 'all':
        network_params = {param_selector: cfg.network_params[param_selector]}

    exec_cmd = get_ingress_cmd(
                interface_list, network_params, duration=cfg.test_duration
                )
            
    logging.info("Executing %s on node %s" % (exec_cmd, node))
    job_body = yaml.safe_load(
                    job_template.render(jobname=str(hash(node))[:5], nodename=node, cmd=exec_cmd)
                )
    api_response = kube_helper.create_job(batch_cli, job_body)
    if api_response is None:
        raise Exception("Error creating job")
    return job_body["metadata"]["name"]


@plugin.step(
    id="network_chaos",
    name="Network Ingress",
    description="Applies filters to ihe ingress side of node(s) interfaces",
    outputs={"success": NetworkScenarioSuccessOutput, "error": NetworkScenarioErrorOutput},
)
def network_chaos(cfg: NetworkScenarioConfig,
) -> typing.Tuple[str, typing.Union[NetworkScenarioSuccessOutput, NetworkScenarioErrorOutput]
]:
    
    cfg.node_interface_name={"kind-control-plane" : ["eth0@veth966e9072"]}
    file_loader = FileSystemLoader(os.path.abspath(os.path.dirname(__file__)))
    env = Environment(loader=file_loader)
    job_template = env.get_template("job.j2")
    pod_template = env.get_template("pod.j2")
    cli, batch_cli = kube_helper.setup_kubernetes(cfg.kubeconfig_path)
    node_interface_dict = get_node_interfaces(cfg.node_interface_name, cfg.label_selector, cfg.instance_count, pod_template, cli)
    
    job_list = []
    try:
        
        if cfg.execution_type == 'parallel':
            for node in node_interface_dict:
                job_list.append(apply_ingress_filter(cfg, node_interface_dict[node], node, job_template, batch_cli))
            job_list
            logging.info("Waiting for parallel job to finish")
            wait_for_job(job_list[:], cfg.test_duration + 300)
            logging.info("Waiting for wait_duration %s" % cfg.wait_duration)
            time.sleep(cfg.wait_duration)
            #cerberus.publish_kraken_status(config, failed_post_scenarios, start_time, end_time)
            logging.info("Deleting jobs")
            delete_job(cli, batch_cli, job_list[:])
            
        elif cfg.execution_type == 'serial':
            for param in cfg.network_params:
                for node in node_interface_dict:
                    job_list.append(apply_ingress_filter(cfg, node_interface_dict[node], node, job_template, batch_cli, param_selector=param))
                logging.info("Waiting for serial job to finish")
                wait_for_job(job_list[:], cfg.test_duration + 300)
                logging.info("Waiting for wait_duration %s" % cfg.wait_duration)
                time.sleep(cfg.wait_duration)
                #cerberus.publish_kraken_status(config, failed_post_scenarios, start_time, end_time)
                logging.info("Deleting jobs")
                delete_job(cli, batch_cli, job_list[:])
                job_list = []
        else:
            
            return "error", NetworkScenarioErrorOutput(
                    "Invalid execution type - serial and parallel are the only accepted types"
                )
        return "success", NetworkScenarioSuccessOutput(
        direction = "ingress",
        #node_interfaces = node_interface_dict,
        network_paramaters= cfg.network_params,
        execution_type = cfg.execution_type
        )
    except Exception as e:
        logging.error("Network Chaos exiting due to Exception %s" % e)
        return "error", NetworkScenarioErrorOutput(
                    format_exc()
                )
    


def get_job_pods(cli, api_response):
    controllerUid = api_response.metadata.labels["controller-uid"]
    pod_label_selector = "controller-uid=" + controllerUid
    pods_list = kube_helper.list_pods(cli, label_selector=pod_label_selector, namespace="default")
    return pods_list[0]


def wait_for_job(batch_cli, joblst, timeout=300):
    waittime = time.time() + timeout
    count = 0
    joblen = len(joblst)
    while count != joblen:
        for jobname in joblst:
            try:
                api_response = kube_helper.get_job_status(batch_cli, jobname, namespace="default")
                if api_response.status.succeeded is not None or api_response.status.failed is not None:
                    count += 1
                    joblst.remove(jobname)
            except Exception:
                logging.warn("Exception in getting job status")
            if time.time() > waittime:
                raise Exception("Starting pod failed")
            time.sleep(5)


def delete_job(cli, batch_cli, joblst):
    for jobname in joblst:
        try:
            api_response = kube_helper.get_job_status(batch_cli, jobname, namespace="default")
            if api_response.status.failed is not None:
                pod_name = get_job_pods(cli, api_response)
                pod_stat = kube_helper.read_pod(cli, name=pod_name, namespace="default")
                logging.error(pod_stat.status.container_statuses)
                pod_log_response = kube_helper.get_pod_log(cli, name=pod_name, namespace="default")
                pod_log = pod_log_response.data.decode("utf-8")
                logging.error(pod_log)
        except Exception:
            logging.warn("Exception in getting job status")
        api_response = kube_helper.delete_job(batch_cli, name=jobname, namespace="default")



def get_ingress_cmd(interface_list, network_parameters, duration=30):
    tc_set = tc_unset = tc_ls = ""
    param_map = {"latency": "delay", "loss": "loss", "bandwidth": "rate"}
    tc_set += "modprobe ifb numifbs={};".format(len(interface_list))

    for i, interface in enumerate(interface_list):
        ifb_name = "ifb{0}".format(i)
        tc_set += "ip link set dev {0} up;".format(ifb_name)
        tc_set += "tc qdisc add dev {0} handle ffff: ingress;".format(interface)
        tc_set += "tc filter add dev {0}parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev {1};".format(interface, ifb_name)
        tc_set = "{0} tc qdisc add dev {1} root netem".format(tc_set, ifb_name)
        tc_unset = "{0} tc qdisc del dev {1} root ;".format(tc_unset, ifb_name)
        tc_unset += "tc qdisc del dev {0} handle ffff: ingress;".format(interface)
        tc_unset += "ip link set dev {0} down;".format(ifb_name)
        tc_ls = "{0} tc qdisc ls dev {1} ;".format(tc_ls, ifb_name)
        for parameter in network_parameters.keys():
            tc_set += " {0} {1} ".format(param_map[parameter], network_parameters[parameter])
        tc_set += ";"
    exec_cmd = "{0} {1} sleep {2};{3} sleep 20;{4}".format(tc_set, tc_ls, duration, tc_unset, tc_ls)
    return exec_cmd