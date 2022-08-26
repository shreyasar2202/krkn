import yaml
import logging
import time
import sys
import os
import random
from jinja2 import Environment, FileSystemLoader
import kraken.cerberus.setup as cerberus
import kraken.kubernetes.client as kubecli
import kraken.node_actions.common_node_functions as common_node_functions


# Reads the scenario config and introduces traffic variations in Node's host network interface.
def run(scenarios_list, config, wait_duration):
    import pdb;pdb.set_trace()
    failed_post_scenarios = ""
    logging.info("Runing the Network Chaos tests")
    for net_config in scenarios_list:
        with open(net_config, "r") as file:
            param_lst = ["latency", "loss", "bandwidth"]
            test_config = yaml.safe_load(file)
            test_dict = test_config["network_chaos"]
            test_duration = int(test_dict.get("duration", 300))
            test_interface = test_dict.get("interfaces", [])
            test_node = test_dict.get("node_name", "")
            test_node_label = test_dict.get("label_selector", "node-role.kubernetes.io/master")
            test_execution = test_dict.get("execution", "serial")
            test_instance_count = test_dict.get("instance_count", 1)
            test_egress = test_dict.get("egress", {"bandwidth": "100mbit"})
            if test_node:
                node_name_list = test_node.split(",")
            else:
                node_name_list = [test_node]
            nodelst = []
            for single_node_name in node_name_list:
                nodelst.extend(common_node_functions.get_node(single_node_name, test_node_label, test_instance_count))
            file_loader = FileSystemLoader(os.path.abspath(os.path.dirname(__file__)))
            env = Environment(loader=file_loader)
            pod_template = env.get_template("pod.j2")
            test_interface = verify_interface(test_interface, nodelst, pod_template)
            joblst = []
            egress_lst = [i for i in param_lst if i in test_egress]
            chaos_config = {
                "network_chaos": {
                    "duration": test_duration,
                    "interfaces": test_interface,
                    "node_name": ",".join(nodelst),
                    "execution": test_execution,
                    "instance_count": test_instance_count,
                    "egress": test_egress,
                }
            }
            logging.info("Executing network chaos with config \n %s" % yaml.dump(chaos_config))
            job_template = env.get_template("job.j2")
            try:
                for i in egress_lst:
                    for node in nodelst:
                        exec_cmd = get_egress_cmd(
                            test_execution, test_interface, i, test_dict["egress"], duration=test_duration
                        )
                        logging.info("Executing %s on node %s" % (exec_cmd, node))
                        job_body = yaml.safe_load(
                            job_template.render(jobname=i + str(hash(node))[:5], nodename=node, cmd=exec_cmd)
                        )
                        joblst.append(job_body["metadata"]["name"])
                        api_response = kubecli.create_job(job_body)
                        if api_response is None:
                            raise Exception("Error creating job")
                    if test_execution == "serial":
                        logging.info("Waiting for serial job to finish")
                        start_time = int(time.time())
                        wait_for_job(joblst[:], test_duration + 300)
                        logging.info("Waiting for wait_duration %s" % wait_duration)
                        time.sleep(wait_duration)
                        end_time = int(time.time())
                        cerberus.publish_kraken_status(config, failed_post_scenarios, start_time, end_time)
                    if test_execution == "parallel":
                        break
                if test_execution == "parallel":
                    logging.info("Waiting for parallel job to finish")
                    start_time = int(time.time())
                    wait_for_job(joblst[:], test_duration + 300)
                    logging.info("Waiting for wait_duration %s" % wait_duration)
                    time.sleep(wait_duration)
                    end_time = int(time.time())
                    cerberus.publish_kraken_status(config, failed_post_scenarios, start_time, end_time)
            except Exception as e:
                logging.error("Network Chaos exiting due to Exception %s" % e)
                sys.exit(1)
            finally:
                logging.info("Deleting jobs")
                delete_job(joblst[:])


def verify_interface(test_interface, nodelst, template):
    pod_index = random.randint(0, len(nodelst) - 1)
    pod_body = yaml.safe_load(template.render(nodename=nodelst[pod_index]))
    logging.info("Creating pod to query interface on node %s" % nodelst[pod_index])
    kubecli.create_pod(pod_body, "default", 300)
    try:
        if test_interface == []:
            cmd = "ip r | grep default | awk '/default/ {print $5}'"
            output = kubecli.exec_cmd_in_pod(cmd, "fedtools", "default")
            test_interface = [output.replace("\n", "")]
        else:
            cmd = "ip -br addr show|awk -v ORS=',' '{print $1}'"
            output = kubecli.exec_cmd_in_pod(cmd, "fedtools", "default")
            interface_lst = output[:-1].split(",")
            for interface in test_interface:
                if interface not in interface_lst:
                    logging.error("Interface %s not found in node %s interface list %s" % (interface, nodelst[pod_index], interface_lst))
                    sys.exit(1)
        return test_interface
    finally:
        logging.info("Deleteing pod to query interface on node")
        kubecli.delete_pod("fedtools", "default")


def get_job_pods(api_response):
    controllerUid = api_response.metadata.labels["controller-uid"]
    pod_label_selector = "controller-uid=" + controllerUid
    pods_list = kubecli.list_pods(label_selector=pod_label_selector, namespace="default")
    return pods_list[0]


def wait_for_job(joblst, timeout=300):
    waittime = time.time() + timeout
    count = 0
    joblen = len(joblst)
    while count != joblen:
        for jobname in joblst:
            try:
                api_response = kubecli.get_job_status(jobname, namespace="default")
                if api_response.status.succeeded is not None or api_response.status.failed is not None:
                    count += 1
                    joblst.remove(jobname)
            except Exception:
                logging.warn("Exception in getting job status")
            if time.time() > waittime:
                raise Exception("Starting pod failed")
            time.sleep(5)


def delete_job(joblst):
    for jobname in joblst:
        try:
            api_response = kubecli.get_job_status(jobname, namespace="default")
            if api_response.status.failed is not None:
                pod_name = get_job_pods(api_response)
                pod_stat = kubecli.read_pod(name=pod_name, namespace="default")
                logging.error(pod_stat.status.container_statuses)
                pod_log_response = kubecli.get_pod_log(name=pod_name, namespace="default")
                pod_log = pod_log_response.data.decode("utf-8")
                logging.error(pod_log)
        except Exception:
            logging.warn("Exception in getting job status")
        api_response = kubecli.delete_job(name=jobname, namespace="default")


def get_egress_cmd(execution, test_interface, mod, vallst, duration=30):
    tc_set = tc_unset = tc_ls = ""
    param_map = {"latency": "delay", "loss": "loss", "bandwidth": "rate"}
    for i in test_interface:
        tc_set = "{0} tc qdisc add dev {1} root netem".format(tc_set, i)
        tc_unset = "{0} tc qdisc del dev {1} root ;".format(tc_unset, i)
        tc_ls = "{0} tc qdisc ls dev {1} ;".format(tc_ls, i)
        if execution == "parallel":
            for val in vallst.keys():
                tc_set += " {0} {1} ".format(param_map[val], vallst[val])
            tc_set += ";"
        else:
            tc_set += " {0} {1} ;".format(param_map[mod], vallst[mod])
    exec_cmd = "{0} {1} sleep {2};{3} sleep 20;{4}".format(tc_set, tc_ls, duration, tc_unset, tc_ls)
    return exec_cmd
