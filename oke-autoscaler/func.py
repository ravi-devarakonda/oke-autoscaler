import time
import pendulum
import oci
import os
import base64
import gzip
import shutil
import io
import json
import csv
import zipfile
import subprocess
from subprocess import Popen
from fdk import response
import logging

# general configuration..
logging.basicConfig(level=logging.INFO)

# functions..
def handler(ctx, data: io.BytesIO=None):
   """
   handler
   Handler function invoked by Oracle Functions service.
   """
   signer = oci.auth.signers.get_resource_principals_signer()
   resp = do(signer)
   return response.Response(ctx,
      response_data=json.dumps(resp),
      headers={"Content-Type": "application/json"})

def get_node_pool_details(ce_client, node_pool_id):
   """
   get_node_pool_details
   Get the details of the specified node pool.
   """
   response = ce_client.get_node_pool(node_pool_id)

   return response

def summarize_metrics_data(monitoring_client, compartment_id, namespace, query, query_start_time, query_end_time, query_resolution):
   """
   summarize_metrics_data
   Returns aggregated data that match the criteria specified in the request.
   """
   metric_data_details = oci.monitoring.models.SummarizeMetricsDataDetails(namespace = namespace,
                                                                           query = query,
                                                                           start_time = query_start_time,
                                                                           end_time = query_end_time,
                                                                           resolution = query_resolution,
                                                                          )
   response = monitoring_client.summarize_metrics_data(compartment_id=compartment_id, summarize_metrics_data_details=metric_data_details)

   return response

def update_node_pool(ce_client, node_pool_id, availability_domain, subnet_id, node_pool_new_size):
   """
   update_node_pool
   Add or remove nodes from the node pool.
   """
   placement_config_details = oci.container_engine.models.NodePoolPlacementConfigDetails(availability_domain=availability_domain, subnet_id=subnet_id)
   config_details = oci.container_engine.models.UpdateNodePoolNodeConfigDetails(size=node_pool_new_size, placement_configs=[placement_config_details])
   update_node_pool_details = oci.container_engine.models.UpdateNodePoolDetails(node_config_details=config_details)

   ce_composite_ops = oci.container_engine.ContainerEngineClientCompositeOperations(ce_client)
   response = ce_composite_ops.update_node_pool_and_wait_for_state(node_pool_id,
                                                                   update_node_pool_details,
                                                                   wait_for_states=[oci.container_engine.models.WorkRequest.STATUS_SUCCEEDED,
                                                                                    oci.container_engine.models.WorkRequest.STATUS_FAILED],
                                                                  )
   if response.data.status == oci.container_engine.models.WorkRequest.STATUS_FAILED:
      get_work_request_errors(ce_client, compartment_id, response.data.id)
   else:
      logging.info("Update node pool succeeded..")

   return

def evaluate_node(compute_client, instance_id):
   """
   evaluate_node
   Get details of compute instance.
   """
   response = compute_client.get_instance(instance_id=instance_id)

   return response

def get_secret_bundle(secrets_client, secret_id):
   """
   get_secret
   Gets a secret bundle from OCI Secrets that matches the specified secret id.
   """
   response = secrets_client.get_secret_bundle(secret_id=secret_id)

   return response

def get_kubeconfig(ce_client, cluster_id):
    """
    get_kubeconfig
    Retrieve the kubconfig file for a specified cluster id.
    """
    response = ce_client.create_kubeconfig(cluster_id)
    with open('/tmp/kubeconfig', 'w') as f:
        f.write(response.data.text)

    if response.data.text:
        logging.info("kubeconfig retrieved")
    else:
        logging.info("Error retrieving the kubeconfig")

    return

def get_unsched_pods(node_pool_name, secret):
   """
   get_unsched_pods
   Retrieve JSON data summarising unschedulable pods.
   """
   # congfigure kubectl for use with oke-autoscale service account:
   #   - add oke-autoscale service account as user definition..
   response = Popen(['kubectl config set-credentials oke-autoscaler --token=' + secret],
                    shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                   )
   output, errors = response.communicate()
   logging.info("Popen Output, kubectl config: " + output)
   logging.info("Popen Errors, kubectl config: " + errors)

   #   - set context..
   response = Popen(['kubectl config set-context --current --user=oke-autoscaler'],
                    shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                   )
   output, errors = response.communicate()
   logging.info("Popen Output, kubectl config: " + output)
   logging.info("Popen Errors, kubectl config: " + errors)

   # get pods..
   response = Popen(['kubectl get pods --all-namespaces -o json | jq \'.items[] | select(.status.conditions[0].reason == "Unschedulable")\' | jq \' select(.spec.nodeSelector.name == "' + node_pool_name + '")\' | jq -s .'],
                    shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                   )

   output, errors = response.communicate()
   logging.info("Popen Output, kubectl get pods: " + output)
   logging.info("Popen Errors, kubectl get pods: " + errors)

   return output

def drain_node(lifo_node_name, secret):
   """
   drain_node
   Cordon and drain the specified worker node.
   """
   # drain worker node..
   response = Popen(['kubectl drain ' + lifo_node_name + ' --delete-local-data --ignore-daemonsets --force --grace-period=10 --timeout=90s'],
                    shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                   )
   output, errors = response.communicate()
   logging.info("Popen Output, kubectl drain: " + output)
   logging.info("Popen Errors, kubectl drain: " + errors)

   return

# oke-autoscaler logic..
def do(signer):
   # configure environment:
   #   - external / user-defined variables..
   fn_var = 0
   if 'node_pool_eval_window' in os.environ:
      node_pool_eval_window = str(os.environ['node_pool_eval_window'])
   else: fn_var = 1
   if 'cluster_id' in os.environ:
      cluster_id = os.environ['cluster_id']
   else: fn_var = 1
   if 'node_pool_id' in os.environ:
      node_pool_id = os.environ['node_pool_id']
   else: fn_var = 1
   if 'secret_id' in os.environ:
      secret_id = os.environ['secret_id']
   else: fn_var = 1
   if 'node_pool_min_size' in os.environ:
      node_pool_min_size = int(os.environ['node_pool_min_size'])
   else: fn_var = 1
   if 'node_pool_max_size' in os.environ:
      node_pool_max_size = int(os.environ['node_pool_max_size'])
   else: fn_var = 1
   if 'node_pool_eval_cpu_load' in os.environ:
      node_pool_eval_cpu_load = int(os.environ['node_pool_eval_cpu_load'])
   else: fn_var = 1
   if 'node_pool_eval_ram_load' in os.environ:
      node_pool_eval_ram_load = int(os.environ['node_pool_eval_ram_load'])
   else: fn_var = 1

   #   - internal variables..
   time_now = pendulum.now("UTC")
   time_now_iso8601 = time_now.to_iso8601_string()
   time_then = time_now.subtract(minutes=int(node_pool_eval_window))
   time_then_iso8601 = time_then.to_iso8601_string()
   logging.info("Time Now: " + time_now_iso8601)
   logging.info("Time Then: " + time_then_iso8601)

   query_start_time = time_then_iso8601
   query_end_time = time_now_iso8601
   monitoring_resolution = node_pool_eval_window + "m"
   query_resolution = monitoring_resolution

   node_pool_status = "ready"
   node_pool_stability = None
   node_pool_stabilization_window = 3
   node_pool_expanding = 0
   node_pool_contract = 0
   node_pool_result_reason = None

   sum_cpu_load = 0
   sum_ram_load = 0

   lifo_node = None
   lifo_node_name = None

   # proceed if all external / user-defined variables defined..
   if fn_var == 0:
      # define api clients..
      ce_client = oci.container_engine.ContainerEngineClient({}, signer=signer)
      monitoring_client = oci.monitoring.MonitoringClient({}, signer=signer)
      compute_client = oci.core.ComputeClient({}, signer=signer)
      secrets_client = oci.secrets.SecretsClient({}, signer=signer)

      # obtain node pool detail from ce_client..
      get_node_pool = get_node_pool_details(ce_client, node_pool_id)
      node_pool_details = json.loads(str(get_node_pool.data))
      node_pool_name = (node_pool_details['initial_node_labels'][0]['value'])
      logging.info ("Node Pool Details: " + json.dumps(node_pool_details, indent=4))

      compartment_id = (node_pool_details['compartment_id'])
      node_pool_init_size = (node_pool_details['node_config_details']['size'])
      availability_domain = (node_pool_details['node_config_details']['placement_configs'][0]['availability_domain'])
      subnet_id = (node_pool_details['node_config_details']['placement_configs'][0]['subnet_id'])

      # set node_pool_status..
      logging.info("Node Lifecycle State: ")
      if get_node_pool.data.nodes != None:
         nodes = json.loads(str(get_node_pool.data.nodes))
         for i in range(len(nodes)):
            logging.info(nodes[i]['lifecycle_state'])
            if (nodes[i]['lifecycle_state']) not in ["ACTIVE", "DELETED"]:
               node_pool_status = "updating"

      # discover and inspect each node in node pool:
      #   - for each node, populate nodes_data dict with node attributes:
      #      - name, id, created, cpu_load, ram_load..
      nodes_data = {}
      if node_pool_init_size >0:
         if node_pool_status != "updating":
            if get_node_pool.data.nodes != None:
               nodes = json.loads(str(get_node_pool.data.nodes))
               for i in range(len(nodes)):
                  if (nodes[i]['lifecycle_state']) != "DELETED":
                     # get node details from ce and compute clients..
                     logging.info('Node Data: ' + json.dumps(nodes[i], indent = 4))
                     node_id = (nodes[i]['id'])
                     node_name = (nodes[i]['private_ip'])
                     instance_response = evaluate_node(compute_client, node_id)
                     instance = json.loads(str(instance_response.data))
                     time_created = (instance['time_created'])

                     # get node cpu utilisation data from monitoring service..
                     namespace = "oci_computeagent"
                     query = "CpuUtilization[" + monitoring_resolution + "]{resourceId=" + node_id + "}.mean()"
                     monitoring_response = summarize_metrics_data(monitoring_client, compartment_id, namespace, query, query_start_time, query_end_time, query_resolution)
                     # get the oci monitoring aggregated datapoint..
                     monitoring = json.loads(str(monitoring_response.data))
                     node_cpu_load = (monitoring[0]['aggregated_datapoints'])
                     node_cpu_load_val = (node_cpu_load[0]['value'])
                     # get node ram utilisation data from monitoring service..
                     namespace = "oci_computeagent"
                     query = "MemoryUtilization[" + monitoring_resolution + "]{resourceId=" + node_id + "}.mean()"
                     monitoring_response = summarize_metrics_data(monitoring_client, compartment_id, namespace, query, query_start_time, query_end_time, query_resolution)
                     # get the oci monitoring aggregated datapoint..
                     monitoring = json.loads(str(monitoring_response.data))
                     node_ram_load = (monitoring[0]['aggregated_datapoints'])
                     node_ram_load_val = (node_ram_load[0]['value'])

                     # insert node data into nodes_data dict..
                     nodes_data[i] = {'name': node_name, 'id': node_id, 'created': time_created, 'cpu_load':node_cpu_load_val , 'ram_load':node_ram_load_val}

                     # determine last node added to node pool..
                     lifo_node = max(nodes_data, key=lambda x: nodes_data[x].get('created'))
                     lifo_node_name = nodes_data[lifo_node].get('name')

                     # set node_pool_stability..
                     lifo_node_created_str = nodes_data[lifo_node].get('created')
                     lifo_node_created = pendulum.parse(lifo_node_created_str)
                     lifo_node_stable = lifo_node_created.add(minutes=(6+node_pool_stabilization_window))
                     if time_now > lifo_node_stable:
                        node_pool_stability = "stable"
                     else:
                        node_pool_stability = "stabilizing"
                     logging.info("Node Pool Stability: " + node_pool_stability)

      # scale-up node pool:
      #   - get kubernetes service account token from oci secret in vault..
      get_secret = get_secret_bundle(secrets_client, secret_id)
      secret_details = json.loads(str(get_secret.data))
      secret_base64 = (secret_details['secret_bundle_content']['content'])
      secret_byte = base64.b64decode(secret_base64)
      secret = secret_byte.decode('utf-8')
      #   - get kubeconfig..
      get_kubeconfig(ce_client, cluster_id)
      #   - evaluate node pool for unschedulablepods condition..
      unsched_pods_response = get_unsched_pods(node_pool_name, secret)
      unsched_pods_json = json.loads(str(unsched_pods_response))
      unsched_pods_val = len(unsched_pods_json)

      #   - update node pool..
      if node_pool_status != "updating":
         if node_pool_stability == "stable":
            if unsched_pods_val > 0.0:
               if node_pool_init_size < node_pool_max_size:
                  node_pool_new_size = node_pool_init_size + 1
                  node_pool_expanding = 1
                  logging.info("Scale-Up Node Pool..")
                  update_node_pool(ce_client, node_pool_id, availability_domain, subnet_id, node_pool_new_size)
                  node_pool_result = "scale-up"

      # scale-down node pool..
      #   - establish aggregated resource % utilisation data points..
      if unsched_pods_val == 0.0:
         if node_pool_init_size != 0:
            if node_pool_status != "updating":
               if node_pool_expanding != 1:
                  for (outer_k, outer_v) in nodes_data.items():
                     for (inner_k, inner_v) in outer_v.items():
                        if inner_k == "cpu_load":
                           sum_cpu_load += inner_v
                        if inner_k == "ram_load":
                           sum_ram_load += inner_v
                           ave_cpu_load = sum_cpu_load / node_pool_init_size
                           ave_ram_load = sum_ram_load / node_pool_init_size
                           logging.info("ave_cpu_load:" + str(ave_cpu_load))
                           logging.info("ave_ram_load:" + str(ave_ram_load))

      #   - evaluate aggregated resource utilisation..
      if unsched_pods_val == 0.0:
         if node_pool_init_size != 0:
            if node_pool_status != "updating":
               if node_pool_expanding != 1:
                  if node_pool_eval_cpu_load != 0:
                     # evaluate cpu load..
                     if ave_cpu_load < node_pool_eval_cpu_load:
                        node_pool_new_size = node_pool_init_size - 1
                        if node_pool_min_size <= node_pool_new_size:
                           node_pool_contract = 1
                           node_pool_contract_cpu = 1
                           logging.info("Scale-Down Node Pool: CPU..")
                  if node_pool_eval_ram_load != 0:
                     # evaluate ram load..
                     if ave_ram_load < node_pool_eval_ram_load:
                        node_pool_new_size = node_pool_init_size - 1
                        if node_pool_min_size <= node_pool_new_size:
                           node_pool_contract = 1
                           node_pool_contract_ram = 1
                           logging.info("Scale-Down Node Pool: RAM..")

                  # if scale-down condition met, drain worker node & update node pool:
                  if node_pool_contract == 1:
                     #   - cordon & drain node..
                     drain_node(lifo_node_name, secret)
                     #   - update node pool..
                     logging.info("Scale-Down Node Pool")
                     update_node_pool(ce_client, node_pool_id, availability_domain, subnet_id, node_pool_new_size)
                     node_pool_result = "scale-down"

      #   - define scale operation direction & cause..
      if "node_pool_result" in locals():
         if node_pool_result == "scale-up":
            node_pool_result_reason = "unschedulable-pods"
         if node_pool_result == "scale-down":
            if "node_pool_contract_cpu" in locals():
               node_pool_result_reason = "cpu"
            if "node_pool_contract_ram" in locals():
               if node_pool_result_reason == "cpu":
                  node_pool_result_reason = "cpu+ram"
               else:
                  node_pool_result_reason = "ram"

      #   - define function response data..
      if "node_pool_result" in locals():
         # scale-up..
         if node_pool_result_reason == "unschedulable-pods":
            result_dict = {'success': {'action': node_pool_result, 'reason': node_pool_result_reason, 'unschedulable-pods-count': str(unsched_pods_val), 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_status, 'node-count': str(node_pool_new_size)}}
            result = json.dumps(result_dict)
         else:
            # scale-down..
            result_dict = {'success': {'action': node_pool_result, 'reason': node_pool_result_reason, 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_status, 'node-count': str(node_pool_new_size)}}
            result = json.dumps(result_dict)
      else:
         # no scale-up: node_pool_max_size..
         if unsched_pods_val > 0:
            if node_pool_init_size == node_pool_max_size:
               result_dict = {'warning': {'action': 'none', 'reason': 'node-max-limit-reached', 'unschedulable-pods-count': str(unsched_pods_val), 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_status, 'node-count': str(node_pool_init_size)}}
               result = json.dumps(result_dict)
         # no action: node_pool_stability..
         if node_pool_stability == "stabilizing":
            result_dict = {'success': {'action': 'none', 'reason': 'node-pool-status', 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_stability, 'unschedulable-pods-count': str(unsched_pods_val), 'node-count': str(node_pool_init_size)}}
            result = json.dumps(result_dict)
         # no action: node_pool_status..
         if node_pool_status == "updating":
            result_dict = {'success': {'action': 'none', 'reason': 'node-pool-updating', 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_status, 'node-count': str(node_pool_init_size)}}
            result = json.dumps(result_dict)
         # no action..
         if unsched_pods_val == 0:
            if node_pool_status != "updating":
               result_dict = {'success': {'action': 'none', 'reason': 'no-resource-pressure', 'node-pool-name': node_pool_name, 'node-pool-status': node_pool_status, 'node-count': str(node_pool_init_size)}}
               result = json.dumps(result_dict)

      #   - log nodes_data dict details..
      logging.info('Nodes: ' + json.dumps(nodes_data, indent = 4))

      #   - log result..
      logging.info('Result: ' + json.dumps(result_dict, indent = 4))

   else:
      # exit if missing external / user-defined variables..
      #   - define function response data..
      result_dict = {'error': {'reason': 'missing-input-data'}}
      result = json.dumps(result_dict)

      #   - log result..
      logging.info('Result: ' + json.dumps(result_dict, indent = 4))

   return result
