# Databricks notebook source
# MAGIC %md # Enable Git Proxy for private Git server connectivity in Repos

# COMMAND ----------

# MAGIC %md
# MAGIC "Run all" this notebook to set up a cluster that proxies requests to your private Git server. Running this notebook does the following things:
# MAGIC 0. Writes a shell script to DBFS (`dbfs:/databricks/dp_git_proxy/dp_git_proxy_init.sh`) that is used as a [cluster-scoped init script](https://docs.databricks.com/clusters/init-scripts.html#example-cluster-scoped-init-scripts).
# MAGIC 0. Creates a [single node cluster](https://docs.databricks.com/clusters/single-node.html) named `dp_git_proxy` that runs the init script on start-up. **Important**: all users in the workspace will be granted "attach to" permissions to the cluster.
# MAGIC 0. Enables a feature flag that controls whether Git requests in Repos are proxied via the cluster.
# MAGIC
# MAGIC Note: You may need to wait several minutes after running this notebook for the cluster to reach a "RUNNING" state. It can also take up to 30 minutes for the feature flag configuration to take effect.
# MAGIC
# MAGIC This private preview feature is available on AWS and Azure.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### [Optional] Customize your cluster's name
# MAGIC Run the cell below to create a widget at the top of this notebook.
# MAGIC After entering your preferred name, "run all" this notebook. You may want to customize the cluster name to signal to users that this cluster is used for Repos and should not be terminated. e.g. `git_proxy_do_not_terminate`

# COMMAND ----------

dbutils.widgets.text("cluster-name", "", "Git Proxy Cluster Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Cluster Init Script to DBFS

# COMMAND ----------

dp_git_proxy_init = '''
#!/bin/bash
set -x
#--------------------------------------------------
mkdir /databricks/dp_git_proxy
cat >  /databricks/dp_git_proxy/dp_git_proxy.py <<EOF
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys
import urllib3
from base64 import b64encode
http = urllib3.PoolManager()
def str_to_b64(s):
    return b64encode(bytes(s, 'ascii')).decode('ascii')
def make_authorization(username, token):
    return "Basic {}".format(str_to_b64(username + ":" + token))
class DPProxyRequestHandler(BaseHTTPRequestHandler):
    def _proxy_response(self, response):
        self.send_response(response.status)
        for k in response.headers:
            if (k.lower() == "transfer-encoding"):
                # TODO: no chunking for now
                self.send_header(k, "identity")
            elif (k.lower() == "connection"):
                self.send_header(k, "close")
            else:
                self.send_header(k, response.headers[k])
        self.end_headers()
        self.wfile.write(response.data)
    def do_GET(self):
        url = "https:/{}".format(self.path)
        print("do_GET: {}".format(url))

        username = self.headers.get('X-Git-User-Name')
        token = self.headers.get('X-Git-User-Token')
        headers = {
            "Accept": self.headers.get('Accept'),
            "Authorization": make_authorization(username, token),
        }

        provider = self.headers.get('X-Git-User-Provider')
        if provider not in ['gitLab', 'gitLabEnterpriseEdition']:
            headers["Accept-Encoding"] = self.headers.get('Accept-Encoding')

        response = http.request('GET', url, headers=headers)
        self._proxy_response(response)
    def do_POST(self):
        url = "https:/{}".format(self.path)
        print("do_POST: {}".format(url))
        content_len = int(self.headers.get('Content-Length', 0))
        username = self.headers.get('X-Git-User-Name')
        token = self.headers.get('X-Git-User-Token')
        post_body = self.rfile.read(content_len)
        headers = {
            "Accept": self.headers.get('Accept'),
            "Accept-Encoding": self.headers.get('Accept-Encoding'),
            "Authorization": make_authorization(username, token),
            "Cache-Control": self.headers.get('Cache-Control'),
            "Connection": self.headers.get('Connection'),
            "Content-Encoding": self.headers.get('Content-Encoding'),
            "Content-Type": self.headers.get('Content-Type'),
            "Pragma": self.headers.get('Pragma'),
            "User-Agent": self.headers.get('User-Agent'),
        }
        response = http.request('POST', url, body=post_body, headers=headers)
        self._proxy_response(response)
    def do_HEAD(self):
        raise Exception("HEAD not supported")
if __name__ == '__main__':
    server_address = ('', int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
    print("Data-plane proxy server binding to {} ...".format(server_address))
    httpd = HTTPServer(server_address, DPProxyRequestHandler)
    httpd.serve_forever()
EOF
#--------------------------------------------------
cat > /etc/systemd/system/gitproxy.service <<EOF
[Service]
Type=simple
ExecStart=/databricks/python3/bin/python3 -u /databricks/dp_git_proxy/dp_git_proxy.py
StandardInput=null
StandardOutput=file:/databricks/dp_git_proxy/daemon.log
StandardError=file:/databricks/dp_git_proxy/daemon.log
Restart=always
RestartSec=1

[Unit]
Description=Git Proxy Service

[Install]
WantedBy=multi-user.target
EOF
#--------------------------------------------------
systemctl daemon-reload
systemctl enable gitproxy.service
systemctl start gitproxy.service
'''  # dp_git_proxy_init_end

location = "/databricks/dp_git_proxy/dp_git_proxy_init.sh"
dbutils.fs.mkdirs("dbfs:/databricks/dp_git_proxy/")
dbutils.fs.put(location, dp_git_proxy_init, True)

# COMMAND ----------

# MAGIC %md ## Create the proxy cluster
# MAGIC Running the following cell creates a cluster with the following configurations. You can customize the cluster configurations by editing `create_cluster_data` in the second cell below.
# MAGIC
# MAGIC - "cluster_name": "dp_git_proxy" or customized cluster name
# MAGIC - "spark_version": "8.2.x-scala2.12"
# MAGIC - "num_workers": 0
# MAGIC - "autotermination_minutes": 0
# MAGIC - "node_type_id":
# MAGIC  - AWS: "m5.large"
# MAGIC  - Azure: "Standard_DS2_v2"
# MAGIC - "spark_conf":
# MAGIC  - "spark.databricks.cluster.profile": "singleNode"
# MAGIC  - "spark.master": "local[*]"
# MAGIC - "custom_tags":
# MAGIC  - "ResourceClass": "SingleNode"
# MAGIC - "init_scripts":
# MAGIC  - "dbfs": "destination": "dbfs:/databricks/dp_git_proxy/dp_git_proxy_init.sh"
# MAGIC - AWS specific:
# MAGIC  - "ebs_volume_count": "1"
# MAGIC  - "ebs_volume_size": "32"

# COMMAND ----------

import requests
admin_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
databricks_instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

headers = {"Authorization": f"Bearer {admin_token}"}

# Clusters
CLUSTERS_LIST_ENDPOINT = "/api/2.0/clusters/list"
CLUSTERS_CREATE_ENDPOINT = "/api/2.0/clusters/create"
CLUSTERS_LIST_NODE_TYPES_ENDPOINT = "/api/2.0/clusters/list-node-types"

# Permissions
UPDATE_PERMISSIONS_ENDPOINT = "/api/2.0/permissions/clusters"

# Workspace Conf
WORKSPACE_CONF_ENDPOINT = "/api/2.0/workspace-conf"

# get name to use for cluster
cluster_name = "dp_git_proxy" # default name
widget_value = dbutils.widgets.get("cluster-name")
workspace_conf_value = requests.get(databricks_instance + WORKSPACE_CONF_ENDPOINT + "?keys=gitProxyClusterName", headers=headers).json()["gitProxyClusterName"]
print(f"widget value: {widget_value}")
print(f"workspace conf value: {workspace_conf_value}")

if widget_value:
  cluster_name = widget_value
elif workspace_conf_value:
  cluster_name = workspace_conf_value
print(f"Using cluster name {cluster_name}")

# COMMAND ----------

create_cluster_data = {
  "cluster_name": cluster_name,
  "spark_version": "8.2.x-scala2.12",
  "num_workers": 0,
  "autotermination_minutes": 0,
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]"
  },
  "custom_tags": {
    "ResourceClass": "SingleNode"
  },
  "init_scripts": {
    "dbfs": { "destination": "dbfs:/databricks/dp_git_proxy/dp_git_proxy_init.sh" }
  }
}
# get list of node types to determine whether this workspace is on AWS or Azure
clusters_node_types = requests.get(databricks_instance + CLUSTERS_LIST_NODE_TYPES_ENDPOINT, headers=headers).json()["node_types"]
node_type_ids = [type["node_type_id"] for type in clusters_node_types]
aws_node_type_id = "m5.large"
azure_node_type_id = "Standard_DS2_v2"
if aws_node_type_id in node_type_ids:
  create_cluster_data = {
    **create_cluster_data,
    "node_type_id": aws_node_type_id,
    "aws_attributes": {
      "ebs_volume_count": "1",
      "ebs_volume_size": "32",
      "first_on_demand": "1"
    }
  }
elif azure_node_type_id in node_type_ids:
  create_cluster_data = {
    **create_cluster_data,
    "node_type_id": azure_node_type_id
  }
else:
  raise ValueError(f"Node types {aws_node_type_id} or {azure_node_type_id} do not exist. Make sure you are on AWS or Azure, or contact support.")

# Note: this only returns up to 100 terminated all-purpose clusters in the past 30 days
clusters_list_response = requests.get(databricks_instance + CLUSTERS_LIST_ENDPOINT, headers=headers).json()
clusters_list = clusters_list_response["clusters"]
clusters_names = [cluster["cluster_name"] for cluster in clusters_list_response["clusters"]]
print(f"List of existing clusters: {clusters_names}")

if cluster_name in clusters_names:
  raise ValueError(f"Cluster called {cluster_name} already exists. Please delete this cluster and re-run this notebook")
else:
  # Create a new cluster named cluster_name that will proxy requests to the private Git server
  print(f"Create cluster POST request data: {create_cluster_data}")
  clusters_create_response = requests.post(databricks_instance + CLUSTERS_CREATE_ENDPOINT, headers=headers, json=create_cluster_data).json()
  print(f"Create cluster response: {clusters_create_response}")
  cluster_id = clusters_create_response["cluster_id"]
  print(f"Created new cluster with id {cluster_id}")
  update_permissions_data = {
    "access_control_list": [
      {
        "group_name": "users",
        "permission_level": "CAN_ATTACH_TO"
      }
    ]
  }
  update_permissions_response = requests.patch(databricks_instance + UPDATE_PERMISSIONS_ENDPOINT + f"/{cluster_id}", headers=headers, json=update_permissions_data).json()
  print(f"Update permissions response: {update_permissions_response}")
  print(f"Gave all users ATTACH TO permissions to cluster {cluster_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flip the feature flag!
# MAGIC This flips the feature flag to route Git requests to the cluster. The change should take into effect within an hour.

# COMMAND ----------

patch_enable_git_proxy_data = {
  "enableGitProxy": "true"
}
patch_git_proxy_cluster_name_data = {
  "gitProxyClusterName": cluster_name
}
requests.patch(databricks_instance + WORKSPACE_CONF_ENDPOINT, headers=headers, json=patch_enable_git_proxy_data)
requests.patch(databricks_instance + WORKSPACE_CONF_ENDPOINT, headers=headers, json=patch_git_proxy_cluster_name_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check that flag has been set
# MAGIC If the command below returns with `{"enableGitProxy":"true"}`, you should be all set. Also, if you configured a custom cluster name using the widget, check that the cluster name in the response matches the name you specified.

# COMMAND ----------

get_flag_response = requests.get(databricks_instance + WORKSPACE_CONF_ENDPOINT + "?keys=enableGitProxy", headers=headers).json()
get_cluster_name_response = requests.get(databricks_instance + WORKSPACE_CONF_ENDPOINT + "?keys=gitProxyClusterName", headers=headers).json()
print(f"Get enableGitProxy response: {get_flag_response}")
print(f"Get gitProxyClusterName response: {get_cluster_name_response}")

# COMMAND ----------

# MAGIC %md ## Validate Git server setup
# MAGIC Attach this notebook to your **Git proxy cluster**. Edit the cell below with your Git provider URL and credentials and uncomment the Git commands. Then, run the cell to verify that your Git server is accessible from the cluster. There will be no errors running the cell if you have successfully set up the cluster and its access to your Git server.

# COMMAND ----------

# MAGIC %sh
# MAGIC # In the steps below, edit $REPO_URL to either of these formats
# MAGIC # https://<personal_access_token>@githubcloud.<company>.com/<user|org>/<repo_name>.git
# MAGIC # or
# MAGIC # https://<SSO>:<personal access token>@<server.address.com>/scm/<project>/<repo>.git
# MAGIC # Also, change $REPO_NAME appropriately
# MAGIC # REPO_URL="https://<personal_access_token>@githubcloud.<company>.com/<user|org>/<repo_name>.git"
# MAGIC # REPO_NAME="<repo_name>"
# MAGIC # GIT_CURL_VERBOSE=1 GIT_TRACE=1 git clone $REPO_URL
# MAGIC # cd $REPO_NAME
# MAGIC # git checkout -b databricks-test
# MAGIC # touch databricks-test.txt
# MAGIC # git add databricks-test.txt
# MAGIC # git commit -m 'Databricks Test'
# MAGIC # git push --set-upstream origin databricks-test   # verify that your instance will accept a pushed changeset
# MAGIC
# MAGIC # Optionally, delete the databricks-test branch from your repository with the following command
# MAGIC # git push origin --delete databricks-test

# COMMAND ----------

# MAGIC %md ## Debugging Steps
# MAGIC Attach this notebook to your **Git proxy cluster**. Run the following cells to debug the setup.

# COMMAND ----------

# MAGIC %md
# MAGIC You should see a file at `path='dbfs:/databricks/dp_git_proxy/dp_git_proxy_init.sh'`. If you do not, this means that the write to DBFS in the cells above failed. Try rerunning the script after deleting the proxy cluster.

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks/dp_git_proxy")

# COMMAND ----------

# MAGIC %md
# MAGIC You should see two files, `daemon.log` and `dp_git_proxy.py` if your init script was executed properly on cluster start.

# COMMAND ----------

# MAGIC %sh
# MAGIC # ls /databricks/dp_git_proxy

# COMMAND ----------

# MAGIC %md
# MAGIC You should see that the "Git Proxy Service" successfully started and is active.

# COMMAND ----------

# MAGIC %sh
# MAGIC # systemctl status gitproxy.service
# MAGIC # journalctl -u gitproxy.service

# COMMAND ----------

# MAGIC %md
# MAGIC `/databricks/dp_git_proxy/daemon.log` contains logs written from the proxy server running on the proxy cluster. You should see `Data-plane proxy server binding to (", 8000)"` at the start of the logs. If you do not see this, the proxy server did not successfully start.
# MAGIC
# MAGIC Following this first line, you should see more log statements for each Git request performed in Repos. Error logs in the file can assist you with debugging your setup.

# COMMAND ----------

# MAGIC %sh
# MAGIC # cat /databricks/dp_git_proxy/daemon.log

# COMMAND ----------

# MAGIC %md ## Disable the feature
# MAGIC Uncomment and run the following cell if you want to disable the feature. You should also terminate the Git proxy cluster manually by going to the "Compute" page.

# COMMAND ----------

# import requests

# admin_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# databricks_instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# headers = {"Authorization": f"Bearer {admin_token}"}
# WORKSPACE_CONF_ENDPOINT = "/api/2.0/workspace-conf"
# patch_enable_git_proxy_data = {
#   "enableGitProxy": "false"
# }
# requests.patch(databricks_instance + WORKSPACE_CONF_ENDPOINT, headers=headers, json=patch_enable_git_proxy_data)
# get_flag_response = requests.get(databricks_instance + WORKSPACE_CONF_ENDPOINT + "?keys=enableGitProxy", headers=headers).json()
# print(f"Get enableGitProxy response: {get_flag_response}")
