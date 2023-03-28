import json
import requests
import uuid
from databricks.sdk.runtime import *
from workflows.cluster import Cluster

class JobControl:
  def __init__(self):
    pass

  def send_job_request(self, action, request_func):
    api_url = sc.getLocalProperty("spark.databricks.api.url")
    org_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("orgId").getOrElse(None)
    token = sc.getLocalProperty("spark.databricks.token")

    response = request_func(f'{api_url}/api/2.1/jobs/{action}?o={org_id}', {"Authorization": f"Bearer {token}"})
    if response.status_code != 200:
      raise Exception("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    return response.json()

  def find_job_by_name(self, name):
    jobs = self.send_job_request('list', lambda u, h: requests.get(u, headers=h))
    j = [job for job in jobs['jobs'] if job['settings'].get('name', '') == name]
    if len(j) > 0:
      return j[0]
    return None
  
  def get_job_by_name(self, name):  
    job = self.find_job_by_name(name)
    if job is None:
      return None

    job_id = job['job_id']

    job = self.send_job_request('get', lambda u, h: requests.get(f'{u}&job_id={job_id}', headers=h))
    return job

class Workflow:
  _tasks = {}
  _clusters = {}
  _name = None
  _job_control = None

  def __init__(self, name):
    self._job_control = JobControl()
    self._tasks = {}
    self._name = name
    self._clusters["default"] = Cluster(self, "default", {
          "cluster_name": "",
          "spark_version": "11.3.x-scala2.12",
          "aws_attributes": {
              "first_on_demand": 1,
              "availability": "SPOT_WITH_FALLBACK",
              "zone_id": "us-west-2a",
              "spot_bid_price_percent": 100,
              "ebs_volume_count": 0
          },
          "node_type_id": "i3.xlarge",
          "spark_env_vars": {
              "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
          },
          "enable_elastic_disk": False,
          "data_security_mode": "SINGLE_USER",
          "runtime_engine": "STANDARD",
          "num_workers": 8
        })
  
  @property
  def name(self):
    return self._name

  def get_cluster(self, name):
    return self._clusters.get(name)

  def _add_cluster(self, cluster):
    self._clusters[cluster.name] = cluster

  def _append(self, task):
    currentTask = self._tasks.get(task.name)
    index = len(self._tasks)
    if currentTask is not None:
      index = currentTask[0]      
    self._tasks[task.name] = (index, task)
  
  def _getLastTask(self):
    maxIndex = -1
    lastTask = None
    for k in self._tasks:
      t = self._tasks[k]
      if t[0] > maxIndex:
        lastTask = t[1]
        maxIndex = t[0]
    return lastTask

  def commit(self):
    task_arr = []
    for k in self._tasks:
      t = self._tasks[k]
      task_arr.insert(t[0], t[1])

    settings = {
                  "name": f"{self.name}",
                  "email_notifications": {
                      "no_alert_for_skipped_runs": False
                  },
                  "webhook_notifications": {},
                  "timeout_seconds": 0,
                  "max_concurrent_runs": 1,
                  "tasks": [
                    {
                        "task_key": f"{t.name}",
                        "depends_on": [
                          {
                              "task_key": f"{d.name}"
                          } for d in t.dependencies
                        ],
                        "python_wheel_task": {
                            "package_name": f"{t.package}",
                            "entry_point": f"{t.entry_point}",
                            "parameters": [f"{p}" for p in t.parameters]
                        },
                        "libraries": [l for l in t.libraries],
                        "job_cluster_key": f"{t.cluster_name}",
                        "timeout_seconds": 0,
                        "email_notifications": {}
                    } for t in task_arr
                  ],
                  "job_clusters": [
                  {
                    "job_cluster_key": f"{i[0]}",
                    "new_cluster": i[1].config
                  } for i in self._clusters.items()
                ],
                "format": "MULTI_TASK"
              }

    j = self._job_control.get_job_by_name(self.name)
    if j is None:
      response = self._job_control.send_job_request('create', lambda u, h: requests.post(f'{u}', json=settings, headers=h))
    else:
      j['new_settings'] = settings
      del j['settings']
      del j['created_time']
      del j['creator_user_name']
      response = self._job_control.send_job_request('reset', lambda u, h: requests.post(f'{u}', json=j, headers=h))
    return response
  
  def run(self):
    self.commit()
    job = self._job_control.find_job_by_name(self.name)
    if job is None:
      return None

    job_id = job['job_id']

    j = {
      "job_id": job_id,
      "idempotency_token": f"{uuid.uuid4()}"
    }
    response = self._job_control.send_job_request('run-now', lambda u, h: requests.post(f'{u}', json=j, headers=h))
    return response