"""
## Sample Airflow DAG to trigger IICS mappings
Data Integration TaskType , use one of the following codes:
  DMASK. Masking task.
  DRS. Replication task.
  DSS. Synchronization task.
  MTT. Mapping task.
  PCS. PowerCenter task.
"""

import json
import sys
import time
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

######### IICS Parameters Start ##########
iics_username = "iicsuser_name"
iics_password = "iics_password"
task_type = 'MTT'
base_url = "https://dm-us.informaticacloud.com/ma/api/v2/user/login"
CDI_task_name = ["Task_Date_Dim", "Task_Items", "Task_Store_Sales"]
CDI_E_task_name = ["Task_Total_Store_Sales_IWDEMO"]



######### IICS Parameters End ##########

# Airflow Parameters -- these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
   'owner': 'infa',
   'depends_on_past': False,
   'email': ['airflow@example.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=1),
   'start_date': datetime.now() - timedelta(seconds=10),
   'schedule': '@daily'
}


def get_session_id(un, pw):
   session_id = ''
   data = {'@type': 'login', 'username': un, 'password': pw}

   url = base_url
   headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
   r = requests.post(url, data=json.dumps(data), headers=headers)
   # print('Session Id API Response Status Code: ' + str(r.status_code))

   if r.status_code == 200:
      session_id = r.json()["icSessionId"]
      server_url = r.json()["serverUrl"]
      # print('Session Id: ' + session_id)

   else:
      print('API call failed:')
      print(r.headers)
      print(r.json())
      sys.exit(1)
   return session_id, server_url

def start_job(session_id, server_url, taskname, taskType):
   '''
   Use Session Id and Server URL from the user login API
   and start the specified job
   '''
   job_start_url = server_url + "/api/v2/job"
   headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
   data = {'@type': 'job', 'taskName': taskname, 'taskType': taskType}
   r = requests.post(job_start_url, data=json.dumps(data), headers=headers)

   if r.status_code == 200:
      response_content = json.loads(r.text)
      taskid = response_content['taskId']
      runid = response_content['runId']
      tname = response_content['taskName']
      print("Job " + taskname + " has been successfully started")
      return taskid, runid, tname

   else:
      print('Job failed to start with status: ' + str(r.status_code))
      print(r.content)

def get_status(server_url, session_id):
   job_activity_url = server_url + "/api/v2/activity/activityMonitor"
   headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
   r = requests.get(job_activity_url, headers=headers)
   if r.status_code == 200:
      response_content = json.loads(r.text)

      for obj in response_content:
         tn = obj['taskName']
         tid = obj['taskId']

      exec_state = obj['executionState']
      rid = obj['runId']
      print("Status of job " + tn + " is " + exec_state)
      return tid, exec_state, tn, rid

   else:
      print('Failed to get activity monitor : ' + str(r.status_code))
      print(r.content)

def execute_task(task_name):
   username = iics_username
   password = iics_password
   login_response = get_session_id(username, password)
   session_id = login_response[0]
   server_url = login_response[1]
   start_job(session_id, server_url, task_name, task_type)
   log_url = server_url + "/api/v2/activity/activityLog/"
   headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
   task_status = get_status(server_url, session_id)
   task_id = task_status[0]
   run_id = task_status[3]

   while True:
      status = {"RUNNING", "INITIALIZED", "STOPPING", "QUEUED"}

      time.sleep(15)
      new_status = get_status(server_url, session_id)

      if new_status is None:
         url = log_url + "?taskId=" + task_id + "&runId=" + str(run_id)

      r = requests.get(url, headers=headers)
      response_content = json.loads(r.text)

      for obj in response_content:
         t_id = obj['id']

      task_log = requests.get(log_url + t_id + "/sessionLog", headers=headers)
      print(task_log.text)
      break


# Airflow DAG

dag = DAG(
   'IICS_Airflow_Demo',
   default_args=default_args,
   description='A Sample IICS Airflow DAG')

cdi_start = DummyOperator(
   task_id='cdi_start',
   dag=dag
)

cdi_end = DummyOperator(
   task_id='cdi_end',
   dag=dag)

for i in CDI_task_name:
   cdi_task = PythonOperator(
      task_id='IICS_CDI_' + i,
      python_callable=execute_task,
      op_kwargs={'task_name': i},
      dag=dag)

   cdi_start >> cdi_task >> cdi_end

for j in CDI_E_task_name:
   cdi_e_task = PythonOperator(
      task_id='IICS_CDI_E_' + j,
      python_callable=execute_task,
      op_kwargs={'task_name': j},
      dag=dag)
   
   cdi_end >> cdi_e_task
