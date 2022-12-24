#import of the required libraries
import re
from datetime import datetime
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

#define the default arguments 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 12, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': '0 0 * * *'
}

#define the dag 
dag = DAG(dag_id = 'process_web_log', catchup = False, default_args = default_args)

#define the first task (scan_for_log)
scan_for_log = FileSensor(
    task_id = 'scan_for_log',
    poke_interval = 30,
    fs_conn_id = 'fs_default',
    filepath = '/Users/liliiaaliakberova/dags/log.txt',
    dag = dag)

#define the function for the extract task
def extractip_to_txt():
    fh = open('/Users/liliiaaliakberova/dags/log.txt') 
    fstring = fh.readlines()

    # declaring the regex pattern for IP addresses
    pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')

    #initialize the list of ip
    lst = []

    # extract the IP addresses
    for line in fstring:
        lst.append(pattern.search(line)[0])

    #write ip addresses
    fp = open(r'/Users/liliiaaliakberova/dags/extracted_data.txt', 'w')
    for i in lst:
        fp.write("%s\n" % i)

#define the function for the transform task 
def transformip_to_txt():

    # open and read the file
    fh = open('/Users/liliiaaliakberova/dags/extracted_data.txt')
    fstring = fh.readlines()

    #initialize the list of ip
    lst = []

    # extract the IP addresses
    for line in fstring:
        if line == '198.46.149.143\n':
            continue
        else:
            lst.append(line)

    #write ip addresses
    fp = open(r'Users/liliiaaliakberova/dags/transformed_data.txt', 'w')
    for i in lst:
        fp.write("%s" % i)


#define the second task (extract_data)
extract_data = PythonOperator(
    task_id = 'extract_data',
    python_callable  = extractip_to_txt,
    dag = dag)

#define the third task (transform_data)
transform_data = PythonOperator(
    task_id = 'transform_data',
    python_callable  = transformip_to_txt,
    dag = dag)

#define the fourth task (load_data) using load_data.sh 
load_data = BashOperator(
    task_id = 'load_data',
    bash_command="/Users/liliiaaliakberova/dags/load_data.sh",
    dag=dag)

#construct the flow
scan_for_log >> extract_data >> transform_data >> load_data

