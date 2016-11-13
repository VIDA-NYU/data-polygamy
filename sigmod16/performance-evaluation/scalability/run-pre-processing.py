# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import boto3
import sys
import uuid

if len(sys.argv) != 5:
    print "Usage: run-pre-processing.py [bucket name] [number of nodes] [aws_id] [aws_key]"
    sys.exit(0)

datasets = ["311","crash","citibike","gas-prices","weather","taxispeed"]
temporal = {"311": "hour",
            "citibike": "hour",
            "crash": "hour",
            "gas-prices": "week",
            "weather": "hour",
            "taxispeed": "hour"}
spatial = {"311": "nbhd",
           "citibike": "nbhd",
           "crash": "nbhd",
           "gas-prices": "city",
           "weather": "city",
           "taxispeed": "nbhd"}
c_spatial = {"311": "points",
             "citibike": "points",
             "crash": "points",
             "gas-prices": "city",
             "weather": "city",
             "taxispeed": "points"}
indices = {"311": ["0","2","1"],
           "citibike": ["1","6","5","2","10","9"],
           "crash": ["0","4","3"],
           "gas-prices": ["0","0"],
           "weather": ["0","0"],
           "taxispeed": ["0","2","1"]}

bucket_name = sys.argv[1]
n_nodes = 0
try:
    n_nodes = int(sys.argv[2])
except:
    print "Invalid number of nodes."
    sys.exit(0)
aws_id = sys.argv[3]
aws_key = sys.argv[4]
    
client = boto3.client('emr', region_name='us-east-1')
waiter = client.get_waiter('cluster_running')

id = uuid.uuid1()
log_id = 'logs-%s'%id
log_dir = 's3://%s/emr-logs/%s'%(bucket_name,log_id)

instance_groups = []
master = {'Name':           'master',
          'InstanceRole':   'MASTER',
          'InstanceType':   'm1.medium',
          'InstanceCount':  1}
slaves = {'Name':           'slave',
          'InstanceRole':   'CORE',
          'InstanceType':   'r3.2xlarge',
          'InstanceCount':  n_nodes}
instance_groups.append(master)
instance_groups.append(slaves)

debugging_step = {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': ['s3://us-west-2.elasticmapreduce/libs/state-pusher/0.1/fetch']
            }
        }

steps = []
for dataset in datasets:
    pre_processing_step = {
                'Name': 'pre-processing-%s'%dataset,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 's3://%s/data-polygamy.jar'%bucket_name,
                    'Args': [
                        "edu.nyu.vida.data_polygamy.pre_processing.PreProcessing",
                        "-m",
                        "r3.2xlarge",
                        "-n",
                        "%d"%n_nodes,
                        "-s3",
                        "-aws_id",
                        "%s"%aws_id,
                        "-aws_key",
                        "%s"%aws_key,
                        "-b",
                        "s3://%s/"%bucket_name,
                        "-dn",
                        "%s"%dataset,
                        "-dh",
                        "%s.header"%dataset,
                        "-dd",
                        "%s.defaults"%dataset,
                        "-t",
                        "%s"%temporal[dataset],
                        "-s",
                        "%s"%spatial[dataset],
                        "-cs",
                        "%s"%c_spatial[dataset],
                        "-i"
                    ]
                }
            }
    pre_processing_step['HadoopJarStep']['Args'] += indices[dataset]
    steps.append(pre_processing_step)

response = client.run_job_flow(Name='Data-Polygamy-%d-nodes-%s'%(n_nodes, id),
                               LogUri=log_dir,
                               AmiVersion='3.7.0',
                               Instances={'InstanceGroups': instance_groups,
                                          'Ec2KeyName':     'data-polygamy'},
                               Steps=[debugging_step],
                               JobFlowRole='EMR_EC2_DefaultRole',
                               ServiceRole='EMR_DefaultRole'
                               )


cluster_id = response['JobFlowId']

response = client.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=steps)

