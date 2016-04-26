import boto3
import sys
import uuid

if len(sys.argv) != 5:
    print "Usage: run_cluster.py [bucket name] [number of nodes] [aws_id] [aws_key]"
    sys.exit(0)
    
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

aggregation_step = {
            'Name': 'aggregation',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://%s/data-polygamy.jar'%bucket_name,
                'Args': [
                    "edu.nyu.vida.data_polygamy.scalar_function_computation.Aggregation",
                    "-m",
                    "r3.2xlarge",
                    "-n",i
                    "%d"%n_nodes,
                    "-s3",
                    "-aws_id",
                    "%s"%swd_id,
                    "-aws_key",
                    "%s"%swd_key,
                    "-b",
                    "s3://%s/"%bucket_name,
                    "-f",
                    "-g",
                    "311",
                    "0",
                    "0",
                    "crash",
                    "0",
                    "0",
                    "citibike",
                    "0",
                    "0",
                    "gas-prices",
                    "0",
                    "0",
                    "weather",
                    "0",
                    "0",
                    "taxispeed",
                    "0",
                    "0"
                ]
            }
        }

index_creation_step = {
            'Name': 'index-creation',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://%s/data-polygamy.jar'%bucket_name,
                'Args': [
                    "edu.nyu.vida.data_polygamy.feature_identification.IndexCreation",
                    "-m",
                    "r3.2xlarge",
                    "-n",
                    "%d"%n_nodes,
                    "-s3",
                    "-aws_id",
                    "%s"%swd_id,
                    "-aws_key",
                    "%s"%swd_key,
                    "-b",
                    "s3://%s/"%bucket_name,
                    "-f",
                    "-g",
                    "311",
                    "crash",
                    "citibike",
                    "gas-prices",
                    "weather",
                    "taxispeed"
                ]
            }
        }

relationship_step = {
            'Name': 'relationship',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://%s/data-polygamy.jar'%bucket_name,
                'Args': [
                    "edu.nyu.vida.data_polygamy.relationship_computation.Relationship",
                    "-m",
                    "r3.2xlarge",
                    "-n",
                    "%d"%n_nodes,
                    "-s3",
                    "-aws_id",
                    "%s"%swd_id,
                    "-aws_key",
                    "%s"%swd_key,
                    "-b",
                    "s3://%s/"%bucket_name,
                    "-f",
                    "-g1",
                    "311",
                    "crash",
                    "citibike",
                    "gas-prices",
                    "weather",
                    "taxispeed"
                ]
            }
        }

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
    Steps=[aggregation_step, index_creation_step, relationship_step])

aggregation_id = response['StepIds'][0]
index_creation_id = response['StepIds'][1]
relationship_id = response['StepIds'][2]

f = open("cluster-%d-nodes.out"%n_nodes, "w")
f.write("log_id:%s\ncluster_id:%s\naggregation_id:%s\nindex_creation_id:%s\nrelationship_id:%s"
        %(log_id,cluster_id,aggregation_id,index_creation_id,relationship_id))
f.close()
