import gzip
import boto3
import tempfile
from boto3.s3.transfer import S3Transfer
import sys

if len(sys.argv) != 2:
    print "Usage: get_stdout.py [bucket name]"
    sys.exit(0)

bucket_name = sys.argv[1]
n_nodes = [1, 2, 4, 8, 16]

client = boto3.client('s3', 'us-east-1')
transfer = S3Transfer(client)

out_file = open('times.out', 'w')

for node in n_nodes:
    
    out_file.write("Number of Nodes: %d\n"%node)
    
    # Getting ids
    ids_file = 'cluster-%d-nodes.out'%node
    f = open(ids_file, 'r')
    line = f.readline()
    log_id = line.split(":")[1].replace("\n", "")
    line = f.readline()
    cluster_id = line.split(":")[1].replace("\n", "")
    line = f.readline()
    aggregation_id = line.split(":")[1].replace("\n", "")
    line = f.readline()
    index_creation_id = line.split(":")[1].replace("\n", "")
    line = f.readline()
    relationship_id = line.split(":")[1].replace("\n", "")
    f.close()
    
    # Retrieving file (aggregation)
    temp = tempfile.TemporaryFile()
    stdout_file = 'emr-logs/%s/%s/steps/%s/stdout.gz'%(log_id, cluster_id, aggregation_id)
    transfer.download_file(bucket_name, stdout_file, temp.name)
    f = gzip.open(temp.name, 'rb')
    aggregation_time = 0.0
    line = f.readline()
    while line != '':
        if 'aggregates' in line:
            aggregation_time = float(line.split('\t')[1].replace("\n", ""))/1000.0
            break
        line = f.readline()
    f.close()
    if aggregation_time == 0.0:
        temp = tempfile.TemporaryFile()
        controller_file = 'emr-logs/%s/%s/steps/%s/controller.gz'%(log_id, cluster_id, aggregation_id)
        transfer.download_file(bucket_name, controller_file, temp.name)
        f = gzip.open(temp.name, 'rb')
        line = f.readline()
        while line != '':
            if 'Step succeeded with' in line:
                aggregation_time = float(line.split(' ')[-2])
                break
            line = f.readline()
        f.close()
    out_file.write("aggregation\t%.4f\n"%aggregation_time)
    
    # Retrieving file (index creation)
    temp = tempfile.TemporaryFile()
    stdout_file = 'emr-logs/%s/%s/steps/%s/stdout.gz'%(log_id, cluster_id, index_creation_id)
    transfer.download_file(bucket_name, stdout_file, temp.name)
    f = gzip.open(temp.name, 'rb')
    index_creation_time = 0.0
    line = f.readline()
    while line != '':
        if 'index' in line:
            index_creation_time = float(line.split('\t')[1].replace("\n", ""))/1000.0
            break
        line = f.readline()
    f.close()
    if index_creation_time == 0.0:
        temp = tempfile.TemporaryFile()
        controller_file = 'emr-logs/%s/%s/steps/%s/controller.gz'%(log_id, cluster_id, index_creation_id)
        transfer.download_file(bucket_name, controller_file, temp.name)
        f = gzip.open(temp.name, 'rb')
        line = f.readline()
        while line != '':
            if 'Step succeeded with' in line:
                index_creation_time = float(line.split(' ')[-2])
                break
            line = f.readline()
        f.close()
    out_file.write("index creation\t%.4f\n"%index_creation_time)
    
    # Retrieving file (index creation)
    temp = tempfile.TemporaryFile()
    stdout_file = 'emr-logs/%s/%s/steps/%s/stdout.gz'%(log_id, cluster_id, relationship_id)
    transfer.download_file(bucket_name, stdout_file, temp.name)
    f = gzip.open(temp.name, 'rb')
    relationship_time = 0.0
    line = f.readline()
    while line != '':
        if 'relationship-restricted' in line:
            relationship_time = float(line.split('\t')[1].replace("\n", ""))/1000.0
            break
        line = f.readline()
    f.close()
    if relationship_time == 0.0:
        temp = tempfile.TemporaryFile()
        controller_file = 'emr-logs/%s/%s/steps/%s/controller.gz'%(log_id, cluster_id, relationship_id)
        transfer.download_file(bucket_name, controller_file, temp.name)
        f = gzip.open(temp.name, 'rb')
        line = f.readline()
        while line != '':
            if 'Step succeeded with' in line:
                relationship_time = float(line.split(' ')[-2])
                break
            line = f.readline()
        f.close()
    out_file.write("relationship\t%.4f\n"%relationship_time)
    
out_file.close()