
import yaml, time, json
import googleapiclient.discovery
from google.cloud import storage

REGION = 'global' # Currently only the "global" region is supported?
FILENAME = 'cf_model.py'
NUM_PARTITIONS = 35

# some useful functions
def load_yaml(filename):
    with open(filename, 'r') as f:
        return yaml.load(f)

def format_dict(base, formatting):
    base_formatted = json.dumps(base)
    for k,v in formatting.items():
        base_formatted = base_formatted.replace("{%s}" % k, v)
    return json.loads(base_formatted)

def parse_gcs_path(path):
    path = path[5:] # chop off gs://
    pieces = path.split('/')
    bucket = pieces.pop(0)
    filepath = '/'.join(pieces)
    return bucket, filepath

# uploads pyspark file to cloud storage
def upload_pyspark_file(project_id, gcs_path, pyspark_filename):
    print('Uploading pyspark file (%s) to GCS...' % pyspark_filename)
    bucket, gcs_filepath = parse_gcs_path(gcs_path)
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(gcs_filepath + '/' + pyspark_filename)
    with open(pyspark_filename, 'r') as f:
        blob.upload_from_file(f)
    print('Done.')

# create spark cluster
def create_cluster(dataproc, project_id, cluster_data):
    print('Creating cluster...')
    result = dataproc.projects().regions().clusters().create(
        projectId=project_id,
        region=REGION,
        body=cluster_data).execute()
    return result

# wait for spark cluster to be created
def wait_for_cluster_creation(dataproc, project_id, cluster_name):
    print('Waiting for cluster creation...')
    while True:
        result = dataproc.projects().regions().clusters().list(
            projectId=project_id,
            region=REGION).execute()
        cluster_list = result['clusters']
        cluster = [c for c in cluster_list if c['clusterName'] == cluster_name][0]
        if cluster['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        if cluster['status']['state'] == 'RUNNING':
            print("Cluster created.")
            break
        time.sleep(30)

# delete spark cluster
def delete_cluster(dataproc, project_id, cluster_name):
    print('Tearing down cluster.')
    result = dataproc.projects().regions().clusters().delete(
        projectId=project_id,
        region=REGION,
        clusterName=cluster_name).execute()
    return result

# helper function to get the cluster id / output bucket path
def get_cluster_id_by_name(project_id, cluster_name):
    result = dataproc.projects().regions().clusters().list(
        projectId=project_id,
        region=REGION).execute()
    cluster_list = result['clusters']
    cluster = [c for c in cluster_list if c['clusterName'] == cluster_name][0]
    return cluster['clusterUuid'], cluster['config']['configBucket']

# downloads pyspark output and puts into file
def download_pyspark_output(project_id, cluster_name, job_id, out_filename):
    print('Downloading output file...')
    cluster_id, output_bucket = get_cluster_id_by_name(project_id, cluster_name)
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(output_bucket)
    output_blob = 'google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'.format(cluster_id, job_id)
    with open(out_filename, 'w') as f:
        bucket.blob(output_blob).download_to_file(f)
    print('Done.')

# submits a pyspark job
def submit_pyspark_job(dataproc, project_id, cluster_name, pyspark_filepath, pyspark_args = []):
    job_details = {
        'projectId': project_id,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': pyspark_filepath,
                'args': pyspark_args
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project_id,
        region=REGION,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}.'.format(job_id))
    return job_id

# waits for pyspark job to finish
def wait_for_job(dataproc, project_id, job_id):
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project_id,
            region=REGION,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished')
            return result
        time.sleep(30)


if __name__ == '__main__':
    try:
        # initialize
        dataproc = googleapiclient.discovery.build('dataproc', 'v1')
        config = load_yaml('config.yaml')
        cluster = format_dict(load_yaml('cluster.yaml'), config)
        project_id = config['project_id']
        cluster_name = cluster['clusterName']

        # upload pyspark file
        upload_pyspark_file(project_id, config['pyspark_filepath'], FILENAME)

        # create cluster
        create_cluster(dataproc, project_id, cluster)
        wait_for_cluster_creation(dataproc, project_id, cluster_name)

        # kick off job
        pyspark_filepath_full = config['pyspark_filepath'] + '/' + FILENAME
        pyspark_args = [str(NUM_PARTITIONS), config['pyspark_filepath']]
        job_id = submit_pyspark_job(dataproc, project_id, cluster_name, pyspark_filepath_full, pyspark_args)
        wait_for_job(dataproc, project_id, job_id)

        # download output
        download_pyspark_output(project_id, cluster_name, job_id, FILENAME + '.out')
    finally:
        # delete cluster
        delete_cluster(dataproc, config['project_id'], cluster['clusterName'])
