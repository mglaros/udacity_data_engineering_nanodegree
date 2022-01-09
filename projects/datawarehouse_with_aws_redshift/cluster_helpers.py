import boto3
from botocore.exceptions import ClientError
import json
import configparser
import time
#Collection of helper functions to automate the creation of
#the Redshift cluster and cleans up and deletes the created resources
#to avoid any incurred costs

def load_config(config_file_name='dwh.cfg'):
    """Load configuration file"""
    config = configparser.ConfigParser()
    config.read_file(open(config_file_name))
    config_dict = {
    "KEY": config.get("AWS","KEY"),
    "SECRET": config.get("AWS","SECRET"),
    "HOST": config.get("CLUSTER", "HOST"),
    "CLUSTER_TYPE": config.get("CLUSTER","CLUSTER_TYPE"),
    "NUM_NODES": config.get("CLUSTER","NUM_NODES"),
    "NODE_TYPE": config.get("CLUSTER","NODE_TYPE"),
    "IAM_ROLE_NAME": config.get("CLUSTER", "IAM_ROLE_NAME"),
    "CLUSTER_IDENTIFIER": config.get("CLUSTER","CLUSTER_IDENTIFIER"),
    "DB_NAME": config.get("CLUSTER","DB_NAME"),
    "DB_USER": config.get("CLUSTER","DB_USER"),
    "DB_PASSWORD": config.get("CLUSTER","DB_PASSWORD"),
    "DB_PORT": config.get("CLUSTER","DB_PORT"),
    "LOG_DATA": config.get("S3", "LOG_DATA"),
    "LOG_JSONPATH": config.get("S3", "LOG_JSONPATH"),
    "SONG_DATA": config.get("S3", "SONG_DATA"),
    "IAM_ROLE": config.get("IAM_ROLE", "ARN")
    }
    return config_dict

def write_config(cluster_endpoint, role_arn, config_file_name='dwh.cfg'):
    """writes out new config file once Redshift cluster is created
    and includes the cluster endpoint and role ARN"""
    config = configparser.ConfigParser()
    config.read_file(open(config_file_name))
    config.set("CLUSTER", "HOST", cluster_endpoint)
    config.set("IAM_ROLE", "ARN", role_arn)
    with open(config_file_name, 'w') as config_file:
        config.write(config_file)


def create_clients(config, region_name='us-west-2'):
    """Creates ec2, s3, iam, and Redshift clients"""

    ec2 = boto3.resource('ec2', 
                        aws_access_key_id=config['KEY'],
                        aws_secret_access_key=config['SECRET'],
                        region_name=region_name)

    s3 = boto3.resource('s3', 
                        aws_access_key_id=config['KEY'],
                        aws_secret_access_key=config['SECRET'],
                        region_name=region_name)

    iam = boto3.client('iam', 
                    aws_access_key_id=config['KEY'],
                    aws_secret_access_key=config['SECRET'],
                    region_name=region_name)

    redshift = boto3.client('redshift', 
                            aws_access_key_id=config['KEY'],
                            aws_secret_access_key=config['SECRET'],
                            region_name=region_name)
    return (ec2, s3, iam, redshift) 



def create_iam_role(config, iam_client):
    """Create an IAM Role that allows Redshift 
    to access S3 bucket (ReadOnly) and returns the created role ARN"""

    try:
        print(f"Creating new IAM Role {config['IAM_ROLE_NAME']}")
        dwhRole = iam_client.create_role(
            Path='/',
            RoleName=config['IAM_ROLE_NAME'],
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        ) 
        print('Attaching S3 Read Only Access Policy')
        iam_client.attach_role_policy(
                RoleName=config['IAM_ROLE_NAME'],
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )
        roleArn = iam_client.get_role(RoleName=config['IAM_ROLE_NAME'])['Role']['Arn']
        print(f"Successfully created role {config['IAM_ROLE_NAME']} with ARN {roleArn}")
        return roleArn
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"IAM role {config['IAM_ROLE_NAME']} already exists")
            roleArn = iam_client.get_role(RoleName=config['IAM_ROLE_NAME'])['Role']['Arn']
            return roleArn
        else:
            print("Unexpected error: %s" % e)

def create_redshift_cluster(config, roleArn, redshift_client):
    """Creates a Redshift cluster using the S3 Read Only role ARN"""

    try:
        response = redshift_client.create_cluster(        
        #Hardware
        ClusterType=config['CLUSTER_TYPE'],
        NodeType=config['NODE_TYPE'],
        NumberOfNodes=int(config['NUM_NODES']),

        #Identifiers & Credentials
        DBName=config['DB_NAME'],
        ClusterIdentifier=config['CLUSTER_IDENTIFIER'],
        MasterUsername=config['DB_USER'],
        MasterUserPassword=config['DB_PASSWORD'],
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
         
    )
        #Get cluster status and wait for it to become available
        cluster_status = redshift_client.describe_clusters(ClusterIdentifier=config['CLUSTER_IDENTIFIER'])['Clusters'][0]["ClusterStatus"]

        while cluster_status != 'available':
            print("Waiting 10 seconds to check cluster status again...")
            time.sleep(10)
            cluster_status = redshift_client.describe_clusters(ClusterIdentifier=config['CLUSTER_IDENTIFIER'])['Clusters'][0]["ClusterStatus"]

        print(f"Cluster {config['CLUSTER_IDENTIFIER']} is available")
        cluster_properties = redshift_client.describe_clusters(ClusterIdentifier=config['CLUSTER_IDENTIFIER'])['Clusters'][0]
        return cluster_properties
        
    except Exception as e:
        print(e)

def open_tcp_port(config, cluster_properties, ec2_client):
    """Open incoming TCP port in order to access the cluster endpoint"""
    try:
        print("Opening an incoming TCP port to access the cluster endpoint...")
        vpc = ec2_client.Vpc(id=cluster_properties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DB_PORT']),
            ToPort=int(config['DB_PORT'])
        )
        print("Success")
        cluster_endpoint = cluster_properties['Endpoint']['Address']
        cluster_iam_role_arn = cluster_properties['IamRoles'][0]['IamRoleArn']

        print(f"Cluster endpoint: {cluster_endpoint}")
        print(f"Cluster IAM role ARN: {cluster_iam_role_arn}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print("Ingress operation already created")
            cluster_endpoint = cluster_properties['Endpoint']['Address']
            cluster_iam_role_arn = cluster_properties['IamRoles'][0]['IamRoleArn']
            print(f"Cluster endpoint: {cluster_endpoint}")
            print(f"Cluster IAM role ARN: {cluster_iam_role_arn}")
            print("Updating config file with cluster endpoint and role arn...")
            write_config(cluster_endpoint, cluster_iam_role_arn)
            print("Success")
            
        else:
            print("Unexpected error: %s" % e)

def initialize():
    """Creates and initializes Redshift cluster"""
    config = load_config()
    ec2, s3, iam, redshift = create_clients(config)
    roleArn = create_iam_role(config, iam_client=iam)
    cluster_properties = create_redshift_cluster(config, roleArn=roleArn, redshift_client=redshift)
    open_tcp_port(config, cluster_properties, ec2_client=ec2)
    
def clean_up():
    """Deletes Redshift cluster, detaches IAM role, and deletes IAM role"""
    config = load_config()
    ec2_client, s3_client, iam_client, redshift_client = create_clients(config)
    print(f"Deleting Redshift cluster {config['CLUSTER_IDENTIFIER']}")
    redshift_client.delete_cluster( ClusterIdentifier=config['CLUSTER_IDENTIFIER'],  SkipFinalClusterSnapshot=True)
    print("Success")
    print(f"Detaching S3 Read Only role policy from {config['IAM_ROLE_NAME']}...")
    iam_client.detach_role_policy(RoleName=config['IAM_ROLE_NAME'], PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print("Success")
    print(f"Deleting IAM role {config['IAM_ROLE_NAME']}...")
    iam_client.delete_role(RoleName=config['IAM_ROLE_NAME'])
    print("Success") 
    
if __name__ == "__main__":
    clean_up()