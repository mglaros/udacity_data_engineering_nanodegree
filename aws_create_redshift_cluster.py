import pandas as pd
import time
import boto3
import json
import configparser

def load_config(config_file_name='dwh.cfg'):
    """load config file"""
    config = configparser.ConfigParser()
    config.read_file(open(config_file_name))
    config_dict = {
    "KEY": config.get('AWS','KEY'),
    "SECRET": config.get('AWS','SECRET'),
    "DWH_CLUSTER_TYPE": config.get("DWH","DWH_CLUSTER_TYPE"),
    "DWH_NUM_NODES": config.get("DWH","DWH_NUM_NODES"),
    "DWH_NODE_TYPE": config.get("DWH","DWH_NODE_TYPE"),
    "DWH_CLUSTER_IDENTIFIER": config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
    "DWH_DB": config.get("DWH","DWH_DB"),
    "DWH_DB_USER": config.get("DWH","DWH_DB_USER"),
    "DWH_DB_PASSWORD": config.get("DWH","DWH_DB_PASSWORD"),
    "DWH_PORT": config.get("DWH","DWH_PORT"),
    "DWH_IAM_ROLE_NAME": config.get("DWH", "DWH_IAM_ROLE_NAME")
    }
    return config_dict


def create_clients(config, region_name='us-west-2'):
    """creates ec2, s3, iam, and redshift clients"""

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
    """Create an IAM Role that makes Redshift 
    able to access S3 bucket (ReadOnly)
    returns the roleARN"""

    try:
        print('Creating a new IAM Role')
        dwhRole = iam_client.create_role(
            Path='/',
            RoleName=config['DWH_IAM_ROLE_NAME'],
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        ) 
        print('Attaching Policy')
        iam_client.attach_role_policy(
                RoleName=config['DWH_IAM_ROLE_NAME'],
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )
        roleArn = iam_client.get_role(RoleName=config['DWH_IAM_ROLE_NAME'])['Role']['Arn']
        print(f"Successfully created role {config['DWH_IAM_ROLE_NAME']} with ARN {roleArn}")
        return roleArn
        
    except Exception as e:
        print(e)

def create_redshift_cluster(config, roleArn, redshift_client, ec2_client):
    """Creates a Redshift cluster using roleARN"""

    try:
        response = redshift_client.create_cluster(        
        #Hardware
        ClusterType=config['DWH_CLUSTER_TYPE'],
        NodeType=config['DWH_NODE_TYPE'],
        NumberOfNodes=int(config['DWH_NUM_NODES']),

        #Identifiers & Credentials
        DBName=config['DWH_DB'],
        ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'],
        MasterUsername=config['DWH_DB_USER'],
        MasterUserPassword=config['DWH_DB_PASSWORD'],
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
         
    )

        cluster_status = redshift_client.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]["ClusterStatus"]
        #wait for cluster to become available
        while cluster_status != 'available':
            print("Waiting 10 seconds to check cluster status again...")
            time.sleep(10)
            cluster_status = redshift_client.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]["ClusterStatus"]

        print(f"Cluster {config['DWH_CLUSTER_IDENTIFIER']} is available")
        myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        print("Opening an incoming TCP port to access the cluster endpoint...")
        vpc = ec2_client.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DWH_PORT']),
            ToPort=int(config['DWH_PORT'])
        )
        return (myClusterProps['Endpoint']['Address'], myClusterProps['IamRoles'][0]['IamRoleArn'])
    except Exception as e:
        print(e)

def clean_up(config, iam_client, redshift_client):
    """delete Redshift cluster, IAM role"""
    print(f"Deleting Redshift cluster {config['DWH_CLUSTER_IDENTIFIER']}")
    redshift_client.delete_cluster( ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'],  SkipFinalClusterSnapshot=True)
    print("Success")
    print("Detaching role policty and deleting IAM role")
    iam_client.detach_role_policy(RoleName=config['DWH_IAM_ROLE_NAME'], PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_client.delete_role(RoleName=config['DWH_IAM_ROLE_NAME'])
    print("Success")





if __name__ == '__main__':
    config = load_config()
    ec2, s3, iam, redshift = create_clients(config)
    roleArn = create_iam_role(config, iam_client=iam)
    dwh_endpoint, dwh_role_arn = create_redshift_cluster(config, roleArn=roleArn, redshift_client=redshift, ec2_client=ec2)
    print("Connect to Redshift via...")
    psql_command = f"psql -h {dwh_endpoint} -U {config['DWH_DB_USER']} -d {config['DWH_DB']} -p {config['DWH_PORT']}"
    print(psql_command)
    clean_up(config, iam_client=iam, redshift_client=redshift)



    





