import sys
import logging

import boto3
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client


def gen_instance_id(cluster_id):
    return cluster_id + '-instance'


def gen_cluster_id(cluster_id):
    return cluster_id + '-clone'


def get_cluster_writer_id(neptune, cluster_id):
    response = neptune.describe_db_clusters(DBClusterIdentifier = cluster_id)

    cluster = response['DBClusters'][0]

    status = cluster['Status']
    assert (status == 'available'), f"unexpected source cluster Status = {status}"

    for member in cluster['DBClusterMembers']:
        if member['IsClusterWriter']:
            return member['DBInstanceIdentifier']

    raise Exception(f'Missing Cluster Writer in {cluster_id} = {cluster}')


def get_db_instance_props(neptune, db_instance_id):
    response = neptune.describe_db_instances(DBInstanceIdentifier=db_instance_id)

    instance = response['DBInstances'][0]

    status = instance['DBInstanceStatus']
    assert (status == 'available'), f"unexpected db instance Status = {status}"

    subnet_group_name = instance['DBSubnetGroup']['DBSubnetGroupName']

    vpc_security_group_ids = [
        it['VpcSecurityGroupId'] for it in instance['VpcSecurityGroups'] if it['Status'] == 'active'
    ]

    security_groups = instance['DBSecurityGroups']

    return (subnet_group_name, vpc_security_group_ids, security_groups)


def gen_available_waiter(WAITER_ID, operation, delay, maxAttempts, path):
    return WaiterModel({
        'version': 2,
        'waiters': {
            WAITER_ID: {
                'operation': operation,
                'delay': delay,
                'maxAttempts': maxAttempts,
                'acceptors': [
                    {
                        'state': 'success',
                        'matcher': 'path',
                        'argument': f"{path} == 'available'",
                        'expected': True
                    }
                ]
            }
        }
    })


def wait_cluster_available(neptune, cluster_id, delay, maxAttempts):

    WAITER_ID = 'neptune_cluster_available'
    WAITER_MODEL = gen_available_waiter(WAITER_ID, 'DescribeDBClusters', delay, maxAttempts, 'DBClusters[0].Status')

    waiter = create_waiter_with_client(WAITER_ID, WAITER_MODEL, neptune)
    waiter.wait(DBClusterIdentifier=cluster_id)


def clone_cluster(neptune, cluster_id, subnet_group_name, vpc_security_group_ids):

    response = neptune.restore_db_cluster_to_point_in_time(
        SourceDBClusterIdentifier=cluster_id,
        DBClusterIdentifier=gen_cluster_id(cluster_id),
        RestoreType='copy-on-write',
        UseLatestRestorableTime=True,
        DBSubnetGroupName=subnet_group_name,
        VpcSecurityGroupIds=vpc_security_group_ids
    )

    status = response['DBCluster']['Status']
    assert (status == 'creating'), f"unexpected clone cluster Status = {status}"

    return response['DBCluster']['DBClusterIdentifier']


def create_db_instance(neptune, cluster_id, db_instance_class, security_groups):

    response = neptune.create_db_instance(
        DBClusterIdentifier=cluster_id,
        Engine='neptune',
        DBInstanceIdentifier=gen_instance_id(cluster_id),
        DBInstanceClass=db_instance_class,
        DBSecurityGroups=security_groups
    )

    status = response['DBInstance']['DBInstanceStatus']
    assert (status == 'creating'), f"unexpected db instance Status = {status}"

    return response['DBInstance']['DBInstanceIdentifier']


def wait_db_instance_available(neptune, clone_instance_id, delay, maxAttempts):

    WAITER_ID = 'neptune_db_instance_available'
    WAITER_MODEL = gen_available_waiter(WAITER_ID, 'DescribeDBInstances', delay, maxAttempts, 'DBInstances[0].DBInstanceStatus')

    waiter = create_waiter_with_client(WAITER_ID, WAITER_MODEL, neptune)
    waiter.wait(DBInstanceIdentifier=clone_instance_id)


def main(neptune, cluster_id):

    DELAY = 10
    MAX_ATTEMPTS = 200
    DB_INST_CLASS = 'db.r5.4xlarge'

    cluster_writer_id = get_cluster_writer_id(neptune, cluster_id)
    logging.info(f"Found writer instance {cluster_writer_id}")

    subnet_group_name, vpc_security_group_ids, security_groups = get_db_instance_props(neptune, cluster_writer_id)
    logging.info(f"Found properties {(subnet_group_name, vpc_security_group_ids, security_groups)}")

    clone_cluster_id = clone_cluster(neptune, cluster_id, subnet_group_name, vpc_security_group_ids)
    logging.info(f"Creating cluster {clone_cluster_id}")
    wait_cluster_available(neptune, clone_cluster_id, DELAY, MAX_ATTEMPTS)
    logging.info(f"Created cluster {clone_cluster_id}")

    clone_instance_id = create_db_instance(neptune, clone_cluster_id, DB_INST_CLASS, security_groups)
    logging.info(f"Creating instance {clone_instance_id}")
    wait_db_instance_available(neptune, clone_instance_id, DELAY, MAX_ATTEMPTS)
    logging.info(f"Created instance {clone_instance_id}")

    logging.info(f"Deleting clone cluster {clone_cluster_id}")
    neptune.delete_db_cluster(DBClusterIdentifier=clone_cluster_id, SkipFinalSnapshot=True)


if __name__ == "__main__":

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)

    neptune = boto3.client('neptune')

    cluster_id = sys.argv[1]
    main(neptune, cluster_id)

