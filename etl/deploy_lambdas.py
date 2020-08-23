import os
import shutil
import tempfile
import logging

import boto3
from botocore.exceptions import ClientError

import utils


dt_now = utils.Datetime().get_datetime_string_f()

season = '202021'
extract_config = {
    'module': 'lambda_extract.py',
    'dependencies': {
        'internal': ['urls.py',
                     'utils.py',
                     'extract.py'],
        'external': ['requests']
    },
    's3': {
        'bucket': 'fpl-alldata',
        'out_object': f'lambda_layers/fpl_extract_{dt_now}.zip'
    },
     'function': {
         'layer_name': f'layerExtractFpls{season}',
         'runtime': 'python3.7',
         'function_name': 'extract-Fpl',
         'timeout': 120,
         'memory': 256,
         'env_vars': {
             'AWS_S3_BUCKET': 'fpl-alldata',
             'AWS_S3_BUCKET_KEY_ROOT': f'etl_staging/raw/season-{season}/'
             },
         'role': 'arn:aws:iam::627712154013:role/lambda-fpl',
         'handler': 'lambda_extract.lambda_handler'
    }
}


def make_package(data, tempdir, tempdir_build):

    shutil.copy(data['module'],
                os.path.join(tempdir, os.path.split(data['module'])[-1]))

    for f in data['dependencies']['internal']:
        shutil.copy(f, os.path.join(tempdir, os.path.split(f)[-1]))

    install_deps = ' '.join(data['dependencies']['external'])
    pip_cmd = f"pip install {install_deps} -t {tempdir}"
    os.system(pip_cmd)
    os.system(f'chmod -R 777 {tempdir}')


    package_name = data['function']['function_name']
    zip_base = os.path.join(tempdir_build, f'{package_name}_deploy_package')
    zipped = shutil.make_archive(zip_base, 'zip', tempdir)
    os.system(f'chmod +x {zipped}')


def publish_layer(lambda_client, data)
    ret = lambda_client.publish_layer_version(
                    LayerName=data['function']['layer_name'],
                    Content={
                        'S3Bucket': data['s3']['bucket'],
                        'S3Key': data['s3']['out_object'],
                    },
                    CompatibleRuntimes=[
                            data['function']['runtime']
                    ]
                    )

# Retrieve latest version of the layer for use
def get_latest_arn(lambda_client, data):
    layer_details = lambda_client.list_layer_versions(
        LayerName=data['function']['layer_name']
    )
    return layer_details['LayerVersions'][0]['LayerVersionArn']


def update_or_create_lambda(lambda_client, data, latest_layer_arn):

    try:
        rsp_cup = lambda_client.update_function_code(
            FunctionName=data['function']['function_name'],
            S3Bucket=data['s3']['bucket'],
            S3Key=data['s3']['out_object']
        )
    except ClientError:
        rsp_create = lambda_client.create_function(
            FunctionName=data['function']['function_name'],
            Runtime=data['function']['runtime'],
            Role=data['function']['role'],
            Handler=data['function']['handler'],
            Code={
                'S3Bucket': data['s3']['bucket'],
                'S3Key': data['s3']['out_object']
            },
            Timeout=data['function']['timeout'],
            MemorySize=data['function']['memory'],
            Environment={
                'Variables': data['function']['env_vars']
            },
            Layers=[latest_layer_arn]
        )
    else:
        rsp_nup = lambda_client.update_function_configuration(
            FunctionName=data['function']['function_name'],
            Runtime=data['function']['runtime'],
            Role=data['function']['role'],
            Handler=data['function']['handler'],
            Timeout=data['function']['timeout'],
            MemorySize=data['function']['memory'],
            Environment={
                'Variables': data['function']['env_vars']
            },
            Layers=[latest_layer_arn])


def deploy_lambda(lambda_config: dict):
    """Build an AWS lambda deployment package and then deploy it to AWS

    lambda_config: configuration dictionary for an AWS lambda function
    """
    aws_lambda = boto3.client('lambda')

    # Two temporary dirs, one for gathering dependencies, the other for storing
    # a zipped version of the deployment package
    tempdir = tempfile.mkdtemp()
    tempdir_build = tempfile.mkdtemp()

    # Gather deployment dependencies and zip
    make_package(lambda_config, tempdir, tempdir_build)

    # Upload deployment package
    utils.upload_s3_file(zipped, lambda_config['s3']['bucket'],
                         lambda_config['s3']['out_object'])

    # Teardown temporary directories
    shutil.rmtree(tempdir)
    shutil.rmtree(tempdir_build)

    # Publish the lambda layer
    publish_layer(aws_lambda, lambda_config)

    # Need to work with most recent lambda layer if multiple have been
    # published
    latest_layer_arn = get_latest_arn(aws_lambda, lambda_config)

    # Preferentially update lambda function if it already exists, otherwise
    # create a new one
    update_or_create_lambda(aws_lambda, lambda_config, latest_layer_arn)


if __name__ == "__main__":
    deploy_lambda(extract_config)
