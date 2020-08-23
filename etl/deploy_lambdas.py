import os
import shutil
import tempfile
import logging

import boto3
from botocore.exceptions import ClientError

import utils


dt = utils.Datetime()
dt_now = dt.get_datetime_string_f()

season = '202021'
name = 'Extract'
data = {
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
         'layer_name': 'layerExtractFpls202021',
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

tempdir = tempfile.mkdtemp()

shutil.copy(data['module'],
            os.path.join(tempdir, os.path.split(data['module'])[-1]))

for f in data['dependencies']['internal']:
    shutil.copy(f, os.path.join(tempdir, os.path.split(f)[-1]))

install_deps = ' '.join(data['dependencies']['external'])
pip_cmd = f"pip install {install_deps} -t {tempdir}"
os.system(pip_cmd)
os.system(f'chmod -R 777 {tempdir}')

tempdir_build = tempfile.mkdtemp()

package_name = data['function']['function_name']
zip_base = os.path.join(tempdir_build, f'{package_name}_deploy_package')
zipped = shutil.make_archive(zip_base, 'zip', tempdir)
os.system(f'chmod +x {zipped}')

# Upload file here
utils.upload_s3_file(zipped, data['s3']['bucket'], data['s3']['out_object'])

shutil.rmtree(tempdir)
shutil.rmtree(tempdir_build)

aws_lambda = boto3.client('lambda')

ret = aws_lambda.publish_layer_version(
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
layer_details = aws_lambda.list_layer_versions(
    LayerName=data['function']['layer_name']
)
latest_layer_arn = \
    layer_details['LayerVersions'][0]['LayerVersionArn']

try:
    rsp_cup = aws_lambda.update_function_code(
        FunctionName=data['function']['function_name'],
        S3Bucket=data['s3']['bucket'],
        S3Key=data['s3']['out_object']
    )
except ClientError:
    rsp_create = aws_lambda.create_function(
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
    rsp_nup = aws_lambda.update_function_configuration(
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


def deploy_lambda():


if __name__ == "__main__":
    deploy_lambda()
