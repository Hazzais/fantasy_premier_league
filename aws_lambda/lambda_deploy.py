"""
Deploy a lambda function deployment package to S3 and update lambda code and
configuration for one or more functions.

Usage:
    - Assumes fpltools is in the interpreter scope and/or the code's working
      directory is that of the repo root
    - LAMBDA_FUNCTIONS defines what and how to deploy the lambda functions.

Potential/probable improvements:
# TODO: Further logging (particularly within class)
# TODO: testing (unit tests etc.)
# TODO: move class outside of file?
# TODO: move lambda dictionary to config file
"""

import os
import shutil
import logging

import boto3

from fpltools.utils import AwsS3


LAMBDA_FUNCTIONS = {'Extract':
                        {'function_module':'aws_lambda/aws_lambda_extract.py',
                         'dependencies':
                             {'internal': ['fpltools'],
                              'external': ['requests']
                              },
                         's3':
                             {'bucket': 'fpl-alldata',
                              'out_object': 'lambda_layers/live_fpl_extract.zip'},
                         'function':
                             {'layer_name': 'lyrExtractFpl',
                              'runtime': 'python3.7',
                              'function_name': 'extractFpl',
                              'timeout': 120,
                              'memory': 256,
                              'env_vars': {
                                  'AWS_S3_BUCKET': 'fpl-alldata',
                                  'AWS_S3_BUCKET_FOLDER': 'etl_staging/raw',
                                  'AWS_S3_LOG_OUTPUT': 'etl_staging/logs'},
                              'role': 'arn:aws:iam::627712154013:role/lambda-fpl',
                              'handler': 'aws_lambda_extract.lambda_handler'
                              }
                         }
                    }


class AwsLambdaDeploy:
    """Build a deploy package to save on S3 containing source lambda code
    and dependencies

    lambda_name: str
        name of lambda function, mostly used during build
    lambda_data: dict
        parameters to use in build
    build_location: str
        file location to gather dependencies and zip. Cleared down after.
    dependencies_subpath: str
        path within the final zip in which dependencies are installed
    """

    def __init__(self, lambda_name, lambda_data,
                 build_location='aws_lambda/deploy_builds',
                 dependencies_subpath=''):
        self.lf_n = lambda_name
        self.lf_v = lambda_data
        self.build_location = build_location
        self.deps_folder = dependencies_subpath
        self.lf_build_loc = os.path.join(self.build_location, self.lf_n)
        self.ls_build_deps = os.path.join(self.lf_build_loc, self.deps_folder)
        self.lambda_function = self.lf_v['function_module']
        self.internal_dependencies = lf_v['dependencies']['internal']
        self.external_dependencies = " ".join(lf_v['dependencies']['external'])
        self.deploy_package = os.path.join(self.build_location,
                                           f'{self.lf_n}_deploy_package')
        self.s3_client = boto3.client('s3')

    def __setup_build_loc(self):
        os.makedirs(self.ls_build_deps, exist_ok=True)

    def __retrieve_lambda_function(self):
        shutil.copy(self.lambda_function,
                    os.path.join(self.lf_build_loc,
                                 os.path.split(self.lambda_function)[-1]))

    def __retrieve_internal_dependencies(self):
        for dep_internal in self.internal_dependencies:
            # Not explicitly tested for nested dependencies but outside scope of
            # requirements
            shutil.copytree(dep_internal, os.path.join(self.ls_build_deps,
                                                       dep_internal))

    def __retrieve_external_dependencies(self):
        pip_cmd = f"pip install {self.external_dependencies} -t {self.ls_build_deps}"
        os.system(pip_cmd)

    def __zip(self):
        shutil.make_archive(self.deploy_package,
                            'zip', root_dir=self.lf_build_loc)

    def __upload(self):
        bucket = self.lf_v['s3']['bucket']
        obj = self.lf_v['s3']['out_object']
        AwsS3()._upload_file(self.deploy_package + '.zip', bucket, obj)

    def __teardown_build_loc(self):
        shutil.rmtree(self.build_location)

    def deploy(self):
        # This is ugly...
        self.__setup_build_loc()
        self.__retrieve_lambda_function()
        self.__retrieve_internal_dependencies()
        self.__retrieve_external_dependencies()
        self.__zip()
        self.__upload()
        self.__teardown_build_loc()


if __name__ == '__main__':

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    lf_client = boto3.client('lambda')

    for lf_n, lf_v in LAMBDA_FUNCTIONS.items():
        # == Send deploy package to S3
        logging.info(f'Deploying {lf_n} lambda function')
        ld = AwsLambdaDeploy(lf_n, lf_v)
        ld.deploy()

        # == Update layer
        layer_rsp = lf_client.publish_layer_version(
                LayerName=lf_v['function']['layer_name'],
                Content={
                    'S3Bucket': lf_v['s3']['bucket'],
                    'S3Key': lf_v['s3']['out_object'],
                },
                CompatibleRuntimes=[
                        lf_v['function']['runtime']
                ]
            )

        # Retrieve latest version of the layer for use
        layer_details = lf_client.list_layer_versions(
            LayerName=lf_v['function']['layer_name']
        )
        latest_layer_arn = \
            layer_details['LayerVersions'][0]['LayerVersionArn']

        for fn in lf_client.list_functions()['Functions']:
            # If the function exists, update it. If not (else statement),
            # create it.
            if fn['FunctionName'] == lf_v['function']['function_name']:
                # == Update function code
                rsp_cup = lf_client.update_function_code(
                    FunctionName=lf_v['function']['function_name'],
                    S3Bucket=lf_v['s3']['bucket'],
                    S3Key=lf_v['s3']['out_object']
                )

                # == Update function configuration
                rsp_nup = lf_client.update_function_configuration(
                    FunctionName=lf_v['function']['function_name'],
                    Runtime=lf_v['function']['runtime'],
                    Role=lf_v['function']['role'],
                    Handler=lf_v['function']['handler'],
                    Timeout=lf_v['function']['timeout'],
                    MemorySize=lf_v['function']['memory'],
                    Environment={
                        'Variables': lf_v['function']['env_vars']
                    },
                    Layers=[latest_layer_arn])
                break
        else:
            rsp_create = lf_client.create_function(
                FunctionName=lf_v['function']['function_name'],
                Runtime=lf_v['function']['runtime'],
                Role=lf_v['function']['role'],
                Handler=lf_v['function']['handler'],
                Code={
                    'S3Bucket': lf_v['s3']['bucket'],
                    'S3Key': lf_v['s3']['out_object']
                },
                Timeout=lf_v['function']['timeout'],
                MemorySize=lf_v['function']['memory'],
                Environment={
                    'Variables': lf_v['function']['env_vars']
                },
                Layers=[latest_layer_arn]
            )
