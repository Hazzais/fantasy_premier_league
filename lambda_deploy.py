import os
import shutil

import boto3

from fpltools.utils import AwsS3

# TODO: logging
# TODO: testing (me)
# TODO: testing (unit tests etc.)
# TODO: automate lambda update on AWS
# TODO: move class outside of file?
# TODO: document

LAMBDA_FUNCTIONS = {'Extract':
                        {'function_module':'aws_lambda/aws_lambda_extract.py',
                         'dependencies':
                             {'internal': ['fpltools'],
                              'external': ['requests']
                              },
                         's3':
                             {'bucket': 'fpl-alldata',
                              'out_object': 'lambda_layers/live_fpl_extract.zip'}
                         }
                    }


class AwsLambdaDeploy:

    def __init__(self, lambda_name, lambda_data,
                 build_location='aws_lambda/deploy_builds',
                 dependencies_subpath='python/lib/site-packages'):
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
        self.__setup_build_loc()
        self.__retrieve_lambda_function()
        self.__retrieve_internal_dependencies()
        self.__retrieve_external_dependencies()
        self.__zip()
        self.__upload()
        self.__teardown_build_loc()

if __name__ == '__main__':
    for lf_n, lf_v in LAMBDA_FUNCTIONS.items():
        ld = AwsLambdaDeploy(lf_n, lf_v)
        ld.deploy()
