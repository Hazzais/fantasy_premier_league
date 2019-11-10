import os
import shutil

import boto3

from fpltools.utils import AwsS3

LAMBDA_FUNCTIONS = {'Extract':
                        {'function_module':
                             'aws_lambda/aws_lambda_extract.py',
                         'dependencies':
                             {'internal':
                                  ['fpltools'],
                              'external':
                                  ['requests'],
                              },
                         's3':
                             {'bucket': 'fpl-alldata',
                              'out_object': 'lambda_layers/live_fpl_extract.zip'}}
                    }

BUILD_LOCATION = 'aws_lambda/deploy_builds'

for lf_n, lf_v in LAMBDA_FUNCTIONS.items():
    print(lf_n)
    lf_build_loc = os.path.join(BUILD_LOCATION, lf_n)
    ls_build_deps = os.path.join(lf_build_loc, 'python', 'lib', 'site-packages')
    os.makedirs(ls_build_deps, exist_ok=True)

    lambda_func = lf_v['function_module']
    shutil.copy(lambda_func, os.path.join(lf_build_loc, os.path.split(lambda_func)[-1]))

    for dep_internal in lf_v['dependencies']['internal']:
        # Not explicitly tested for nested dependencies but outside scope of
        # requirements
        shutil.copytree(dep_internal, os.path.join(ls_build_deps, dep_internal))

    dep_list_external = " ".join(lf_v['dependencies']['external'])
    pip_cmd = f"pip install {dep_list_external} -t {ls_build_deps}"
    os.system(pip_cmd)

    deploy_package = os.path.join(BUILD_LOCATION,
                                     f'{lf_n}_deploy_package')
    shutil.make_archive(deploy_package,
                        'zip', root_dir=lf_build_loc)

    s3_client = boto3.client('s3')

    bucket = lf_v['s3']['bucket']
    obj = lf_v['s3']['out_object']

    AwsS3()._upload_file(deploy_package + '.zip', bucket, obj)

    shutil.rmtree(BUILD_LOCATION)




class AwsLambdaDeploy:

    def __init__(self,
                 versioning=False,
                 lambda_folder='aws_lambda',
                 deploy_build_root='aws_lambda/deploy_builds'):
        self.__versioning = versioning
        self.__lambda_folder = lambda_folder
        self.__deploy_build_root = deploy_build_root

    def build(self, lambda_function, out_name):
        """Wrapper to build a zip folder (i.e. the deployment package)"""
        self._create_build_folder(lambda_function)
        imports = self._get_imports(lambda_function)
        requirements = self._get_requirements(imports)
        self._install_requirements(requirements)
        self._copy_local()
        self._zip_build()
        self._teardown_build_folder()
        pass

    def deploy(self, build_folder, bucket, file):
        pass

    def _create_build_folder(self, lambda_function, build_folder=None):
        if build_folder is None:
            build_folder = self.__deploy_build_root

        if not os.path.exists(build_folder):
            raise FileNotFoundError("Build folder does not exist")

        os.mkdir(os.path.join(build_folder, lambda_function.split('.')[0]))

    def _get_imports(self, lambda_function):
        pass

    def _get_requirements(self, imports):
        pass

    def _install_requirements(self, requirements):
        pass

    def _copy_local(self):
        pass

    def _zip_build(self):
        pass

    def _teardown_build_folder(self):
        pass

# For each lambda function in folder, get imports, create build folder
# Pip install imports in that folder
# Zip
# Teardown build folder
if __name__ == '__main__':
    pass
