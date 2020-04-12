import os
import pytest

from lambda_deploy import AwsLambdaDeploy


def test_ld_create_build_folder_no_exists(tmp_path):
    a = AwsLambdaDeploy(deploy_build_root=tmp_path / 'no_exist_folder')
    with pytest.raises(FileNotFoundError) as e:
        a._create_build_folder('tmp', build_folder=None)


def test_ld_create_build_folder_works_cls(tmp_path):
    a = AwsLambdaDeploy(deploy_build_root=tmp_path)
    a._create_build_folder('should_be_made')
    assert os.path.exists(os.path.join(tmp_path, 'should_be_made'))


def test_ld_create_build_folder_works_passed(tmp_path):
    a = AwsLambdaDeploy()
    a._create_build_folder('should_be_made', build_folder=tmp_path)
    assert os.path.exists(os.path.join(tmp_path, 'should_be_made'))
