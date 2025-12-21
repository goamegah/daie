# tests/devops/deploy_artifacts_test.py
from unittest.mock import patch, MagicMock
import pytest
from pathlib import Path
import sys

# Add deployment module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "deployment" / "devops"))
import deploy_artifacts


def test_upload_directory_to_volume_with_single_file(tmp_path):
    # Given: a local directory with a single file
    test_file = tmp_path / "test.yml"
    test_file.write_text("test: data")
    volume_path = "/Volumes/catalog/schema/volume"
    
    mock_workspace_client = MagicMock()
    
    # When: uploading the directory
    files_count = deploy_artifacts.upload_directory_to_volume(
        workspace_client=mock_workspace_client,
        local_dir=str(tmp_path),
        volume_path=volume_path
    )
    
    # Then: directory should be created and file uploaded
    mock_workspace_client.files.create_directory.assert_called()
    mock_workspace_client.files.upload.assert_called_once()
    assert files_count == 1


def test_upload_directory_to_volume_recursive(tmp_path):
    # Given: a local directory with nested structure
    (tmp_path / "source").mkdir()
    (tmp_path / "source" / "opendata").mkdir()
    file1 = tmp_path / "source" / "opendata" / "accidents_v1.yml"
    file1.write_text("source: opendata")
    
    volume_path = "/Volumes/catalog/artifacts/metadata/dev"
    mock_workspace_client = MagicMock()
    
    # When: uploading recursively
    files_count = deploy_artifacts.upload_directory_to_volume(
        workspace_client=mock_workspace_client,
        local_dir=str(tmp_path),
        volume_path=volume_path
    )
    
    # Then: nested directories should be created and file uploaded
    assert mock_workspace_client.files.create_directory.call_count >= 2
    assert files_count == 1


def test_upload_directory_to_volume_nonexistent_source():
    # Given: a non-existent source directory
    local_dir = "/path/that/does/not/exist"
    volume_path = "/Volumes/catalog/schema/volume"
    mock_workspace_client = MagicMock()
    
    # When/Then: should raise FileNotFoundError
    with pytest.raises(FileNotFoundError, match="n'existe pas"):
        deploy_artifacts.upload_directory_to_volume(
            workspace_client=mock_workspace_client,
            local_dir=local_dir,
            volume_path=volume_path
        )


def test_deploy_artifacts_metadata_to_dev():
    # Given: deployment parameters for metadata in dev
    artifact_type = "metadata"
    env = "dev"
    
    # When: deploying with mocked dependencies
    with patch('deploy_artifacts.WorkspaceClient') as mock_client_class, \
         patch('deploy_artifacts.upload_directory_to_volume', return_value=5) as mock_upload, \
         patch.dict('os.environ', {
             'DATABRICKS_HOST': 'https://test.azuredatabricks.net',
             'AZURE_CLIENT_ID': 'test-client-id',
             'AZURE_CLIENT_SECRET': 'test-secret',
             'AZURE_TENANT_ID': 'test-tenant-id'
         }):
        
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = MagicMock(user_name='test@example.com')
        mock_client_class.return_value = mock_client
        
        deploy_artifacts.deploy_artifacts(artifact_type, env)
    
    # Then: upload should be called with correct parameters
    mock_upload.assert_called_once()
    call_args = mock_upload.call_args[0]  # positional args
    assert call_args[1] == 'lakehouse/metadata'  # local_dir
    assert '/artifacts/metadata/dev' in call_args[2]  # volume_path


def test_deploy_artifacts_to_custom_environment_with_developer_name():
    # Given: deployment to custom environment with developer name
    artifact_type = "metadata"
    env = "feature_123"
    developer_name = "john"
    
    # When: deploying to custom environment
    with patch('deploy_artifacts.WorkspaceClient') as mock_client_class, \
         patch('deploy_artifacts.upload_directory_to_volume', return_value=3) as mock_upload, \
         patch.dict('os.environ', {
             'DATABRICKS_HOST': 'https://test.azuredatabricks.net',
             'AZURE_CLIENT_ID': 'test-client-id',
             'AZURE_CLIENT_SECRET': 'test-secret',
             'AZURE_TENANT_ID': 'test-tenant-id'
         }):
        
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = MagicMock(user_name='test@example.com')
        mock_client_class.return_value = mock_client
        
        deploy_artifacts.deploy_artifacts(artifact_type, env, developer_name)
    
    # Then: should deploy to developer-specific path
    call_args = mock_upload.call_args[0]
    assert f'/artifacts/metadata/{developer_name}' in call_args[2]  # volume_path


def test_deploy_artifacts_config_type():
    # Given: deployment of config artifact type
    artifact_type = "config"
    env = "prod"
    
    # When: deploying config without developer_name (defaults to 'dev' folder)
    with patch('deploy_artifacts.WorkspaceClient') as mock_client_class, \
         patch('deploy_artifacts.upload_directory_to_volume', return_value=2) as mock_upload, \
         patch.dict('os.environ', {
             'DATABRICKS_HOST': 'https://test.azuredatabricks.net',
             'AZURE_CLIENT_ID': 'test-client-id',
             'AZURE_CLIENT_SECRET': 'test-secret',
             'AZURE_TENANT_ID': 'test-tenant-id'
         }):
        
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = MagicMock(user_name='test@example.com')
        mock_client_class.return_value = mock_client
        
        deploy_artifacts.deploy_artifacts(artifact_type, env)
    
    # Then: should use config source directory and default to 'dev' folder
    call_args = mock_upload.call_args[0]
    assert call_args[1] == 'config'  # local_dir
    assert '/artifacts/config/dev' in call_args[2]  # volume_path (defaults to 'dev' folder)


def test_deploy_artifacts_missing_env_vars():
    # Given: missing environment variables
    artifact_type = "metadata"
    env = "dev"
    
    # When/Then: should raise ValueError
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(ValueError, match="Variables d'environnement manquantes"):
            deploy_artifacts.deploy_artifacts(artifact_type, env)


def test_deploy_artifacts_unknown_artifact_type():
    # Given: unknown artifact type
    artifact_type = "unknown_type"
    env = "dev"
    
    # When/Then: should raise ValueError
    with patch.dict('os.environ', {
             'DATABRICKS_HOST': 'https://test.azuredatabricks.net',
             'AZURE_CLIENT_ID': 'test-client-id',
             'AZURE_CLIENT_SECRET': 'test-secret',
             'AZURE_TENANT_ID': 'test-tenant-id'
         }):
        with pytest.raises(ValueError, match="Type d'artifact inconnu"):
            deploy_artifacts.deploy_artifacts(artifact_type, env)


if __name__ == "__main__":
    # Debug section
    import tempfile
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        test_upload_directory_to_volume_with_single_file(tmp_path)
        test_upload_directory_to_volume_recursive(tmp_path)
    
    test_upload_directory_to_volume_nonexistent_source()
    test_deploy_artifacts_metadata_to_dev()
    test_deploy_artifacts_to_custom_environment_with_developer_name()
    test_deploy_artifacts_config_type()
    test_deploy_artifacts_missing_env_vars()
    test_deploy_artifacts_unknown_artifact_type()
    
    print("All tests passed!")
