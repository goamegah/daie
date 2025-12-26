# tests/elt/raw_file_load_test.py
from unittest.mock import patch, MagicMock, call
import pytest
from daie.jobs.elt import raw_file_load


def test_move_files_with_single_file():
    # Given: a source directory with one file and a destination directory
    src = "abfss://transient@storage.dfs.core.windows.net/source/data"
    dst = "abfss://bronze@storage.dfs.core.windows.net/raw/data"
    mock_file = MagicMock()
    mock_file.path = f"{src}/file1.csv"
    
    mock_dbutils = MagicMock()
    mock_dbutils.fs.ls.return_value = [mock_file]
    
    # When: calling move_files with mocked dbutils
    with patch('daie.jobs.elt.raw_file_load.su.get_dbutils', return_value=mock_dbutils):
        raw_file_load.move_files(src, dst)
    
    # Then: destination directory should be created and file should be moved
    mock_dbutils.fs.mkdirs.assert_called_once_with(dst)
    mock_dbutils.fs.ls.assert_called_once_with(src)
    mock_dbutils.fs.mv.assert_called_once_with(
        f"{src}/file1.csv",
        f"{dst}/file1.csv",
        recurse=True
    )


def test_move_files_with_multiple_files():
    # Given: a source directory with multiple files
    src = "abfss://transient@storage.dfs.core.windows.net/source/data"
    dst = "abfss://bronze@storage.dfs.core.windows.net/raw/data"
    
    mock_file1 = MagicMock()
    mock_file1.path = f"{src}/file1.csv"
    mock_file2 = MagicMock()
    mock_file2.path = f"{src}/file2.parquet"
    mock_file3 = MagicMock()
    mock_file3.path = f"{src}/file3.json"
    
    mock_dbutils = MagicMock()
    mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2, mock_file3]
    
    # When: calling move_files with multiple files
    with patch('daie.jobs.elt.raw_file_load.su.get_dbutils', return_value=mock_dbutils):
        raw_file_load.move_files(src, dst)
    
    # Then: all files should be moved to destination
    mock_dbutils.fs.mkdirs.assert_called_once_with(dst)
    assert mock_dbutils.fs.mv.call_count == 3
    expected_calls = [
        call(f"{src}/file1.csv", f"{dst}/file1.csv", recurse=True),
        call(f"{src}/file2.parquet", f"{dst}/file2.parquet", recurse=True),
        call(f"{src}/file3.json", f"{dst}/file3.json", recurse=True)
    ]
    mock_dbutils.fs.mv.assert_has_calls(expected_calls, any_order=False)


def test_move_files_with_empty_directory():
    # Given: an empty source directory
    src = "abfss://transient@storage.dfs.core.windows.net/source/empty"
    dst = "abfss://bronze@storage.dfs.core.windows.net/raw/empty"
    
    mock_dbutils = MagicMock()
    mock_dbutils.fs.ls.return_value = []
    
    # When: calling move_files with empty directory
    with patch('daie.jobs.elt.raw_file_load.su.get_dbutils', return_value=mock_dbutils):
        raw_file_load.move_files(src, dst)
    
    # Then: destination directory should be created but no files moved
    mock_dbutils.fs.mkdirs.assert_called_once_with(dst)
    mock_dbutils.fs.ls.assert_called_once_with(src)
    mock_dbutils.fs.mv.assert_not_called()


def test_move_files_with_nested_path():
    # Given: a file with nested path structure
    src = "abfss://transient@storage.dfs.core.windows.net/source/nested/path"
    dst = "abfss://bronze@storage.dfs.core.windows.net/raw/nested/path"
    
    mock_file = MagicMock()
    mock_file.path = f"{src}/data.csv"
    
    mock_dbutils = MagicMock()
    mock_dbutils.fs.ls.return_value = [mock_file]
    
    # When: calling move_files with nested paths
    with patch('daie.jobs.elt.raw_file_load.su.get_dbutils', return_value=mock_dbutils):
        raw_file_load.move_files(src, dst)
    
    # Then: file should be moved preserving the filename
    mock_dbutils.fs.mv.assert_called_once_with(
        f"{src}/data.csv",
        f"{dst}/data.csv",
        recurse=True
    )


def test_main_with_valid_parameters():
    # Given: valid environment, source, and entity parameters
    env = "dev"
    source = "opendata"
    entity = "accident_v1"
    
    mock_metadata = {
        "source": source,
        "entity": entity,
        "transient_dir": "abfss://transient@storage.dfs.core.windows.net/opendata/accident_v1"
    }
    mock_raw_base_path = "/Volumes/daie_chn_dev_bronze/dev_raw_opendata/accident_v1/raw_files"
    
    # When: calling main with mocked dependencies
    with patch('daie.jobs.elt.raw_file_load.ec.get_source_metadata', return_value=mock_metadata) as mock_get_metadata, \
         patch('daie.jobs.elt.raw_file_load.ec.get_or_create_volume_location_from_metadata', return_value=mock_raw_base_path) as mock_get_volume, \
         patch('daie.jobs.elt.raw_file_load.move_files') as mock_move_files:
        
        raw_file_load.main(env=env, source=source, entity=entity)
    
    # Then: metadata should be retrieved, volume location created, and files moved
    mock_get_metadata.assert_called_once_with(env=env, source=source, entity=entity)
    mock_get_volume.assert_called_once_with(
        env=env,
        metadata=mock_metadata,
        sub_folder="raw_files",
        stage="raw"
    )
    mock_move_files.assert_called_once_with(
        src=mock_metadata["transient_dir"],
        dst=mock_raw_base_path
    )


def test_main_with_additional_kwargs():
    # Given: valid parameters with additional unused kwargs
    env = "test"
    source = "external_api"
    entity = "users_v2"
    extra_param = "ignored_value"
    
    mock_metadata = {
        "source": source,
        "entity": entity,
        "transient_dir": "abfss://transient@storage.dfs.core.windows.net/external_api/users_v2"
    }
    mock_raw_base_path = "/Volumes/daie_chn_test_bronze/test_raw_external_api/users_v2/raw_files"
    
    # When: calling main with extra kwargs (should be ignored via **_)
    with patch('daie.jobs.elt.raw_file_load.ec.get_source_metadata', return_value=mock_metadata), \
         patch('daie.jobs.elt.raw_file_load.ec.get_or_create_volume_location_from_metadata', return_value=mock_raw_base_path), \
         patch('daie.jobs.elt.raw_file_load.move_files') as mock_move_files:
        
        raw_file_load.main(env=env, source=source, entity=entity, extra_param=extra_param)
    
    # Then: function should execute successfully ignoring extra parameters
    mock_move_files.assert_called_once()


def test_main_integration_flow():
    # Given: a complete integration scenario with all components
    env = "prod"
    source = "crm"
    entity = "customers_v1"
    
    mock_metadata = {
        "source": source,
        "entity": entity,
        "transient_dir": "abfss://transient@storage.dfs.core.windows.net/crm/customers_v1"
    }
    mock_raw_base_path = "/Volumes/daie_chn_prod_bronze/prod_raw_crm/customers_v1/raw_files"
    
    mock_file1 = MagicMock()
    mock_file1.path = f"{mock_metadata['transient_dir']}/customers_20231201.csv"
    mock_file2 = MagicMock()
    mock_file2.path = f"{mock_metadata['transient_dir']}/customers_20231202.csv"
    
    mock_dbutils = MagicMock()
    mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2]
    
    # When: executing the complete main flow
    with patch('daie.jobs.elt.raw_file_load.ec.get_source_metadata', return_value=mock_metadata), \
         patch('daie.jobs.elt.raw_file_load.ec.get_or_create_volume_location_from_metadata', return_value=mock_raw_base_path), \
         patch('daie.jobs.elt.raw_file_load.su.get_dbutils', return_value=mock_dbutils):
        
        raw_file_load.main(env=env, source=source, entity=entity)
    
    # Then: the complete flow should execute with files being moved
    mock_dbutils.fs.mkdirs.assert_called_once_with(mock_raw_base_path)
    assert mock_dbutils.fs.mv.call_count == 2


if __name__ == "__main__":
    # Debug section - run tests individually
    test_move_files_with_single_file()
    test_move_files_with_multiple_files()
    test_move_files_with_empty_directory()
    test_move_files_with_nested_path()
    test_main_with_valid_parameters()
    test_main_with_additional_kwargs()
    test_main_integration_flow()
    print("All tests passed!")
