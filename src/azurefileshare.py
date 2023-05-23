]]]]]import os
import logging
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient
from azure.core.exceptions import ResourceExistsError, AzureError
from azure.identity import DefaultAzureCredential
from src.common import logger_config, log_function_call, check_path_exist, generate_file_name, convert_to_realpath


logger_config()


class AzureFileShareHandler():
    """
    Class to handle Azure file share
    """
    
    def __init__(self):
        self.fileshare_connstr = os.getenv("AZURE_STORAGE_ACCOUNT")  # TODO: this is not correct
        self.fileshare_name = os.getenv("AZURE_FILESHARE_NAME")
        if os.getenv("AZURE_STORAGE_ACCOUNT_KEY") is not None:
            self.storageaccount_cred = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        else:
            self.storageaccount_cred = DefaultAzureCredential()
        
        self.fileshare_client: ShareFileClient = self.create_fileshare_client()
        self.directory_client: ShareDirectoryClient = self.create_directory_client()

    @property
    @log_function_call
    def create_fileshare_client(self, file_path=''):
        """
        Establish file client base on which type of object we are interacting with

        file_path: path to file

        return: fileshare_client
        """
        try:
            self.fileshare_client = ShareFileClient.from_connection_string(conn_str=self.fileshare_connstr, share_name=self.fileshare_name, file_path=file_path, credential=self.storageaccount_cred)
            return self.fileshare_client
        except AzureError as err:
            logging.critical(err)
            raise err

    @property
    @log_function_call
    def create_directory_client(self, file_path='./'):
        """
        Establish file client base on which type of object we are interacting with

        file_path: path to file

        return: fileshare_client
        """
        try:
            self.directory_client = ShareDirectoryClient.from_connection_string(conn_str=self.fileshare_connstr, share_name=self.fileshare_name, file_path=file_path,
                                                                                credential=self.storageaccount_cred)
            return self.directory_client
        except AzureError as err:
            logging.critical(err)
            raise err
    
    @classmethod
    @log_function_call
    def _validate_folder_path(folder_path):
        """
        Validate folder path
        
        folder_path: folder path to validate
        
        return: folder_path
        """
        if folder_path.startswith('./'):
            return folder_path
        else:
            return f"./{folder_path}"

    @property
    @log_function_call
    def create_subdir(self, folder_path):
        """
        Create subdirectory in Azure file share
        
        folder_path: folder path to create
        
        return: folder_path
        """
        dir_path = self._validate_folder_path(folder_path)
        path_combine = ''
        for folder in dir_path.split('/'):
            path_combine += f"{folder}/"
            if path_combine == './':
                continue
            try:
                self.directory_client.create_subdirectory(path_combine)
            except ResourceExistsError as err:
                if err.status_code == '409':
                    logging.warning(err.message)
                    continue
        return path_combine

    @log_function_call
    def download_file(self, cloud_source_file, local_dest_file):
        """
        Download from file from Azure file share
        
        cloud_source_file: file path in Azure file share
        local_dest_file: file path in local
        
        return: local_dest_file
        """
        # TODO: check if file exist for download
        dest_path = check_path_exist(local_dest_file, avoid_duplicate=True)
        self.create_fileshare_client(file_path=cloud_source_file)
        try:
            with open(dest_path, 'w+b') as file_handle:
                data = self.fileshare_client.download_file()
                data.readinto(file_handle)
            return dest_path
        except Exception as err:
            raise Exception(err)

    @log_function_call
    def upload_file(self, cloud_dest_file, local_source_file):
        """
        Upload local file to AzureFileShare
        
        cloud_dest_file: file path in Azure file share
        local_source_file: file path in local
        
        return: cloud_dest_file
        """
        dest_path_parts = os.path.split(cloud_dest_file)
        self.create_subdir(dest_path_parts[0])
        self.create_fileshare_client(file_path=cloud_dest_file)
        absoulute_local_filepath = convert_to_realpath(local_source_file)
        if check_path_exist(absoulute_local_filepath):
            with open(absoulute_local_filepath, "rb") as data:
                try:
                    self.fileshare_client.upload_file(data)
                    return cloud_dest_file
                # TODO: ISSUES: upload file using ShareFileClient does not throw 409
                # Which make the file overwritten
                except ResourceExistsError as err:
                    if err.status_code == '409':
                        logging.warning(err.message)
                        new_cloud_dest_file = generate_file_name(cloud_dest_file)
                        self.fileshare_client.create_file(new_cloud_dest_file)
                        self.fileshare_client.upload_file(data)
                        return new_cloud_dest_file
        else:
            logging.error(f"local source file not exist: {absoulute_local_filepath}")
            raise FileNotFoundError(f"local source file not exist: {absoulute_local_filepath}")
