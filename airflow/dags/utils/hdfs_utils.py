from hdfs import InsecureClient
from dotenv import load_dotenv
import os

load_dotenv('/opt/airflow/.env')

class HDFSManager:

    def __init__(self):
        self.__name_node_url = os.getenv('HDFS_URL')
        self.__user = os.getenv('HDFS_USER')

        assert self.__name_node_url is not None
        assert self.__user is not None

        self._client = InsecureClient(self.__name_node_url, user=self.__user)

    def exists(self, path):
        """Check if a path exists in HDFS"""
        return self._client.status(path, strict=False) is not None

    def list_files(self, path):
        """List all files/directories in a given path"""
        return self._client.list(path)

    def mkdirs(self, path):
        """Create directory including parent directories if needed"""
        return self._client.makedirs(path)

    def delete(self, path, recursive=False):
        """Delete a file or directory"""
        return self._client.delete(path, recursivse=recursive)

    def copy_from_local(self, local_path, hdfs_path, overwrite=True):
        """Upload file from local system to HDFS"""
        return self._client.upload(hdfs_path, local_path, overwrite=overwrite)

    def copy_to_local(self, hdfs_path, local_path, overwrite=True):
        """Download file from HDFS to local system"""
        return self._client.download(hdfs_path, local_path, overwrite=overwrite)

    def read_file(self, file_path):
        """Read file contents as string"""
        with self._client.read(file_path) as reader:
            return reader.read()

    def write_file(self, file_path, content):
        """Write string content to a file"""
        with self._client.write(file_path, overwrite=True) as writer:
            writer.write(content)

    def get_size(self, path):
        """Get size of a file in bytes"""
        status = self._client.status(path)
        return status['length']





