import logging
import os
import oss2
import sys
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZJlabOSSConnector:
    def __init__(self, access_key, secret, endpoint):
        self.auth = oss2.Auth(access_key, secret)
        self.endpoint = endpoint

    # Other methods remain unchanged...
    def upload_file(self, bucket_name, remote_fp, local_fp, progress_callback=None):
        """Upload a file to OSS with optional progress callback."""
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        with open(local_fp, 'rb') as fileobj:
            # Use the put_object method with the progress_callback parameter.
            result = bucket.put_object(remote_fp, fileobj, progress_callback=progress_callback)
        return result
    def sync_folder(self, bucket_name, local_dir, remote_dir):
        """
        Sync a local directory with a directory on OSS using object_exists to check for file existence.
        :param bucket_name: The name of the bucket on OSS
        :param local_dir: Path to the local directory
        :param remote_dir: Path to the remote directory (excluding bucket name)
        """
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)

        # Traverse the local directory and sync files
        for root, dirs, files in os.walk(local_dir):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_path, local_dir).replace('\\', '/')
                remote_path = f'{remote_dir}/{relative_path}'

                try:
                    exist = bucket.object_exists(remote_path)
                except oss2.exceptions.NoSuchKey:
                    exist = False
                except oss2.exceptions.NoSuchBucket as e:
                    logging.error(f'Bucket {bucket_name} does not exist or is inaccessible: {e}')
                    continue
                except Exception as e:
                    logging.debug(f'Error checking existence of {relative_path}: {e}')
                    exist = False

                if not exist:
                    logging.info(f'New file {relative_path}. Uploading...')
                    try:
                        # Pass the percentage function as the progress_callback.
                        self.upload_file(bucket_name, remote_path, local_path, progress_callback=self.percentage)
                        logging.info(f'Successfully uploaded {relative_path}')
                    except Exception as e:
                        logging.error(f'Failed to upload {relative_path}: {e}')
                else:
                    logging.info(f'File {relative_path} already exists. Skipping...')

    @staticmethod
    def percentage(consumed_bytes, total_bytes):
        """Callback function for showing upload progress."""
        if total_bytes:
            rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
            print('\r{0}% '.format(rate), end='')
            sys.stdout.flush()
