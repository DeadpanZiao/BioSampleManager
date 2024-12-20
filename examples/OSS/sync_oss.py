from BSM.OSSConnector.OSSConnector import ZJlabOSSConnector

if __name__ == "__main__":
    access_key =''
    secret = ''
    endpoint_url = ''
    connector = ZJlabOSSConnector(access_key, secret, endpoint_url)

    bucket_name = ''
    local_dir = ''
    remote_dir = ''
    connector.sync_folder(bucket_name, local_dir, remote_dir)