import json
from io import BytesIO

from minio.error import S3Error


class MinioWrapper:
    def __init__(self, minio_client):
        self.client = minio_client

    def get_json(self, bucket, object_name):
        from minio import S3Error
        try:
            response = self.client.get_object(bucket, object_name)
            object_data = response.read().decode('utf-8')
            response.close()
            response.release_conn()
            return json.loads(object_data)
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return None
            raise err

    def object_exists(self, bucket_name, object_name):
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return False
            raise

    def list_objects(self, bucket, prefix=None, recursive=False):
        try:
            return self.client.list_objects(bucket, prefix=prefix, recursive=recursive)

        except S3Error as err:
            print(f"Error occurred: {err}")

    def put_json(self, bucket, object_name, json_obj):
        data_bytes = BytesIO(json.dumps(json_obj, indent=2).encode("utf-8"))
        _len = data_bytes.getbuffer().nbytes
        self.client.put_object(bucket, object_name, data_bytes, length=_len, content_type="application/json")
        obj = self.client.stat_object(bucket, object_name)
        if obj.object_name != object_name:
            raise Exception("pud data error")

    def create_bucket(self, bucket_name):
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
