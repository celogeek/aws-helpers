import gzip
import io
import re
from contextlib import contextmanager

import boto3
from botocore.config import Config as S3Config

S3_SPLIT = re.compile("^s3://([^/]+)/(.*)$")


class S3:
    """Helper for aws s3 using boto3

    This module is design to work nice with python3

    Attributes:
        config (botocore.config.Config): configuration for s3
        boto3_session (boto3.session.Session): session of boto3
    """

    def __init__(self, config=None):
        """Initialize instance of S3 helper

        Args:
            config(dict, optional): configuration pass to boto3 s3 resource

        Examples:

            s3 = S3({"proxies":{"http":"proxy:8888", "https":"proxy:8888"}})

            s3 = S3({"region_name":"us-east-1"})

        """
        if not config:
            config = {}

        self.config = S3Config(**config)
        self.boto3_session = boto3.session.Session()
        self.s3c = self.boto3_session.resource("s3", config=self.config)

    def split(self, path):
        """Split path to bucket, key

        Args:
            path (str): s3 string to split into a bucket and prefix

        Returns:
            (bucket, prefix)


        Examples:
            bucket, key = s3.split("s3://my_bucket/my/key")

            bucket
            # my_bucket

            key
            # my/key


        """
        return S3_SPLIT.match(path).groups()

    def list(self, bucket=None, prefix=None, path=None):
        """List object of a path recursively

        Args:
            bucket (str, optional): bucket where to connect
            prefix (str, optional): prefix to list
            path (str, optional): full path where to connect and filter

        Require:
            You need to pass either (bucket, prefix) or (path)

        Returns:
            s3.Bucket.objectsCollection(s3.Bucket, s3.ObjectSummary)

        Examples:

            mykeys = S3().list(bucket, prefix)

            mykeys = S3().list(path="s3://bucket/prefix")

        """
        if path:
            bucket, prefix = self.split(path)

        return self.s3c.Bucket(bucket).objects.filter(Prefix=prefix)

    @contextmanager
    def get(self, bucket=None, key=None, path=None, compressed=None, decoder=None):
        """Stream content from s3 file

        This method support uncompressing and decoding on the fly. It is a context method.

        Args:
            bucket (str, optional): bucket to read from
            key (str, optional): key to read from
            path (str, optional): full path of s3 file to read from
            compressed (bool, optional): uncompress the content on the fly
            decoder (lambda, optional): decode the content on the fly

        Require:
            You need to pass either (bucket, prefix) or (path)

        If compressed is None, it will be set to True if the key ends with ".gz", False otherwise

        If a decoder is set, it will be use to decode the content on the fly

        Example:

            s = S3()
            bucket="my_bucket"
            key="my/path/my_compressed_file.json.gz
            with s3.get(bucket, key, decoder=simplejson.dumps) as f:
                for row in f:
                    print(f) #will print python object (dict)

        Raise:
            KeyError: if bucket or key is missing, or path not defined to fill them
        """

        if path:
            bucket, key = self.split(path)

        if not bucket:
            raise KeyError("bucket")

        if not key:
            raise KeyError("key")

        if compressed is None:
            compressed = key.endswith(".gz")

        s3Response = self.s3c.ObjectSummary(bucket, key).get()
        body = s3Response["Body"]

        if compressed:
            stream = gzip.open(body)
        else:
            stream = io.BytesIO(body.read())

        if decoder:
            yield map(decoder, stream)
        else:
            yield stream

        stream.close()

    @contextmanager
    def put(self, bucket=None, key=None, path=None, compress=None, encoder=None):
        """Stream content to s3 file

        This method support compressing and encoding on the fly. It is a context method.

        Args:
            bucket (str, optional): bucket to read from
            key (str, optional): key to read from
            path (str, optional): full path of s3 file to read from
            compress (bool, optional): compress the content on the fly
            encoder (lambda, optional): encode the content on the fly

        Require:
            You need to pass either (bucket, prefix) or (path)

        If compress is None, it will be set to True if the key ends with ".gz", False otherwise

        If a encoder is set, it will be use to encode the content on the fly. After each line and "\n" will be insert.

        s = S3()
        bucket="my_bucket"
        key="my/path/my_compressed_file.json.gz
        with s3.put(bucket, key, encoder=simplejson.dumps) as f:
            f.write(myDict)


        """
        if path:
            bucket, key = self.split(path)

        if not bucket:
            raise KeyError("bucket")

        if not key:
            raise KeyError("key")

        if compress is None:
            compress = key.endswith(".gz")

        def patchWriterIfNeed(fh):
            if encoder:
                _write = fh.write
                fh.write = lambda data: _write(encoder(data).encode() + b"\n")
            return fh

        with io.BytesIO() as f:
            if compress:
                with gzip.open(f, "wb") as fz:
                    yield patchWriterIfNeed(fz)
            else:
                yield patchWriterIfNeed(f)

            f.seek(0)

            self.s3c.Bucket(bucket).upload_fileobj(f, Key=key)
