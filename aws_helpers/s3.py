import gzip
import os
import re
import tempfile
from contextlib import contextmanager

import boto3
from botocore.config import Config as S3Config

S3_SPLIT = re.compile("^s3://([^/]+)/(.*)$")


class S3:
    def __init__(self, config):
        if not config:
            config = {}
        self.config = S3Config(**config)
        self.s3c = boto3.session.Session().resource("s3", config=self.config)

    def split(self, path):
        """Split path to bucket, key

        bucket, key = s3.split("s3://my_bucket/my/key")
        bucket # my_bucket
        key # my/key

        """
        return S3_SPLIT.match(path).groups()

    def list(self, bucket, prefix):
        return self.s3c.Bucket(bucket).objects.filter(Prefix=prefix)

    @contextmanager
    def get(self, bucket=None, key=None, path=None, compressed=None, decoder=None):
        """Stream content from s3

        s = S3()
        bucket="my_bucket"
        key="my/path/my_compressed_file.json.gz
        with s3.get(
            bucket, key,
            compressed=True,
            decoder=simplejson.dumps
        ) as f:
            for row in f:
                print(f) #will print python object
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
            stream = body

        if decoder:
            yield map(decoder, stream)
        else:
            yield stream

        stream.close()

    @contextmanager
    def put(self, bucket=None, key=None, path=None, compressed=None, encoder=None):
        """Stream content to s3

        s = S3()
        bucket="my_bucket"
        key="my/path/my_compressed_file.json.gz
        with s3.put(
            bucket, key,
            compressed=True,
            encoder=simplejson.dumps
        ) as f:
            f.write(myDict)


        """
        if path:
            bucket, key = self.split(path)

        if not bucket:
            raise KeyError("bucket")

        if not key:
            raise KeyError("key")

        if compressed is None:
            compressed = key.endswith(".gz")

        def patchWriterIfNeed(fh):
            if encoder:
                _write = fh.write
                fh.write = lambda data: _write(encoder(data).encode() + b"\n")
            return fh

        with tempfile.TemporaryFile() as f:
            if compressed:
                with gzip.open(f, "wb") as fz:
                    yield patchWriterIfNeed(fz)
            else:
                yield patchWriterIfNeed(f)

            f.seek(0)

            self.s3c.Bucket(bucket).upload_fileobj(f, Key=key)
