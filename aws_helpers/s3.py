"""S3 helper for boto3 on python3."""
from botocore.config import Config as S3Config
from contextlib import contextmanager
from multiprocessing import Process, cpu_count, Manager
from threading import Thread
from time import sleep
import boto3
import gzip
import io
import re

S3_SPLIT = re.compile("^s3://([^/]+)/(.*)$")


class S3:
    """Helpers for aws s3 using boto3.

    This module is design to work nice with python3

    Attributes:
        config (botocore.config.Config): configuration for s3
        boto3_session (boto3.session.Session): session of boto3

    """

    def __init__(self, config=None):
        """Initialize instance of S3 helper.

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
        """Split path to bucket, key.

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

    def list(self, bucket=None, prefix=None, path=None, format="object"):
        """List object of a path recursively.

        Args:
            bucket (str, optional): bucket where to connect
            prefix (str, optional): prefix to list
            path (str, optional): full path where to connect and filter
            format (str, optional): can be "object", "tuple", "full". Default to "object"

        Require:
            You need to pass either (bucket, prefix) or (path)

        Returns:
            if format == "tuple":
                iterator((bucket, key))
            elif format == "full":
                iterator("s3://{bucket}/{key}")
            else:
                s3.Bucket.objectsCollection(s3.Bucket, s3.ObjectSummary)

        Examples:
            mykeys = S3().list(bucket, prefix)
            mykeys = S3().list(path="s3://bucket/prefix")

        """
        if path:
            bucket, prefix = self.split(path)

        resp = self.s3c.Bucket(bucket).objects.filter(Prefix=prefix)

        if format == "tuple":
            for s3file in resp:
                yield s3file.bucket_name, s3file.key
        elif format == "full":
            for s3file in resp:
                yield "s3://{}/{}".format(s3file.bucket_name, s3file.key)
        else:
            for s3file in resp:
                yield s3file

    def dir(self, bucket=None, prefix=None, path=None):
        """Get the subdirectory of s3 path.

        Args:
            bucket (str, optional): bucket where to connect
            prefix (str, optional): prefix to list
            path (str, optional): full path where to connect and filter

        Require:
            You need to pass either (bucket, prefix) or (path)

        Returns:
            iterator(sub path of bucket / prefix)

        Examples:
            for d in S3().dir(bucket, prefix):
                print(d)

            # let say you have this :
            # s3://test/my/path/dir1
            # s3://test/my/path/dir2
            # s3://test/my/path/dir3

            list("test, "by/path/")
            # ["dir1", "dir2", "dir3"]

        """
        if path:
            bucket, prefix = self.split(path)

        for result in self.s3c.meta.client.list_objects(
            Bucket=bucket, Prefix=prefix, Delimiter="/"
        ).get("CommonPrefixes"):
            yield result["Prefix"][len(prefix):-1]

    @contextmanager
    def get(self, bucket=None, key=None, path=None, compressed=None, decoder=None):
        """Stream content from s3 file.

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
            key="my/path/my_compressed_file.json.gz"
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

        stream = None
        while not stream:
            try:
                s3Response = self.s3c.ObjectSummary(bucket, key).get()
                stream = io.BytesIO(s3Response["Body"].read())
            except Exception as e:
                print(e)
                print("Retrying ...")
                sleep(3)

        if compressed:
            stream = gzip.open(stream)

        if decoder:
            yield map(decoder, stream)
        else:
            yield stream

        stream.close()

    @contextmanager
    def put(self, bucket=None, key=None, path=None, compress=None, encoder=None):
        r"""Stream content to s3 file.

        This method support compressing and encoding on the fly. It is a context method.

        Args:
            bucket (str, optional): bucket to read from
            key (str, optional): key to read from
            path (str, optional): full path of s3 file to read from
            compress (bool, optional): compress the content on the fly
            encoder (lambda, optional): encode the content on the fly

        Require:
            You need to pass either (bucket, prefix) or (path)

        Defaults:
            If compress is None, it will be set to True if the key ends with ".gz", False otherwise
            If a encoder is set, it will be use to encode the content on the fly. After each line and "\n" will be insert.

        Example:
            s = S3()
            bucket="my_bucket"
            key="my/path/my_compressed_file.json.gz"
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

    def touch(self, bucket=None, key=None, path=None):
        """Create and empty file.

        Args:
            bucket (str, optional): bucket to read from
            key (str, optional): key to read from
            path (str, optional): full path of s3 file to read from

        Require:
            You need to pass either (bucket, prefix) or (path)

        Example:
            s = S3()
            bucket="my_bucket"
            key="my/path/_SUCCESS"
            s3.touch(bucket, key)
        """
        if path:
            bucket, key = self.split(path)

        if not bucket:
            raise KeyError("bucket")

        if not key:
            raise KeyError("key")

        self.s3c.Bucket(bucket).upload_fileobj(io.BytesIO(), Key=key)


class S3EndOfIteration:
    """Class to indicate the end of iteration."""


class S3StreamWorker(Process):
    """Worker for S3Streamer."""

    def __init__(self, q_in, q_out, func, func_params, s3config):
        """Initialize the worker for s3.

        Args:
            q_in(Queue): queue that contain the file to process
            q_out(Queue): queue for the result
            func(lambda): function to call when a file is readed from the q_in
            func_params(any, optional): additional params. if None, it will not be pass to the worker
            s3config: config s3 for the s3 connexion initialization
        """
        Process.__init__(self)
        self.daemon = True
        self.q_in = q_in
        self.q_out = q_out
        self.func = func
        self.func_params = func_params
        self.s3config = s3config

    def run(self):
        """Daemon run loop."""
        s3 = S3(self.s3config)
        while True:
            s3files = self.q_in.get()
            if isinstance(s3files, S3EndOfIteration):
                self.q_out.put(S3EndOfIteration())
                break
            try:
                args = [s3, s3files]
                if self.func_params is not None:
                    args.append(self.func_params)

                for result in self.func(*args):
                    self.q_out.put(result)

            except Exception as e:
                self.q_out.put(e)
                raise(e)


class S3Stream:
    """S3 Streamer.

    This will manager boto3 connexion in different sub process.

    It is like Pool.imap_unordered, with inializer for boto3.

    """

    def __init__(
        self,
        bucket=None,
        prefix=None,
        path=None,
        s3config=None,
        nb_workers=None,
        func=None,
        func_iter=None,
        func_params=None,
        bulk_size=None,
        spread_last_bulk=True
    ):
        """Initialize the streamer.

        Args:
            bucket (str, optional): bucket to read from
            prefix (str, optional): prefix to read from
            path (str, optional): full path with bucket and prefix
            s3config (dict, optional): config for S3
            nb_workers (int, optional): nb worker to use for processing. default cpu_count * 4
            func (lambda): function that will receive the s3 path to process
            func_iter (iterator, optional): iterator to pass to func, if missing, use bucket, prefix to fill it
            func_params(any, optional): additional params. if None, it will not be pass to the worker
            bulk_size (int, optional): will send bulk_size number of files to process to the worker
            spread_last_bulk (boolean): will spread the last bulk if last bulk size < bulk_size into the other bulk

        Require:
            You need to pass either (bucket, prefix) or (path)

        Example:

            def decode(s3, s3file):
              with s3.get(path=s3file, decoder=simplejson.loads) as f:
                for js in f:
                  yield js["..."]

            for msg in S3Stream(path="s3://...", func=decode):
                # do something with msg

            # you can also give a list of s3 file to process
            s3files = list("{}/{}".format(s.bucket_name, s.key) for s in S3().list(path="...") is "/k=28/" in s.key)
            for msg in S3Stream(func=decode, func_iter=s3files):
               # do something

            # for bulk processing file

            def decode(s3, s3files):
              results = {}
              for s3file in s3files:
                  with s3.get(path=s3file, decoder=simplejson.loads) as f:
                    for js in f:
                      results[...]=...
              yield results

            for results in S3Stream(path="s3://...", func=decode, bulk_size=32):
                # do something with msg


        """
        if not func_iter:
            s3 = S3(s3config)
            bucket = bucket
            prefix = prefix
            if path:
                bucket, prefix = s3.split(path)

            func_iter = [
                "s3://{}/{}".format(s3file.bucket_name, s3file.key) for s3file in s3.list(bucket, prefix)
            ]

        bulk_iter = None
        if bulk_size:
            bulk_iter = []
            bulk_idx = -1
            for i, s3file in enumerate(func_iter):
                if i % bulk_size == 0:
                    bulk_iter.append([])
                    bulk_idx += 1
                bulk_iter[bulk_idx].append(s3file)

            # spread last bulk if last bulk < bulk size and bulk_iter > 1
            if spread_last_bulk and len(bulk_iter) > 1 and len(bulk_iter[-1]) < bulk_size:
                last_bulk = bulk_iter.pop()
                for i, s3file in enumerate(last_bulk):
                    bulk_iter[i % len(bulk_iter)].append(s3file)

        self.s3config = s3config
        self.func = func
        self.nb_workers = nb_workers if nb_workers else cpu_count()
        self.func_iter = bulk_iter if bulk_iter else func_iter
        self.func_params = func_params

        self.manager = Manager()
        self.q_in = self.manager.Queue()
        self.q_out = self.manager.Queue()
        self.workers = []

    def __iter__(self):
        """Activate iterator function."""
        # starting workers
        for i in range(self.nb_workers):
            w = S3StreamWorker(self.q_in, self.q_out, self.func, self.func_params, self.s3config)
            w.start()
            self.workers.append(w)

        # starting feeder
        def fill_q_in():
            for v in self.func_iter:
                self.q_in.put(v)
            for _ in range(self.nb_workers):
                self.q_in.put(S3EndOfIteration())

        self.feeder = Thread(target=fill_q_in, daemon=True)
        self.feeder.start()

        return self

    def __next__(self):
        """Get a new element from the q_out."""
        result = self.q_out.get()
        if isinstance(result, Exception):
            raise StopIteration
        elif isinstance(result, S3EndOfIteration):
            self.nb_workers -= 1
            if not self.nb_workers:
                # ending subprocess
                self.feeder.join()
                for w in self.workers:
                    w.join()
                raise StopIteration
            return self.__next__()
        return result
