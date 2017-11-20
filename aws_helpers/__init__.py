from . import s3

S3 = s3.S3
S3_SPLIT = s3.S3_SPLIT

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
