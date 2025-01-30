from enum import Enum

class SearchPerformance():
    compressionType = None


class CompressionType(str, Enum):
    DEFAULT = "default"
    BEST_COMPRESSION = "best_compression"
