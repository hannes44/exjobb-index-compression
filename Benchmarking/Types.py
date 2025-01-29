from enum import Enum

class SearchPerformance():
    compressionType = None

# The different datasets we will be testing the compression on
# We will test the indexing time, search time and the size of the index
class SearchDatasets(Enum):
    SMALL_UNSTRUCTURED = 1
    LARGE_UNSTRUCTURED = 2

class CompressionType(Enum):
    NONE = 1
