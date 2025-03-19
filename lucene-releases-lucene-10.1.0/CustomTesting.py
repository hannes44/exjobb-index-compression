import os

#integerCompressionAlgorithmsToBenchmark = ["PFOR", "NEWPFOR", "DELTA", "DEFAULT"]
integerCompressionAlgorithmsToBenchmark = ["PFOR", "NEWPFOR", "DELTA", "DEFAULT"]
#datasetsToBenchmark = ["COMMONCRAWL"]
datasetsToBenchmark = ["COMMONCRAWL"]
#benchmarkTypes = ["INDEXING", "SEARCH"]
benchmarkTypes = ["SEARCH"]

os.system("call gradlew :lucene:core:assemble")
os.system("call gradlew :lucene:luke:assemble")


for dataset in datasetsToBenchmark:
    indexDataFilePath = "../BenchmarkData/IndexingData/" + dataset + ".csv"
    searchDataFilePath = "../BenchmarkData/SearchData/" + dataset + ".csv"

    if os.path.exists(indexDataFilePath):
        os.remove(indexDataFilePath)
        
    if os.path.exists(searchDataFilePath):
        os.remove(searchDataFilePath)
        
    for benchmarkType in benchmarkTypes:
        for integerComp in integerCompressionAlgorithmsToBenchmark:

                command = (
                    'call java --add-modules jdk.incubator.vector -jar '
                    '"lucene\\luke\\build\\lucene-luke-10.1.0-SNAPSHOT\\lucene-luke-10.1.0-SNAPSHOT-standalone.jar" '
                    + benchmarkType + ' '  + dataset + ' ' + integerComp
                )
                os.system(command)

