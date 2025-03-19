import os

integerCompressionAlgorithmsToBenchmark = ["PFOR", "NEWPFOR", "DELTA", "DEFAULT"]
datasetsToBenchmark = ["COMMONCRAWL"]
benchmarkTypes = ["INDEXING", "SEARCH"]




os.system("call gradlew :lucene:core:assemble")
os.system("call gradlew :lucene:luke:assemble")


for benchmarkType in benchmarkTypes:
    for integerComp in integerCompressionAlgorithmsToBenchmark:
        for dataset in datasetsToBenchmark:
            command = (
                'call java --add-modules jdk.incubator.vector -jar '
                '"lucene\\luke\\build\\lucene-luke-10.1.0-SNAPSHOT\\lucene-luke-10.1.0-SNAPSHOT-standalone.jar" '
                + benchmarkType + ' '  + dataset + ' ' + integerComp
            )
            os.system(command)

