import os
import csv

class BenchmarkData:
    integerCompressionAlgorithm = None
    termCompressionAlgorithm = None
    IndexSizeMB = None
    IndexingSpeed = None
    SearchSpeedNS = None
    
    def getDataRow(self):
        return [self.integerCompressionAlgorithm, self.termCompressionAlgorithm, self.IndexSizeMB, self.IndexingSpeed, self.SearchSpeedNS]

integerCompressionAlgorithmsToBenchmark = ["PFOR", "NEWPFOR", "DELTA", "DEFAULT"]
datasetsToBenchmark = ["COMMONCRAWL"]
benchmarkTypes = ["INDEXING", "SEARCH"]

benchmarkDataForDataset = {}


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
            
    # Parse the benchmark data for the current dataset
    
            
    benchmarkDatas = []
    integerCompressionToIndex = {}

    # Specify the file path

    # Open the CSV file
    with open(searchDataFilePath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)  # Create a CSV reader object

        # Iterate through each row in the CSV
        for row in reader:
            benchmarkData = BenchmarkData()
            benchmarkData.integerCompressionAlgorithm = row[0]
            benchmarkData.SearchSpeedNS = row[1]
            benchmarkData.termCompressionAlgorithm = "LZ4"
            benchmarkDatas.append(benchmarkData)
            integerCompressionToIndex[row[0]] = len(benchmarkDatas) - 1
            
    # Open the CSV file
    with open(indexDataFilePath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)  # Create a CSV reader object

        # Iterate through each row in the CSV
        for row in reader:
            integerCompressionAlg = row[0]
            index = integerCompressionToIndex[integerCompressionAlg]
            benchmarkDatas[index].IndexSizeMB = row[1]
            benchmarkDatas[index].IndexingSpeed = row[2]
           
        finalCSVOutputPath = "../BenchmarkData/" + dataset + ".csv"
                # Open the file in write mode
        with open(finalCSVOutputPath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)  # Create a CSV writer object
            writer.writerow(["IntegerCompressionAlgorithm", "TermCompressionAlgorithm", "IndexSize(MB)", "IndexSpeed(ms)", "SearchSpeed(ns)"])

            for data in benchmarkDatas:
                row = data.getDataRow()
                writer.writerow(row)
        