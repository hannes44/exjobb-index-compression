import os

os.system("call gradlew :lucene:core:assemble")
os.system("call gradlew :lucene:luke:assemble")

command = (
    'call java --add-modules jdk.incubator.vector -jar '
    '"lucene\\luke\\build\\lucene-luke-10.1.0-SNAPSHOT\\lucene-luke-10.1.0-SNAPSHOT-standalone.jar" ' 'SEARCH'
)
os.system(command)

