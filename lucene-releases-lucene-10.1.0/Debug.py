import os

if os.system("call gradlew :lucene:core:assemble") != 0:
    raise RuntimeError("Failed to assemble lucene:core")
if os.system("call gradlew :lucene:luke:assemble") != 0:
    raise RuntimeError("Failed to assemble lucene:luke")

command = (
    'call java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 --add-modules jdk.incubator.vector -jar '
    '"lucene\\luke\\build\\lucene-luke-10.1.0-SNAPSHOT\\lucene-luke-10.1.0-SNAPSHOT-standalone.jar" ' 'INDEXING'
)
#os.system(command)


command = (
    'call java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 --add-modules jdk.incubator.vector -jar '
    '"lucene\\luke\\build\\lucene-luke-10.1.0-SNAPSHOT\\lucene-luke-10.1.0-SNAPSHOT-standalone.jar" ' 'SEARCH'
)
os.system(command)

