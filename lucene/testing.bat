call gradlew assemble
call gradlew :lucene:luke:run
call java -jar "lucene\luke\build\lucene-luke-9.12.0-SNAPSHOT/lucene-luke-9.12.0-SNAPSHOT-standalone.jar"
