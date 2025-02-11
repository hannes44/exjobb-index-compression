@echo off
setlocal enabledelayedexpansion

:: Run Gradle assemble
echo Running Gradle build...
call gradlew :lucene:core:assemble
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Build failed during "gradlew assemble".
    exit /b %ERRORLEVEL%
)

:: Run Luke using Gradle
echo Running Luke via Gradle...
call gradlew :lucene:luke:run
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Luke failed to run via Gradle.
    exit /b %ERRORLEVEL%
)

:: Run Luke via Java JAR
echo Running Luke via Java JAR...
call java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 -jar "lucene\luke\build\lucene-luke-9.12.0-SNAPSHOT/lucene-luke-9.12.0-SNAPSHOT-standalone.jar"
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Luke failed to run via Java JAR.
    exit /b %ERRORLEVEL%
)

echo All tasks completed successfully!
exit /b 0
