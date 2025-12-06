#!/bin/bash

# Stop on errors
set -e

echo "Cleaning up old files"
rm -f *.class WordCount.jar

echo "Compiling WordCount.java"
if javac -cp "$(hadoop classpath)" WordCount.java; then
    echo "Compilation Successful."
else
    echo "Compilation Failed. Make sure 'hadoop' is in your PATH."
    exit 1
fi

echo "Packaging WordCount.jar"
jar -cvf WordCount.jar *.class

echo "Build Complete!"
echo "You can now run the Python experiment runner."