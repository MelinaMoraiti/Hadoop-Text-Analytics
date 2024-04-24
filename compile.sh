#!/bin/bash

# Check for the correct number of arguments

if [ "$#" -ne 2 ]; then
	echo "Usage $0: <classes dir> <path to source files>"
	exit 1
fi

CLASSES_DIR="$1"

SOURCES="$2"

if [ -d "$CLASSES_DIR" ]; then
	echo "Deleting existing classed dir: $CLASSES_DIR"
	rm -r "$CLASSES_DIR"
fi
mkdir "$CLASSES_DIR"

#Compile Java code
echo "Compiling Java sources..."
javac -classpath "$(yarn classpath)" -d "$CLASSES_DIR" "$SOURCES"/*.java