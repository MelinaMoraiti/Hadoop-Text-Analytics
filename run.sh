#!/bin/bash

# Check for the correct number of arguments

if [ "$#" -ne 6 ]; then
	echo "Usage $0: <input dir> <intermediate dir> <output dir> <classes dir> <package_name.main_class_name> <JAR name>"
	exit 1
fi

INPUT_DIR="$1"
INTERMEDIATE_DIR="$2"
OUTPUT_DIR="$3"
CLASSES_DIR="$4"
MAIN="$5"
JAR_NAME="$6"

#Create JAR file
if [ -e "$JAR_NAME" ]; then
	echo "Deleting existing JAR file ..."
	rm "$JAR_NAME"
fi
echo "Creating JAR file ... "
jar -cvf tfidf.jar -C "$CLASSES_DIR"/ .

#Execute HADOOP job
echo "Running Hadoop job ..."
hadoop jar tfidf.jar "$MAIN" "$INPUT_DIR" "$INTERMEDIATE_DIR" "$OUTPUT_DIR"

#Print results
echo "Print result outputs from Hadoop's HDFS..."
hadoop fs -cat "$INTERMEDIATE_DIR"/part-00000
echo "==========================================================================="
hadoop fs -cat "$OUTPUT_DIR"/part-00000

#Delete output directories
echo "Removing outputs from Hadoop's HDFS ..."
hadoop fs -rm -r "$INTERMEDIATE_DIR" "$OUTPUT_DIR"

echo "Job Completed successfully ..."