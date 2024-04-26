#!/bin/bash

# Check for the correct number of arguments

if [ "$#" -ne 7 ]; then
	echo "Usage $0: <input dir> <intermediate dir> <intermediate dir> <output dir> <classes dir> <package_name.main_class_name> <JAR name>"
	exit 1
fi

INPUT_DIR="$1"
INTERMEDIATE_DIR="$2"
INTERMEDIATE_DIR2="$3"
OUTPUT_DIR="$4"
CLASSES_DIR="$5"
MAIN="$6"
JAR_NAME="$7"

#Create JAR file
if [ -e "$JAR_NAME" ]; then
	echo "Deleting existing JAR file ..."
	rm "$JAR_NAME"
fi
echo "Creating JAR file ... "
jar -cvf tfidf.jar -C "$CLASSES_DIR"/ .

#Execute HADOOP job
echo "Running Hadoop job ..."
hadoop jar tfidf.jar "$MAIN" "$INPUT_DIR" "$INTERMEDIATE_DIR" "$INTERMEDIATE_DIR2" "$OUTPUT_DIR"

#Print results
echo "Print result outputs from Hadoop's HDFS..."
hadoop fs -cat "$INTERMEDIATE_DIR"/part-00000
echo "============================================================="
hadoop fs -cat "$INTERMEDIATE_DIR2"/part-00000
echo "============================================================="
hadoop fs -cat "$OUTPUT_DIR"/part-00000

#Copy output part files from HDFS to local filesystem before removing 
hadoop fs -get "$OUTPUT_DIR"/part* .

#Delete output directories
echo "Removing outputs from Hadoop's HDFS ..."
hadoop fs -rm -r "$INTERMEDIATE_DIR" "$INTERMEDIATE_DIR2" "$OUTPUT_DIR"

echo "Job Completed successfully ..."