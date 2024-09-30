# Hadoop-Text-Analytics-App

## Overview
This project implements text analytics functionalities using the Hadoop MapReduce framework. It calculates the number of files a term appears in, the maximum term frequency, and computes the TF-IDF (Term Frequency-Inverse Document Frequency) for a set of text documents.

## 🛠️ Requirements
- Apache Hadoop
- Java Development Kit (JDK)

## Getting Started

### 📊 Output Format

The output from the final reducer will be structured as follows:
word max_docname max_tf m

- **`word`**: The analyzed term
- **`max_docname`**: The file with the maximum term frequency
- **`max_tf`**: The highest frequency of the term in that file
- **`m`**: The number of files containing the term
  
## ⚙️ How to Run?

Easily build and run your project with the provided shell scripts:

### Build Script

```bash
./build.sh <classes dir> <path to source files>
```

### Run Script

```bash
./run.sh <input dir> <intermediate dir> <intermediate dir> <output dir> <num of reducers> <classes dir> <package_name.main_class_name> <JAR name>
```

### Parameters

- <input dir>: Directory for input text files.
- <intermediate dir>: Directory for intermediate output.
- <output dir>: Directory for final output.
- <num of reducers>: Number of reducers (1, 2, or 4).
- <classes dir>: Directory with compiled classes.
- <package_name.main_class_name>: Main class of your application.
- <JAR name>: Name of the JAR file to execute.
