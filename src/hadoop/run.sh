#/bin/bash
# Written with the assistance of ChatGPT

# Exit if a command fails
set -e
source ~/.bashrc

# Check if the user provided an argument ($# means "number of arguments") 
if [ $# -eq 0 ]; then     
	echo "Error: Please provide a dataset name (e.g., toy, small, or medium)."     
	echo "Usage: $0 <dataset_name>"     
	exit 1 
fi  

# Store the first argument ($1) in a clear variable 
DATASET_NAME=$1 
LOCAL_DATA_DIR="../../dataset/${DATASET_NAME}"  

# --- VALIDATE LOCAL FOLDER --- 
# Check if the local data folder actually exists before we do anything 
if [ ! -d "$LOCAL_DATA_DIR" ]; then     
	echo "Error: Local data directory '$LOCAL_DATA_DIR' not found."     
	echo "Please make sure it's in the same directory as this script."     
	exit 1
fi

echo "--- 1. CLEANING: Removing old .class and .jar files ---" 
rm -f WordCount*.class WordCount.jar  

echo "--- 2. COMPILING: Building new .class files ---" 
javac -cp "$(hadoop classpath)" WordCount.java  

echo "--- 3. PACKAGING: Creating new WordCount.jar ---" 
jar -cvf WordCount.jar WordCount*.class  

# Define our HDFS paths using variables 
HDFS_INPUT_DIR="/user/$USER/input" 
HDFS_OUTPUT_DIR="/user/$USER/output_${DATASET_NAME}"   

echo "--- 4. Preparing HDFS ---" 
echo "Removing old HDFS output directory: $HDFS_OUTPUT_DIR" 
hdfs dfs -rm -r -f $HDFS_OUTPUT_DIR

echo "--- 5. Creating new HDFS input directory: $HDFS_INPUT_DIR ---" 
hdfs dfs -mkdir -p $HDFS_INPUT_DIR

echo "--- 6. Uploading new data from '$LOCAL_DATA_DIR' to '$HDFS_INPUT_DIR' ---"  
hdfs dfs -put "${LOCAL_DATA_DIR}"/* $HDFS_INPUT_DIR

echo "--- 7. RUNNING: Submitting the job ---" 
hadoop jar WordCount.jar WordCount $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR
