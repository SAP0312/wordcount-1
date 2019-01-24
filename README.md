# Wordcount

## Word Count MapReduce Program which uses gradle build tool. 

### Build Jar
* First checkout and import this project in IntelliJ IDE.
* Build the project.
* Go to root of the directory and run --> `./gradlew build jar`
* After this Jar will be created in WordCount/build/libs/

### Download dataset from https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/

### Upload Dataset to HDFS
`hadoop fs -CopyFromLocal dataset.txt /dataset.txt`

### Run WordCount MapReduce Jar.
`hadoop jar <jarName> <inputDatasetPathAndFile> <outputDirectory>`

