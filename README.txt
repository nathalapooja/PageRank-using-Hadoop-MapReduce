// Name- POOJA REDDY NATHALA
//800974452
//pnathala@uncc.edu

$ cd Downloads
Assuming that all java files are stored in Downloads and jar files are storing in downloads

Steps 1,2,3 are common to execute any java file on hadoop file system.
Step1: Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/pagerank/input in HDFS: 
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input 
Step2:Put the wiki-micro.txt into the input directory using the following command:
$ hadoop fs -put wiki-micro.txt /user/cloudera/pagerank/input
Step3:To Compile any java class we are creating a build directory inside the Downloads
$ mkdir -p build 

Steps to execute :
Step4: Use the following command to compile all files in folder
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint
Step5: Use the following command to create a JAR file for PageRank application.
$ jar -cvf pagerank.jar -C build/ .
Step6: Use the following command to run the PageRank application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument and also give number of iterations as third argument
$ hadoop jar pagerank.jar Driver /user/cloudera/pagerank/input/wiki-micro.txt /user/cloudera/pagerank/output 10
Here we are storing the output in /user/cloudera/pagerank/output folder
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/pagerank/output/*
Step8:To get the hdfs output file to local directory use the following command
$  hadoop fs -get /user/cloudera/pagerank/output/part-r-00000 /home/cloudera/Desktop/​Micro-Wiki.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/pagerank/output
-------------------------------------------------------------------------------------------------------------------------------------
Driver Class:The Main class is the Driver class which contains only one main function and which calls all other modules 
 Main function has 3 arguments: InputPath, OutputPath and number of iterations
 Main function runs jobs in the following order
 1. LinkGraph - to perform document count from XML representation in the input corpus and formatting pages with initial page rank 1/N and corresponding links 
 2. PageRank_Process - runs the PageRank algorithm and returns page, pagerank and list of links 
 3. CleanUp_Sort - removes all outlinks and extracts only Page with its corresponding rank in the descending order using compareTo() method
-----------------------------------------------------------------------------------------------------------------------------------------
 
