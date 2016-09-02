Instruction to Run ALS_Final.py 

1] Put the data file in HDFS using hadoop fs -put command

Ex - hadoop fs -put /users/rgajera/rat* /user/rgajera/als_input

2] Navigate to the location where your ALS_Final.py is placed by cd.. command

3] Run the ALS_Final.py. It takes path of the rating.csv file as input argument

Ex - pyspark ALS_Final.py /user/rgajera/als_input/rating.csv


Instruction to run MovieLensItemCF.scala

1] This program needs five different parameters as command line arguments
	-	Similarity Measure (euclidean,cosine,pearson)
	-	K - neighbours (1,5,7,..)
	-	Number of Recommendation (5,10,20,...)
	- 	Input file path (Use u.data file as input)
	-	Output file path
	
Example - spark-shell MovieLensItemCF.scala cosine 1 100 /users/rgajera/u.data /users/rgajera/reco1


To run the project you need to install Kafka,ZooKeeper and Meteor.
