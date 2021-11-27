



**Outline**

**Appendix of TraSS**

​	Other Measures

​	Enlarged Element

​	Instantiation

**Experiments**

​	Setup

​	Experiments

# TraSS

We have implemented the proposed framework in HBase, a popular key-value data store. We have published the framework, experimental code and the results in https://github.com/huajunge/k-sim.git. Details of DTW, Hausdorff and Range query，pleas refer to [other_measures.pdf](./other_measures.pdf)

# Experiments

### 1. Setup

- (1) [SupplementaryMaterial](./SupplementaryMaterial) gives  the resource of TraSS and sample data for testing TraSS;
- (2) Building this project to get trass.jar (k-sim-traj-1.0-SNAPSHOT-jar-with-dependencies.jar), or using trass.jar in the [SupplementaryMaterial](./SupplementaryMaterial).
- (3) Putting the trass.jar to the lib path of HBase.

### 2. Storing and Querying

- Storing

  ```
  spark-submit --class com.just.ksim.experiments.storing.StoringTDriveToHBase --master yarn --deploy-mode cluster --num-executors 30 --executor-memory 8G --executor-cores 2 trass.jar  ./tdrive tdrive_table hdfs:///index_time 8 1 1 16
  ```

  where,  ./tdrive is data path; tdrive_table is the table for storing data; hdfs:///index_time is the path to record the indexing time; 8 is the shards; 1  1 is the data size; 16 is the maximum resolution of XZ* index.

- Threshold Similarity Search

  ```
  spark-submit --class com.just.ksim.experiments.query.SimilarityQuery trass.jar  ./tdrive_q tdrive_table 0.015 hdfs:///sim_015 8 16
  ```

  where,  ./tdrive_q is the path query trajectories; tdrive_table is the table stored with tdrive data; 0.015 is the threshold; hdfs:///sim_015 is the path of query time; 8 is the shards; 16 is the maximum resolution of XZ* index.

- Top-k Similarity Search

  ```
  spark-submit --class com.just.ksim.experiments.query.KNNQuery trass.jar  ./tdrive_q tdrive_table 150 hdfs:///knn_150 8 0.002 16
  ```

  where,  ./tdrive_q is the path query trajectories; tdrive_table is the table stored with tdrive data; 150 is the k; hdfs:///knn_015 is the path of query time; 8 is the shards; 16 is the maximum resolution of XZ* index. 

- Range query

  ```
  spark-submit --class com.just.ksim.experiments.query.RangeQuery trass.jar '116.20094,39.97886,116.21094,39.98886;116.20094,39.97886,116.21094,39.98886' tdrive_table hdfs:///range_query_results 8 16
  ```

  

