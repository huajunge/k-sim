# DiskS

We have implemented the proposed framework in HBase, a popular disk-based NoSQL database. We have published the framework, experimental code and the results in https://github.com/huajunge/k-sim.git

### 1. Setup

- (1) puting the disk-sim.jar to the lib path of HBase.  
- (2) building this project to get disks.jar (k-sim-traj-1.0-SNAPSHOT-jar-with-dependencies.jar), or using disks.jar in the Supplementary Material.zip.

### 2. Experiments

- Data
  tdrive and tdrive_q are experimental data (sampled), see in Supplementary Material.zip

- Storing

  ```
  spark-submit --class com.just.ksim.experiments.storing.StoringTDriveToHBase --master yarn --deploy-mode cluster --num-executors 30 --executor-memory 8G --executor-cores 2 disks.jar  ./tdrive tdrive_table hdfs:///index_time 8 1 1 16
  ```

  where,  ./tdrive is data path; tdrive_table is the table for storing data; hdfs:///index_time is the path to record the indexing time; 8 is the shards; 1  1 is the range of data size (e.g., 1 2 means that we will use two copies of the dataset); 16 is the maximum resolution of XZ* index.

- Threshold Similarity Search

  ```
  spark-submit --class com.just.ksim.experiments.query.SimilarityQuery disks.jar  ./tdrive_q tdrive_table 0.015 hdfs:///sim_015 8 16
  ```

  where,  ./tdrive_q is the path query trajectories; tdrive_table is the table stored with tdrive data; 0.015 is the threshold; hdfs:///sim_015 is the path of query time; 8 is the shards; 16 is the maximum resolution of XZ* index.

-  Top-k Similarity Search

  ```
  spark-submit --class com.just.ksim.experiments.query.KNNQuery disks.jar  ./tdrive_q tdrive_table 150 hdfs:///knn_150 8 0.002 16
  ```

  where,  ./tdrive_q is the path query trajectories; tdrive_table is the table stored with tdrive data; 150 is the k; hdfs:///knn_015 is the path of query time; 8 is the shards; 16 is the maximum resolution of XZ* index. 

