(ns squery-spark.mongo-connector.utils
  (:import (org.apache.spark.sql Dataset DataFrameReader)))

;;for views i specify partitioner to work for example SinglePartitionPartitioner or ShardedPartitioner
(defn load-collection [spark db-namespace]
  (let [[db collection] (clojure.string/split (name db-namespace) #"\.")
        df (-> spark
               .read
               (.format "mongodb")
               (.option "database" db)
               (.option "collection" collection)
               ;(.option "partitioner" "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
               (.option "partitioner" "com.mongodb.spark.sql.connector.read.partitioner.ShardedPartitioner"))]
    (.load df)))