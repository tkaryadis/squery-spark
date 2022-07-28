(ns squery-spark.state.connection
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark SparkConf)))

(defn get-spark-session []
  (let [conf (-> (SparkConf.)
                 (.setMaster "local[*]")
                 (.setAppName "local-app")
                 ;(.set "spark.driver.port" "40889")
                 (.set "spark.sql.shuffle.partitions" "5") ;;200 default(too slow for local)
                 )
        spark-session (-> (SparkSession/builder)
                          (.config conf)
                          (.getOrCreate))]
    spark-session))

(defn get-spark-context [spark]
  (-> spark
      (.sparkContext)
      (JavaSparkContext/fromSparkContext)))