(ns squery-spark.datasets.utils
  (:require [squery-spark.state.connection :refer [get-java-spark-context]]
            [squery-spark.datasets.schema :refer [build-schema]]
            [squery-spark.datasets.internal.common :refer [columns]])
  (:import (org.apache.spark.sql RowFactory Column SparkSession)
           (org.apache.spark SparkContext)
           (org.apache.spark.api.java JavaPairRDD JavaRDD JavaSparkContext)
           (scala Tuple2)))

(defn parallelize
  ([spark data]
   (-> ^JavaSparkContext (get-java-spark-context spark)
       ^JavaRDD (.parallelize data)))
  ([spark data npartitions]
   (-> (get-java-spark-context spark)
       ^JavaRDD (.parallelize data npartitions))))

(defn parallelize-pairs
  ([spark data]
   (-> ^JavaSparkContext (get-java-spark-context spark)
       ^JavaPairRDD (.parallelizePairs data)))
  ([spark data npartitions]
   (-> (get-java-spark-context spark)
       ^JavaPairRDD (.parallelizePairs data npartitions))))

(defn seq->row [seq]
  (if (not (coll? seq))
    (RowFactory/create (into-array Object [seq]))
    (RowFactory/create (into-array Object seq))))

(defn seq->rdd-rows [seq spark]
  (parallelize spark (map seq->row seq)))

(defn seq->rdd
  ([seq spark] (parallelize spark seq))
  ([seq npartitions spark] (parallelize spark seq npartitions)))

(defn seq->pair-rdd
  ([seq spark] (parallelize-pairs spark (map #(Tuple2. (get % 0) (get % 1)) seq)))
  ([seq npartitions spark]
   (parallelize-pairs spark (map #(Tuple2. (get % 0) (get % 1)) seq) npartitions)))

(defn rdd->df [rdd spark schema]
  (let [schema (if (vector? schema) (build-schema schema) schema)]
    (.createDataFrame ^SparkSession spark rdd schema)))

(defn seq->df [spark seq schema]
  (let [rdd (seq->rdd-rows seq spark)]
    (rdd->df rdd spark schema)))

(defn show
  ([df] (.show df))
  ([df n-rows] (.show df n-rows))
  ([df n-rows truncate] (.show df n-rows truncate))
  ([df n-rows truncate vertical?] (.show df n-rows truncate vertical?)))

(defn take-rows [df n]
  (.takeAsList df n))

(defn repartition
  ([df n] (.repartition df n))
  ([df n & cols] (.repartition df n (into-array Column (columns cols)))))