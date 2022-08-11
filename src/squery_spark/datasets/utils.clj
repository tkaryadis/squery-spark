(ns squery-spark.datasets.utils
  (:require [squery-spark.state.connection :refer [get-java-spark-context]]
            [squery-spark.datasets.schema :refer [build-schema]]
            [squery-spark.datasets.internal.common :refer [columns]])
  (:import (org.apache.spark.sql RowFactory Column SparkSession)))

(defn parallelize [spark data]
  (-> (get-java-spark-context spark)
      (.parallelize data)))

(defn seq->row [seq]
  (if (not (coll? seq))
    (RowFactory/create (into-array Object [seq]))
    (RowFactory/create (into-array Object seq))))

(defn seq->rdd [spark seq]
  (parallelize spark (map seq->row seq)))

(defn rdd->df [spark rdd schema]
  (let [schema (if (vector? schema) (build-schema schema) schema)]
    (.createDataFrame ^SparkSession spark rdd schema)))

(defn seq->df [spark seq schema]
  (let [rdd (seq->rdd spark seq)]
    (rdd->df spark rdd schema)))

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