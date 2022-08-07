(ns squery-spark.datasets.utils
  (:require [squery-spark.state.connection :refer [get-java-spark-context]]
            [squery-spark.datasets.schema :refer [build-schema]])
  (:import (org.apache.spark.sql RowFactory)))

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
    (.createDataFrame spark rdd schema)))

(defn seq->df [spark seq schema]
  (let [rdd (seq->rdd spark seq)]
    (rdd->df spark rdd schema)))