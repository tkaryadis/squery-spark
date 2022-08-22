(ns squery-spark.spark-definitive-book.ch12
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.rdds.rdd :refer :all]

    ;;dataset related
            [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all]
            [squery-spark.datasets.udf :refer :all]

            )
  (:refer-clojure)
  (:import (org.apache.spark.sql Dataset RelationalGroupedDataset Column Row)
           (java.util HashMap)
           (org.apache.spark.rdd RDD)
           (org.apache.spark.api.java JavaRDD))
  (:gen-class))

(def spark (get-spark-session))
(def sc (get-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")
(defn -main [])

;;spark.range(500).rdd
(-> spark (.range 500) j-rdd print-rdd)

;;spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
(-> spark (.range 10) todf j-rdd (lmap (fn [row] (.getLong row 0))) print-rdd)

;;TODO simpler way? cant find toDF for java
;;spark.range(10).rdd.toDF()
(let [myrdd (-> spark (.range 10) j-rdd (lmap (fn [n] (row n))))]
  (show (rdd->df spark myrdd [[:id :long]])))

;;val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
;  .split(" ")
;val words = spark.sparkContext.parallelize(myCollection, 2)

(def my-collection (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple" #"\s"))
(def words (seq->rdd spark my-collection 2))
(print-rdd words)
