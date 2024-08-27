(ns squery-spark.sources-sinks.files
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context get-java-spark-context]]
            [squery-spark.datasets.utils :refer [seq->rdd rdd->df show]]
            [squery-spark.datasets.schema :refer [todf]]
            [squery-spark.datasets.rows :refer [row]]
            [squery-spark.rdds.rdd :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s]
            [clojure.core :as c]
            [squery-spark.utils.functional-interfaces :refer :all]

    ;;only for development, autocomplete of IDE, not needed
            [squery-spark.rdds.rdd :refer :all])
  (:refer-clojure)
  (:import (org.apache.spark.api.java.function Function)
           (org.apache.spark.sql Dataset RelationalGroupedDataset Column Row)
           (java.util HashMap)
           (org.apache.spark.rdd RDD)
           (org.apache.spark.api.java JavaPairRDD JavaRDD JavaSparkContext))
  (:gen-class))

(def spark (get-spark-session))
(def sc ^JavaSparkContext (get-java-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery/squery-spark/")
(defn -main [])

;;each line = rdd member
(def textfile1-rdd (.textFile sc (str data-path "/data/mytestdata/textfile1")))

;;wordcount
(r->> textfile1-rdd
      (flatmap (fn [line] (c/map (fn [word] (p word 1)) (clojure.string/split line #"\s+"))))
      (group-reduce (fn [sum _] (inc sum)))
      (rdd)
      (sort-desc (fn [p] (pget p 1)))
      println)