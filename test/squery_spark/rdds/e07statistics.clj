(ns squery-spark.rdds.e07statistics
  (:require [clojure.test :refer :all]))

(ns squery-spark.rdds.e01create-and-various
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
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
           (org.apache.spark.api.java JavaRDD))
  (:gen-class))

(def spark (get-spark-session))
(def sc (get-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery/squery-spark/")
(defn -main [])

;;org.apache.spark.api.java.JavaRDD

;;from collection
(def my-collection (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple" #"\s"))
(def words ^JavaRDD (seq->rdd my-collection 2 spark))


;val confidence = 0.95
;val timeoutMilliseconds = 400
;words.countApprox(timeoutMilliseconds, confidence)

(prn (.countApprox words 400 0.95))

;;skipped 2-3 with approx

;;words.countByValue()

(pprint (r/frequencies words))