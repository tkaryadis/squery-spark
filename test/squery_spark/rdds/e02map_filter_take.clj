(ns squery-spark.rdds.e02map-filter-take
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.utils :refer [seq->rdd rdd->df show seq->pair-rdd]]
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
  (:import (org.apache.spark.api.java.function Function Function2)
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

(def range10 ^JavaRDD (-> spark (.range 10) df->rdd))

;;r is the local dsl enviroment

;;map
(r (print (map (fn [x] (* x 2)) range10)))

;;flatmap
(r (print (flatmap (fn [x] [x x]) range10)))

;;filter
(r (print (filter #(even? %) (seq->rdd [1 2 3 4] spark))))

;;first
(prn (.first ^JavaRDD (seq->rdd [1 2 3 4] spark)))

;;take
(prn (r/take 2 (seq->rdd [1 2 3 4] spark)))
;;sort+take
(prn (r/take-asc 3 (seq->rdd [1 3 2 4] spark)))
;;sort-dsc +take
(prn (r/take-dsc 3 (seq->rdd [1 3 2 4] spark)))

;;pair-rdds-----------------------------------------------------------

;;each value gets upper-case
(print-pairs (map-values (fn [v] (s/upper-case v)) (seq->pair-rdd [[1 "hello"] [2 "hey"]] spark)))

;;each value, becomes a sequence and then flatmap
;;   (1,h) (1,e) (1,l) (1,l) (1,o) (2,h) (2,e) (2,y)
(print-pairs (flatmap-values (fn [v] (seq v)) (seq->pair-rdd [[1 "hello"] [2 "hey"]] spark)))

