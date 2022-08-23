(ns squery-spark.spark-definitive-book.ch13
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]

            ;;only for development, autocomplete of IDE, not needed
            [squery-spark.rdds.rdd :refer :all]

            ;;dataset related
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.utils :refer [seq->rdd]]

            [clojure.pprint :refer [pprint]]
            [clojure.string :as s]

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


;;val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
;  .split(" ")
;val words = spark.sparkContext.parallelize(myCollection, 2)

(def my-collection (s/split "Spark The Definitive Guide : Big Data Processing Made Simple" #" "))
(def words (seq->rdd my-collection 2 spark))

;words.map(word => (word.toLowerCase, 1))

;;r is the local dsl enviroment, map here is the rdd map (we need it when we shadow clojure.core)
(r (print (map (fn [w] [(s/lower-case w) 1]) words)))

;val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
;keyword.mapValues(word => word.toUpperCase).collect()

(def key-word (key-by #(first %) words))
(print-pairs key-word)

;;each value gets upper-case
(print-pairs (map-values (fn [kw] (s/upper-case kw)) key-word))

;;keyword.flatMapValues(word => word.toUpperCase).collect()

;;[k,v] => (k,f=[v1 v2 ...]) (flatmap)=> (k,v1) (k,v2) ...
(print-pairs (flat-map-values (fn [kw] [(s/upper-case kw)]) key-word))

;keyword.keys.collect()
;keyword.values.collect()
;keyword.lookup("s")

(r (print (keys key-word))
   (print (vals key-word))
   (print-pairs key-word)
   (pprint (get key-word "S"))     ;;TODO does work for some reason, returns empty list
   )