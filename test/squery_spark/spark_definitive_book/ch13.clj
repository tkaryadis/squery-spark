(ns squery-spark.spark-definitive-book.ch13
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-java-spark-context]]
            [squery-spark.state.connection :refer [get-spark-session get-java-spark-context]]
            [squery-spark.datasets.utils :refer [seq->rdd rdd->df show]]
            [squery-spark.datasets.schema :refer [todf]]
            [squery-spark.datasets.rows :refer [row]]
            [squery-spark.rdds.rdd :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s]
            [clojure.core :as c]

    ;;only for development, autocomplete of IDE, not needed
            [squery-spark.rdds.rdd :refer :all])
  (:refer-clojure)
  (:import (org.apache.spark.sql Dataset RelationalGroupedDataset Column Row)
           (java.util HashMap)
           (org.apache.spark.rdd RDD)
           (org.apache.spark.api.java JavaRDD JavaSparkContext))
  (:gen-class))


(def spark (get-spark-session))
(def sc (get-java-spark-context spark))
(.setLogLevel (get-java-spark-context spark) "ERROR")
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

(def key-word (to-kv #(first %) words))
(print-pairs key-word)

;;each value gets upper-case
(print-pairs (map-values (fn [kw] (s/upper-case kw)) key-word))

;;keyword.flatMapValues(word => word.toUpperCase).collect()

;;[k,v] => (k,f=[v1 v2 ...]) (flatmap)=> (k,v1) (k,v2) ...
(print-pairs (map-flat-values (fn [kw] [(s/upper-case kw)]) key-word))

;keyword.keys.collect()
;keyword.values.collect()
;keyword.lookup("s")

(r (print (keys key-word))
   (print (vals key-word))
   (print-pairs key-word)
   (pprint (get key-word "S"))     ;;TODO doesnt work for some reason, returns empty list
   )


;;val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
;  .collect()
;import scala.util.Random
;val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
;words.map(word => (word.toLowerCase.toSeq(0), word))
;  .sampleByKey(true, sampleMap, 6L)
;  .collect()

(def distinct-chars (collect (r/distinct (map-flat (fn [w] (map s/lower-case w)) words))))
(pprint distinct-chars)

(def sample-map (into {} (mapv (fn [c] [c, (rand)]) distinct-chars)))
(prn sample-map)

(r (print-pairs
     (sample-by-key (to-kv (cfn [w] (s/lower-case (first w))) words)
                    true
                    sample-map
                    6)))

;words.map(word => (word.toLowerCase.toSeq(0), word))
;  .sampleByKeyExact(true, sampleMap, 6L).collect()

(r (print-pairs
     (sample-by-key-exact (to-kv (cfn [w] (s/lower-case (first w))) words)
                          true
                          sample-map
                          6)))

;;val chars = words.flatMap(word => word.toLowerCase.toSeq)
;val KVcharacters = chars.map(letter => (letter, 1))
;def maxFunc(left:Int, right:Int) = math.max(left, right)
;def addFunc(left:Int, right:Int) = left + right
;val nums = sc.parallelize(1 to 30, 5)


(def cs (map-flat (fn [w] (map s/lower-case w)) words))
(r/print cs)

(def kv-cs (r/to-pair-rdd (r/map (fn [l] (kv l 1)) cs)))
(defn max-func [left right] (max left right))
(defn add-func [left right] (+ left right))
(def nums (.parallelize sc (range 1 31) 5))

(r/print nums)

;;val timeout = 1000L //milliseconds
;val confidence = 0.95
;KVcharacters.countByKey()
;KVcharacters.countByKeyApprox(timeout, confidence)   skipped
;KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
;KVcharacters.reduceByKey(addFunc).collect()

;;group-reduce better than this
(r (print (map (cfn [t2] (apply + (tget t2 1))) (group kv-cs))))

(pprint (group-count kv-cs))
(r (print-pairs (group kv-cs)))
(r (print-pairs (group-reduce + kv-cs)))



