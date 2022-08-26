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

;;words.map(word => (word.toLowerCase, 1))

;;r is the local dsl enviroment, map here is the rdd map (we need it when we shadow clojure.core)
(r (print (map (fn [w] [(s/lower-case w) 1]) words)))

;val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
;keyword.mapValues(word => word.toUpperCase).collect()


(def key-word (map-to-pair (fn [w] [(first w) w]) words))
;;same as keyBy (def key-word (to-kv #(first %) words))
(r/print-pairs key-word)


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

(def cs (map-flat (fn [w] (map s/lower-case w)) words))
(r/print cs)

(def kv-cs (r/to-pair-rdd (r/map (fn [l] (kv l 1)) cs)))
(defn max-func [left right] (max left right))
(defn add-func [left right] (+ left right))

;;val timeout = 1000L //milliseconds
;val confidence = 0.95
;KVcharacters.countByKey()
;KVcharacters.countByKeyApprox(timeout, confidence)   skipped
;KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
;KVcharacters.reduceByKey(addFunc).collect()
;;KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()


;;group-reduce better than this
(r (print (map (cfn [t2] (apply + (tget t2 1))) (group kv-cs))))

(println "PAIR-RDD")
(r (print kv-cs))

;;TODO write difference
(r (print-pairs (group kv-cs)))                             ;;[k1 [v1,v2 ...] ..]
(pprint (group-count kv-cs))

(r (print-pairs (group-reduce + kv-cs)))
(r (print-pairs (group-reduce add-func add-func 0 kv-cs)))  ;;this does the same as the above

(r (print-pairs (group-reduce add-func max-func 0 kv-cs)))

;val nums = sc.parallelize(1 to 30, 5)

(def nums (.parallelize sc (range 1 31) 5))
(r/print nums)

;nums.aggregate(0)(maxFunc, addFunc)
;val depth = 3
;nums.treeAggregate(0)(maxFunc, addFunc, depth)
;KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()

(r (prn (reduce max-func add-func 0 nums)))

(def depth 3)
(r (prn (reduce-tree max-func add-func depth 0 nums)))

;;val valToCombiner = (value:Int) => List(value)
;val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
;val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
;// now we define these as function variables
;val outputPartitions = 6
;KVcharacters
;  .combineByKey(
;    valToCombiner,
;    mergeValuesFunc,
;    mergeCombinerFunc,
;    outputPartitions)
;  .collect()

(r (print-pairs (group-reduce-fn conj #(apply conj %1 %2) vector kv-cs)))


;// in Scala
;KVcharacters.foldByKey(0)(addFunc).collect()

(r (print-pairs (group-reduce-neutral + 0 kv-cs)))

;
;// in Scala
;import scala.util.Random
;val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
;val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
;val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
;val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
;charRDD.cogroup(charRDD2, charRDD3).take(5)

(def distinct-cs (map-flat (fn [w] (map s/lower-case w)) words))
(r/print distinct-cs)
(def char-rdd1 (map-to-pair (fn [c] [c (rand)]) distinct-cs))
(def char-rdd2 (map-to-pair (fn [c] [c (rand)]) distinct-cs))
(def char-rdd3 (map-to-pair (fn [c] [c (rand)]) distinct-cs))

(print-pairs (cogroup char-rdd1 char-rdd2 char-rdd3))

;;val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
;val outputPartitions = 10
;KVcharacters.join(keyedChars).count()
;KVcharacters.join(keyedChars, outputPartitions).count()

(def keyed-chars (map-to-pair (fn [c] [c (rand)]) distinct-cs))
(print-pairs (join kv-cs keyed-chars))
(print-pairs (join kv-cs keyed-chars 10))

;;;val numRange = sc.parallelize(0 to 9, 2)
;;words.zip(numRange).collect()

(def num-range (.parallelize sc (range 0 10) 2))
(prn (r/count num-range))
(prn (r/count words))

(print-pairs (zip words num-range))