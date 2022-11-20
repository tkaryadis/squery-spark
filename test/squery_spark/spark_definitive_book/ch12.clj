(ns squery-spark.spark-definitive-book.ch12
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
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
           (org.apache.spark.api.java JavaRDD))
  (:gen-class))

(def spark (get-spark-session))
(def sc (get-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")
(defn -main [])

;;r is the namespace alias with the rdds
;;r also is the macro with rdd dsl , its local shadowing
;;r-> is the macro with rdd dsl and ->
;;r shadows clojure, so we have 3 options, if we want to use clojure.core symbols that are shadowed inside the r enviroment
;;   seperate the clojure code
;;   fn c/map (c alias of clojure.core)
;;   cfn and normal clojure, its like fn but shadows the r, that shadowed clojure
;;   (all those dsl enviroments are few assignements with zero cost in perfomance)

;;spark.range(500).rdd
(r-> spark (.range 500) j-rdd print)

;;spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

;;lmap is the map with the rdd on the left as first arguments
(r-> spark (.range 10) todf j-rdd (lmap (fn [row] (.getLong row 0))) print)

;;TODO simpler way? cant find toDF for java
;;spark.range(10).rdd.toDF()
(-> spark
    (.range 10)
    j-rdd
    (lmap (fn [n] (row n)))
    (rdd->df spark [[:id :long]])
    show)

;;val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
;  .split(" ")
;val words = spark.sparkContext.parallelize(myCollection, 2)

(def my-collection (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple" #"\s"))
(def words (seq->rdd my-collection 2 spark))
(r/print words)

;;words.setName("myWords")
;words.name

(.setName words "my-words")
(prn (.name words))

;spark.sparkContext.textFile("/some/path/withTextFiles")
;spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

;;words.distinct().count()

;;r is the macro, dsl local enviroment, to use shadow clojure
;;here we dont need it, its only 2 functions, we could do it with r/print r/distinct (r meaning the namespace alias)
(r (prn (count (distinct words))))

;;def startsWithS(individual:String) = {
;  individual.startsWith("S")
;}
;words.filter(word => startsWithS(word)).collect()

(r (print (filter #(s/starts-with? % "S") words)))

;;val words2 = words.map(word => (word, word(0), word.startsWith("S")))

(def words2 (r/map (fn [word] [word (first word) (s/starts-with? word "S")]) words))
(r/print words2)

;;words2.filter(record => record._3).take(5)

;; fn is used so we need c/get because we are in r enviroment
(r (pprint (take 5 (filter (fn [w] (true? (c/get w 2))) words2))))

;;words.flatMap(word => word.toSeq).take(5)

;;cfn is used, to hide the r enviroment
;;r,cfn makes sense when alot of code, but cfn can be avoided by seperating the clojure code out of r enviroment
(r (pprint (take 5 (map-flat (cfn [word] (seq word)) words))))

;words.sortBy(word => word.length() * -1).take(2)

(r (pprint (take 2 (sort-desc (cfn [word] (* (count word) -1)) words))))

;;val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

(def fity-fifty-split (random-split words 0.5 0.5))
(dorun (map r/print fity-fifty-split))

;;spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210

(prn (r/reduce (fn [x y] (+ x y)) (seq->rdd (range 1 21) spark)))

;;def wordLengthReducer(leftWord:String, rightWord:String): String = {
;  if (leftWord.length > rightWord.length)
;    return leftWord
;  else
;    return rightWord
;}
;words.reduce(wordLengthReducer)

;;reduces to the biggest word
(prn (r/reduce (fn [lw rw] (if (> (count lw) (count rw)) lw rw)) words))

;;words.count()

(prn (r/count words))

;val confidence = 0.95
;val timeoutMilliseconds = 400
;words.countApprox(timeoutMilliseconds, confidence)

(prn (.countApprox words 400 0.95))

;;skipped 2-3 with approx

;;words.countByValue()

(pprint (r/frequencies words))

;words.first()

(prn (r/first words))


;;spark.sparkContext.parallelize(1 to 20).max()
;spark.sparkContext.parallelize(1 to 20).min()

(let [rdd (seq->rdd (range 1 21) spark)]
  (prn [(r/max rdd <) (r/min rdd <)]))

;words.take(5)

(pprint (r/take 5 words))

;words.takeOrdered(5)

(pprint (r/take-ordered 5 words))

;words.top(5)

(pprint (r/top 5 words))

;;TODO there are some more