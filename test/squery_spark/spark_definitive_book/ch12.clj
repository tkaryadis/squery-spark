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

            [clojure.pprint :refer [pprint]]

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
(-> spark (.range 500) j-rdd rprint)

;;spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
(-> spark (.range 10) todf j-rdd (lmap (fn [row] (.getLong row 0))) rprint)

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
(rprint words)

;;words.setName("myWords")
;words.name

(.setName words "my-words")
(prn (.name words))

;spark.sparkContext.textFile("/some/path/withTextFiles")
;spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

;;words.distinct().count()
(prn (rcount (rdistinct words)))

;;def startsWithS(individual:String) = {
;  individual.startsWith("S")
;}
;words.filter(word => startsWithS(word)).collect()

(rprint (rfilter #(clojure.string/starts-with? % "S") words))

;;val words2 = words.map(word => (word, word(0), word.startsWith("S")))

(def words2 (rmap (fn [word] [word (first word) (clojure.string/starts-with? word "S")]) words))
(rprint words2)

;;words2.filter(record => record._3).take(5)

(pprint (rtake 5 (rfilter (fn [w] (true? (get w 2))) words2)))

;;words.flatMap(word => word.toSeq).take(5)

(pprint (rtake 5 (rflat-map (fn [word] (seq word)) words)))

;words.sortBy(word => word.length() * -1).take(2)

(pprint (rtake 2 (rsort! (fn [word] (* (count word) -1)) words)))


;;val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

(def fity-fifty-split (random-split words 0.5 0.5))
(dorun (map rprint fity-fifty-split))


;;spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210

(prn (rreduce (fn [x y] (+ x y)) (seq->rdd (range 1 21) spark)))

;;def wordLengthReducer(leftWord:String, rightWord:String): String = {
;  if (leftWord.length > rightWord.length)
;    return leftWord
;  else
;    return rightWord
;}
;words.reduce(wordLengthReducer)

;;reduces to the biggest word
(prn (rreduce (fn [lw rw] (if (> (count lw) (count rw)) lw rw)) words))

;;words.count()

(prn (rcount words))

;val confidence = 0.95
;val timeoutMilliseconds = 400
;words.countApprox(timeoutMilliseconds, confidence)

(prn (.countApprox words 400 0.95))

;;skipped 2-3 with approx

;;words.countByValue()

(pprint (rfrenquencies words))

;words.first()

(prn (rfirst words))


;;spark.sparkContext.parallelize(1 to 20).max()
;spark.sparkContext.parallelize(1 to 20).min()

(let [rdd (seq->rdd (range 1 21) spark)]
  (prn [(rmax rdd <) (rmin rdd <)]))

;words.take(5)

(pprint (rtake 5 words))

;words.takeOrdered(5)

(pprint (rtake-ordered 5 words))

;words.top(5)

(pprint (rtop 5 words))

;;TODO there are some more