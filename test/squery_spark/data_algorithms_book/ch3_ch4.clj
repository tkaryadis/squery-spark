(ns squery-spark.data-algorithms-book.ch3-ch4
  (:refer-clojure :only [])
  (:use [squery-spark.datasets.queries]
        [squery-spark.datasets.stages]
        [squery-spark.datasets.operators]
        [squery-spark.datasets.rows]
        [squery-spark.datasets.utils]
        [squery-spark.rdds.rdd])
  (:refer-clojure)
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context get-java-spark-context]]
            [squery-spark.rdds.rdd :as r :refer [rlet r r-> r->> cfn cpfn]]
            [squery-spark.datasets.stages]
            [clojure.pprint :refer [pprint]]
            [squery-spark.datasets.queries :refer [q]]
            [squery-spark.datasets.utils :refer [seq->rdd seq->pair-rdd seq->df show]]
            [squery-spark.rdds.utils :refer [read-csv-no-header]]
            [clojure.string :as s]
            [clojure.core :as c])
  (:gen-class)
  (:import (org.apache.spark SparkContext)
           (org.apache.spark.rdd PairRDDFunctions)
           (org.apache.spark.sql Dataset)
           (org.apache.spark.sql.types DataTypes)
           (scala.collection.convert Wrappers$IteratorWrapper)))

(def spark (get-spark-session))
(def sc (get-java-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")


(def data-path "/home/white/IdeaProjects/squery-spark/data-used/data-algorithms/")
(defn -main [])


;;ch3(mappings) simple code filter etc

;;ch4(reductions for pair rdds)

(def prdd (seq->pair-rdd [["alex", 2], ["alex", 4], ["alex", 8],
                          ["jane", 3], ["jane", 7],
                          ["rafa", 1], ["rafa", 3], ["rafa", 5], ["rafa", 6],
                          ["clint", 9]]
                         spark))

;;group  (groupbykey)
(r (print-pairs (group prdd)))

;;group-reduce(reducebykey)    1f
(r (print-pairs (group-reduce + prdd)))

;;group-reduce(aggregateByKey) 2f and init value
(r (print-pairs (group-reduce + + 0 prdd)))

;;group-reduce-fn (fn will applied to the first (k,v) to get the init-value)
(r (print-pairs (group-reduce + + identity prdd)))

;;group-reduce-fn (with f-init that does something useful)
(r (print-pairs (map-values (fn [p] (/ (p0 p) (p1 p)))
                     (group-reduce
                       ;;c the state pair v the next value a number
                       (cpfn [c v] [(+ (p0 c) v) (+ (p1 c) 1)])
                       ;;c1 c2 both pairs
                       (cpfn [c1 c2] [(+ (p0 c1) (p0 c2))
                                      (+ (p1 c1) (p1 c2))])
                       ;;create the init state pair from v number
                       ;;v becomes [v 1] [sum count]
                       (cpfn [v] [v 1])
                       prdd))))

;;------------------------------movies dataset-----------------------------------

;;find the average rating for each user
;;userId,movieId,rating,timestamp

(def ratings (read-csv-no-header sc (str data-path "/ch2/ml-latest-small/ratings.csv")))

;;sol1 group-reduce-fn  or init-value
(rlet [ratings (map-to-pair (cfn [line]
                              (let [line-parts (clojure.string/split line #",")]
                                [(read-string (get line-parts 0))
                                 (Double/parseDouble (get line-parts 2))]))
                            ratings)
       ratings (group-reduce (cpfn [c v] [(+ (p0 c) v) (+ (p1 c) 1)])
                             (cpfn [c1 c2] [(+ (p0 c1) (p0 c2))
                                               (+ (p1 c1) (p1 c2))])
                             (fn [v] [v 1])  ;;or  (p 0 0)
                             ratings)
       ratings (map-values (fn [p] (/ (p0 p) (p1 p))) ratings)
       ]
  (print-pairs ratings))

;;sol2
(rlet [ratings (map-to-pair (cfn [line]
                              (let [line-parts (clojure.string/split line #",")]
                                [(read-string (get line-parts 0))
                                 (p (Double/parseDouble (get line-parts 2))
                                    1)]))
                            ratings)
       ratings (group-reduce (cpfn [value this]
                               [(+ (p0 value) (p0 this))
                                (+ (p1 value) (p1 this))])
                             ratings)
       ;ratings
       #_(map-to-pair (cfn [pair]
                      [(p0 pair) (/ (p0 (p1 pair)) (p1 (p1 pair)))])
                    ratings)]
  (print-pairs ratings))