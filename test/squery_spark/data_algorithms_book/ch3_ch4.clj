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
            [squery-spark.rdds.rdd :as r :refer [rlet r r-> r->> cfn pfn]]
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
(r (print-pairs (map-values (pfn [p] (/ (get p 0) (get p 1)))
                            (group-reduce
                              ;;state = [sum count]
                              ;;c the state pair v the next value a number
                              (pfn [c v] [(+ (get c 0) v) (+ (get c 1) 1)])
                              ;;c1 c2 both pairs
                              (pfn [c1 c2] [(+ (get c1 0) (get c2 0))
                                            (+ (get c1 1) (get c2 1))])
                              ;;create the init state pair from v number
                              ;;v becomes [v 1] [sum count]
                              (pfn [v] [v 1])
                              prdd))))


;;------------------------------movies dataset-----------------------------------

;;find the average rating for each user
;;userId,movieId,rating,timestamp

(def ratings (read-csv-no-header sc (str data-path "/ch2/ml-latest-small/ratings.csv")))

;;sol1 with init-value/fn
;;make-pair [userid rating]
;;group-reduce [sum,count]    fn(combineByKey) or init-value(aggregate by key)
(rlet [ratings (map (pfn [line]
                      (let [line-parts (clojure.string/split line #",")]
                        [(read-string (get line-parts 0))
                         (Double/parseDouble (get line-parts 2))]))
                    ratings)
       ;;state = [sum,count]
       ratings (group-reduce (pfn [c v] [(+ (get c 0) v) (+ (get c 1) 1)])
                             (pfn [c1 c2] [(+ (get c1 0) (get c2 0))
                                           (+ (get c1 1) (get c2 1))])
                             (pfn [v] [v 1])  ;;or  (p 0 0)
                             ratings)
       ratings (map-values (pfn [p] (/ (get p 0) (get p 1))) ratings)]
  (print-pairs ratings))

;;sol2 without init-value/fn
;;make-pair [userid [rating 1]]  = [userid [sum,count]]  so i make the init-value from step1
;;group-reduce with 2 args
(rlet [ratings (map (pfn [line]
                      (let [line-parts (clojure.string/split line #",")]
                        [(read-string (get line-parts 0))
                         [(Double/parseDouble (get line-parts 2)) 1]]))
                    ratings)
       ;;state = [sum,count]
       ratings (group-reduce (pfn [value this]
                               [(+ (get value 0) (get this 0))
                                (+ (get value 1) 1)])
                             ratings)
       ratings (map-values (pfn [p] (/ (get p 0) (get p 1))) ratings)
       ]
  (print-pairs ratings))

