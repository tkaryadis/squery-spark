(ns squery-spark.data-algorithms-book.ch1
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
            [clojure.string :as s]
            [clojure.core :as c])
  (:gen-class)
  (:import (org.apache.spark.sql Dataset)
           (org.apache.spark.sql.types DataTypes)))

(def spark (get-spark-session))
(def sc (get-java-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")


(def data-path "/home/white/IdeaProjects/squery-spark/data-used/data-algorithms/")
(defn -main [])

(def records (-> sc (.textFile (str data-path "/ch1/simple.txt"))))

(r->> records
      (map (cfn [l] (s/lower-case l)))
      (map-flat (cfn [l] (s/split l #",")))
      (filter (cfn [w] (> (count w) 2)))
      print)

;;TODO skipped example with statistics, data missing

(def tuples (seq->pair-rdd
              [["A", 7], ["A", 8], ["A", -4],
               ["B", 3], ["B", 9], ["B", -1],
               ["C", 1], ["C", 5]]
              spark))

(rlet [positives (filter (cfn [t] (> (tget t 1) 0)) tuples)
       _ (print positives)
       sum-avg (r->> positives
                     group
                     (map-values (cfn [v] (p (apply + v)
                                             (/ (apply + v) (.size v))))))
       _ (print sum-avg)
       sum-count (map-values (cfn [v] (p v 1)) positives)
       _ (print sum-count)
       sum-count-agg (group-reduce (cfn [v t]
                                     (p (+ (tget v 0) (tget t 0))
                                         (+ (tget v 1) (tget t 1))))
                                   sum-count)
       _ (print sum-count-agg)
       sum-and-avg (map-values (cfn [v] (p (tget v 0)
                                             (/ (tget v 0)
                                                (tget v 1))))
                               sum-count-agg)
       _ (print sum-and-avg)

       ])

;;skipped small dataframe example,missing data

(def emps [["Sales", "Barb", 40], ["Sales", "Dan", 20],
           ["IT", "Alex", 22], ["IT", "Jane", 24],
           ["HR", "Alex", 20], ["HR", "Mary", 30]])

(def emps-df (seq->df spark emps [:dept :name [:hours DataTypes/LongType]]))

(q emps-df
   (group :dept
          {:average (avg :hours)}
          {:total (sum :hours)})
   show)

(rlet [data [["fox", 6], ["dog", 5], ["fox", 3], ["dog", 8],
            ["cat", 1], ["cat", 2], ["cat", 3], ["cat", 4]]
       rdd (seq->pair-rdd data spark)
       _ (prn (count rdd))
       _ (pprint (collect-kv rdd))
       sum-per-key (group-reduce + rdd)
       _ (pprint (collect-kv sum-per-key))
       sum_filtered (filter (cfn [p] (> (tget p 1) 9)) sum-per-key)
       _ (pprint (collect-kv sum_filtered))
       grouped (group rdd)
       _ (print-pairs grouped)
       grouped-collected (collect-kv grouped)
       ;;scala iterable wrapper implements the iterator so into [] works
       _ (pprint (collect-kv (map (cfn [p]
                                    (p (tget p 0)
                                        (into [] (tget p 1))))
                                  grouped)))
       aggregate (map-values (cfn [v] (apply + v)) grouped)
       _ (pprint (collect-kv aggregate))
       ])


(def census-df ^Dataset (-> spark .read (.json (str data-path "/ch1/census_2010.json"))))

(prn (q census-df .count))

(q census-df
   ((> :age 54))
   (group {:count (count-acc)})
   show)

(q census-df
   ((> :age 54))
   {:total (+ :males :females)}
   show)

;;skipped mysql save example, i use mongo anyways