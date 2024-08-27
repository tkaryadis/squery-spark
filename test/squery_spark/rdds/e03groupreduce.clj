(ns squery-spark.rdds.e03groupreduce
  (:refer-clojure :only [])
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.utils :refer [seq->rdd seq->pair-rdd rdd->df show]]
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

(prn (r (reduce + range10)))


;;group(group_key,iterable with all values with the same key)
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      group
      print)

;;group+count the group, returns hashmap  {k1 : count1 ....}
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-count)
      prn)

;;group+reduce f on the group, 2 args state(initially_the_first_member)+next member
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-reduce (fn [state x] (+ state x)))
      print)

;;same like above
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-reduce +)
      print)

;;group+reduce using f1 in partition, f2 between partitions, 0 init value
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-reduce + + 0)
      print)

;;same as above but init value from a function using the first value as arg
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-reduce + + (fn [x] x))
      print)

;;i give a natural value, that can always be used as init value
(r->> range10
      (map-to-pair (fn [x] [(if (even? x) 2 1) x]))
      (group-reduce-neutral + 0)
      print)

;;reduce-tree
; tree has to do with internals of spark, to avoid memory problems, result is the same with reduce

;;(1,([2],[5],[6])) (3,([4],[],[]))
(print-pairs (cogroup (seq->pair-rdd [[1 2] [3 4]] spark)
                      (seq->pair-rdd [[1 5]] spark)
                      (seq->pair-rdd [[1 6]] spark)))