(ns squery-spark.rdds.e04join
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
  (:import (org.apache.spark.api.java.function Function)
           (org.apache.spark.sql Dataset RelationalGroupedDataset Column Row)
           (java.util HashMap)
           (org.apache.spark.rdd RDD)
           (org.apache.spark.api.java JavaRDD))
  (:gen-class))

;;org.apache.spark.api.java.JavaRDD

;;r is the namespace alias with the rdds
;;r also is the macro with rdd dsl , its local shadowing (alternative way instead of r/map to use map directly)
;;r-> is the macro with rdd dsl and -> ,  versions like lmap exists to use with r->
;;r->> also exists
;;r shadows clojure, so we have 3 options, if we want to use clojure.core symbols that are shadowed inside the r enviroment
;;   seperate the clojure code
;;   fn c/map (c alias of clojure.core)
;;   cfn and normal clojure, its like fn but shadows the r, that shadowed clojure
;;   (all those dsl enviroments are few assignements with zero cost in perfomance)

;;when an example will have perfomance problem because of clojure-friendlyness it will be noted each case and why

(def spark (get-spark-session))
(def sc (get-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery/squery-spark/")
(defn -main [])



;;joins(on pairRDDs only they done based on key, and value is sequence)

(def rdd1 ^JavaPairRDD (seq->pair-rdd [[1 2] [2 8] [3 4]] spark))
(def rdd2 ^JavaPairRDD  (seq->pair-rdd [[1 5] [3 6]] spark))
(def rdd3 ^JavaPairRDD  (seq->pair-rdd [[1 7]] spark))

;;inner-join(default,join only if both)(results =  tuple2(common_key,tuple2(v1,v2)))
(r (print (.join rdd1 rdd2)))
(r (print (.join (.join rdd1 rdd2) rdd3)))

;;left outer join (all left join, even if missing(all values are optional type))
;;   (1,(2,Optional[5])) (2,(8,Optional.empty)) (3,(4,Optional[6]))
(r (print (.leftOuterJoin rdd1 rdd2)))

;;right outer join(all right will joined even if missing(optional type wil have in tuple))
(r (print (.rightOuterJoin rdd2 rdd1)))

;;fullouter join => result has all keys from both left and right(no duplicates), and values if they exist else optional

;;cartesian (tuple2=(tupleleft,tupleright)  for all members of the left, join with all members of the right)
;;  join happens only if key exists on the left
(r (print (.cartesian rdd2 rdd1)))