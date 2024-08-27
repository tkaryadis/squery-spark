(ns squery-spark.rdds.e01create-and-various
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

;;org.apache.spark.api.java.JavaRDD

;;from collection
(def my-collection (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple" #"\s"))
(def words ^JavaRDD (seq->rdd my-collection 2 spark))
(prn (type words))
(r/print words)

;;give name to RDD
(.setName words "myRDD")
(prn (.name words))

;;from dataframe
(-> spark
    (.range 10)
    df->rdd                                                 ;;JavaRDD
    r/print)

;;pair rdds----------------------------------------------------------------------------------------

;;org.apache.spark.api.java.JavaPairRDD
;;each member is scala.Tuple2

;;pair RDD like rdd with tuples2 (not like hash maps)

;;from sequence(collection with collection members or Tuple2)
(r (print (seq->pair-rdd [[1 2] [3 4]] spark)))

(def range10 ^JavaRDD (-> spark (.range 10) df->rdd))

;;rdd -> pairRDD from rdd+function
;;f should return a collection, first 2 members become the pair (a vec works also, as java collection)
(r (print (map-to-pair (fn [x] [1 x]) range10)))
(prn (type (map-to-pair (fn [x] [1 x]) range10)))
(prn (type (.first (map-to-pair (fn [x] [1 x]) range10))))

;;restricted version of map-to-pair, where i keep the value as if, and f is applied to value to ge the key
(->> range10
     (map-to-pair-key (fn [x] (inc x)))      ;;keep the value as is, and key to be x+1
     r/print)

;;rdd -> pairRDD from rdd that contains tuples
(-> range10
    (lmap (fn [x] (p x x)))                                 ;;each member become a Tuple2 (p is from pair)
    pair-rdd-from-pair
    r/print)

;;rdd -> pairRDD from rdd that contains collections
(-> range10
    (lmap (fn [x] [x x]))               ;;member is collection so the first 2 will be converted to pair
    pair-rdd
    r/print)

(r (print (keys (seq->pair-rdd [["k1" "v1"] ["k2" "v2"]] spark)))
   (print (vals (seq->pair-rdd [["k1" "v1"] ["k2" "v2"]] spark)))
   (print-pairs (seq->pair-rdd [["k1" "v1"] ["k2" "v2"]] spark))
   (c/println (.lookup ^JavaPairRDD (seq->pair-rdd [["k1" "v1"] ["k2" "v2"]] spark) "k1"))
   )

;;------------------------------------various-----------------------------------------------------

;;using local dsl and r-----------------------------------------------------------

;;using clojure core functions inside a local dsl enviroment inside r, here c/map
(r (print (map (fn [v] (into [] (c/map inc v))) (seq->rdd [[1 2] [3 4] [5 6]] spark))))

;;alternative way with extra perfomance cost on function call is to use cfn, to make clojure core back inside that function
;;cfn adds a clojure core dsl, with 10 assignments
(r (print (map (cfn [v] (into [] (map inc v)))
               (seq->rdd [[1 2] [3 4] [5 6]] spark))))

;;if the above 2 are not good enough, only other solution is to define the function outside of r enviroment

;;sort
(r (print (sort (fn [v] (get v 1)) (seq->rdd [[0 1] [2 3] [0 0]] spark))))
(r (print (sort-desc (fn [v] (get v 1)) (seq->rdd [[0 1] [2 3] [0 0]] spark))))

;;count
(-> spark
    (.range 10)
    ^JavaRDD df->rdd
    .count
    prn)

;;distinct
(prn (r (count (distinct (seq->rdd [1 1 2 3] spark)))))

;;TODO simpler way? cant find toDF for java
(-> spark
    (.range 10)
    df->rdd
    (lmap (fn [n] (row n)))
    (rdd->df spark [[:id :long]])
    show)

(let [rdd (seq->rdd (range 1 21) spark)]
  (prn [(r/max rdd <) (r/min rdd <)]))

(def fity-fifty-split (random-split words 0.5 0.5))
(dorun (map r/print fity-fifty-split))

(r (print (.distinct ^JavaRDD (seq->rdd [1 2 3 2 2 2 5] spark))))

;;TODO
#_(r (print-pairs
       (sample-by-key (to-kv (cfn [w] (s/lower-case (first w))) words)
                      true
                      sample-map
                      6)))


;;TODO
;(print-pairs (zip words num-range))

;;weird things -----------------------------------------------????????????????????????????????????????????????????

;;its false, maybe internal spark type?
(r (print (map
            (fn [x] (true? x))
            (map (fn [v] (c/get v 1)) (seq->rdd [[1 true] [2 false] [5 true]] spark)))))
