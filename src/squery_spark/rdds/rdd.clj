(ns squery-spark.rdds.rdd
  (:refer-clojure :exclude [map reduce filter sort keys vals get
                            count distinct seq take frequencies first
                            max min print])
  (:require [clojure.core :as c])
  (:import (org.apache.spark.api.java.function FlatMapFunction PairFunction Function Function2 VoidFunction)
           (org.apache.spark.api.java JavaRDD)
           (scala Tuple2)))

;;-----------------------functions-----------------------------

(defrecord VF [f]
  VoidFunction
  (call [this x]
    (f x)))

(defrecord F1 [f]
  Function
  (call [this x]
    (f x)))

(defrecord F2 [f]
  Function2
  (call [this x y]
    (f x y)))

(defrecord FlatMap [f]
  FlatMapFunction
  (call [this x]
    (.iterator (f x))))

(defrecord Pair [f]
  PairFunction
  (call [this x]
    (c/let [vector-pair (f x)]
      (Tuple2. (c/first vector-pair) (c/second vector-pair)))))

(defn spair [k v]
  (Tuple2. k v))

;;--------------------interop----------------------------------

;;from https://github.com/zero-one-group/geni
(defn scala-tuple->vec [p]
  (->> (.productArity p)
       (c/range)
       (c/map #(.productElement p %))
       (c/into [])))

(defn pair [p]
  [(.productElement p 0) (.productElement p 1)])

;;-------------------tranformations----------------------------

;;f argument must produce an iterable sequence for example seq,vector,list
;;that will be flatted
(defn flat-map [f rdd]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn lflat-map [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn map [f rdd]
  (.map ^JavaRDD rdd (F1. f)))

(defn lmap [rdd f]
  (.map ^JavaRDD rdd (F1. f)))

(defn reduce [f rdd]
  (.reduce rdd (F2. f)))

;;reduce operation on the partitions is not deterministic
(defn lreduce [rdd f]
  (.reduce rdd (F2. f)))

(defn void-map [rdd f]
  (.foreach rdd (VF. f)))

(defn filter [f rdd]
  (.filter rdd (F1. f)))

(defn lfilter [rdd f]
  (.filter rdd (F1. f)))

(defn sort [f rdd]
  (.sortBy rdd (F1. f) true 1))

(defn lsort [rdd f]
  (.sortBy rdd (F1. f) true 1))

(defn sort-desc [f rdd]
  (.sortBy rdd (F1. f) false 1))

(defn lsort-desc [rdd f]
  (.sortBy rdd (F1. f) false 1))

(defn random-split [rdd & weights]
  (.randomSplit rdd (double-array weights)))

(defn mapPartitions [rdd f]
  (.mapPartitions rdd (FlatMap. f)))

(defn mapPartitionsWithIndex [rdd f preservesPartitioning]
  (.mapPartitionsWithIndex rdd (F2. f) preservesPartitioning))

;;-----------------transformations Pair related-------------------

(defn map-to-pair [f rdd]
  (.mapToPair rdd (Pair. f)))

;;map_ to (Tuple. a1 a2)  dont produce a JavaPairRDD (rdd with tutples produce)so i have to use mapToPair for it
(defn lmap-to-pair [rdd f]
  (.mapToPair rdd (Pair. f)))

(defn reduce-by-key [f rdd]
  (.reduceByKey rdd (F2. f)))

(defn lreduce-by-key [rdd f]
  (.reduceByKey rdd (F2. f)))

(defn key-by [f rdd]
  (.keyBy rdd (F1. f)))

(defn lkey-by
  "returns [f-result,key] scala.Tuple2"
  [rdd f]
  (.keyBy rdd (F1. f)))

(defn map-values [f pair-rdd ]
  (.mapValues pair-rdd (F1. f)))

(defn lmap-values [pair-rdd f]
  (.mapValues pair-rdd (F1. f)))

(defn flat-map-values [f pair-rdd]
  (.flatMapValues pair-rdd (FlatMap. f)))

(defn lflat-map-values
  "[k,v] => (k,f=[v1 v2 ...]) (flatmap)=> (k,v1) (k,v2) ..."
  [pair-rdd f]
  (.flatMapValues pair-rdd (FlatMap. f)))

(defn keys [rdd]
  (.keys rdd))

(defn vals [rdd]
  (.values rdd))

(defn get [rdd k]
  (.lookup rdd k))


;;---------------------------------------------------

(defn count [rdd]
  (.count rdd))

(defn distinct [rdd]
  (.distinct rdd))

(defn seq [rdd]
  (.collect rdd))

(defn take [n rdd]
  (.take rdd n))

(defn take-ordered
  ([n rdd] (.takeOrdered rdd n))
  ([n comp rdd] (.takeOrdered rdd n comp)))

(defn ltake-ordered
  ([rdd n] (.takeOrdered rdd n))
  ([rdd n comp] (.takeOrdered rdd n comp)))

(defn top
  ([n rdd] (.top rdd n))
  ([n comp rdd] (.top rdd n comp)))

(defn ltop
  ([rdd n] (.top rdd n))
  ([rdd n comp] (.top rdd n comp)))

(defn frequencies [rdd]
  (.countByValue rdd))

(defn first [rdd]
  (.first rdd))

(defn max [rdd comp]
  (.max rdd comp))

(defn min [rdd comp]
  (.min rdd comp))

;;---------------------------------------------------

(defn print [rdd]
  (dorun (c/map c/print (-> rdd (.collect))))
  (c/println))

(defn print-pairs [paired-rdd]
  (dorun (c/map (c/comp c/print pair) (-> paired-rdd (.collect))))
  (c/println))

(defn j-rdd [df]
  (-> df .rdd .toJavaRDD))

;;---------------------------------------------------dsl---------------------------------------

(def rdd-operators-mappings
  '[
    map squery-spark.rdds.rdd/map
    reduce squery-spark.rdds.rdd/reduce
    filter squery-spark.rdds.rdd/filter
    sort squery-spark.rdds.rdd/sort
    keys squery-spark.rdds.rdd/keys
    vals squery-spark.rdds.rdd/vals 
    get squery-spark.rdds.rdd/get
    count squery-spark.rdds.rdd/count
    distinct squery-spark.rdds.rdd/distinct
    seq squery-spark.rdds.rdd/seq
    take squery-spark.rdds.rdd/take
    frequencies squery-spark.rdds.rdd/frequencies
    first squery-spark.rdds.rdd/first
    max squery-spark.rdds.rdd/max
    min squery-spark.rdds.rdd/min
    print squery-spark.rdds.rdd/print

    ])

(def rdd-clojure-mappings
  '[
    map clojure.core/map
    reduce clojure.core/reduce
    filter clojure.core/filter
    sort clojure.core/sort
    keys clojure.core/keys
    vals clojure.core/vals
    get clojure.core/get
    count clojure.core/count
    distinct clojure.core/distinct
    seq clojure.core/seq
    take clojure.core/take
    frequencies clojure.core/frequencies
    first clojure.core/first
    max clojure.core/max
    min clojure.core/min
    print clojure.core/print

    ])

(defmacro r [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     ~@args))

(defmacro r-> [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     (-> ~@args)))

;;(fn [word] (* (c/count word) -1))
(defmacro cfn [args body]
  `(let ~squery-spark.rdds.rdd/rdd-clojure-mappings
     (fn ~args ~body)))