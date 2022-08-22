(ns squery-spark.rdds.rdd
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
    (let [vector-pair (f x)]
      (Tuple2. (first vector-pair) (second vector-pair)))))

(defn t [k v]
  (Tuple2. k v))

;;-------------------tranformations----------------------------

;;f argument must produce an iterable sequence for example seq,vector,list
;;that will be flatted
(defn flat-map-r [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn rmap [f rdd]
  (.map ^JavaRDD rdd (F1. f)))

(defn lmap [rdd f]
  (.map ^JavaRDD rdd (F1. f)))

;;reduce operation on the partitions is not deterministic
(defn reduce-r [rdd f]
  (.reduce rdd (F2. f)))

(defn foreach [rdd f]
  (.foreach rdd (VF. f)))

(defn filter-r [rdd f]
  (.filter rdd (F1. f)))

(defn sort-r [rdd f]
  (.sortBy rdd (F1. f) true 1))

(defn mapPartitions [rdd f]
  (.mapPartitions rdd (FlatMap. f)))

(defn mapPartitionsWithIndex [rdd f preservesPartitioning]
  (.mapPartitionsWithIndex rdd (F2. f) preservesPartitioning))

;;-----------------transformations Pair related-------------------

;;TODO
;;map_ to (Tuple. a1 a2)  dont produce a JavaPairRDD (rdd with tutples produce)so i have to use mapToPair for it
(defn mapToPair [rdd f]
  (.mapToPair rdd (Pair. f)))

(defn reduceByKey [rdd f]
  (.reduceByKey rdd (F2. f)))

(defn keyBy [rdd f]
  (.keyBy rdd (F1. f)))

(defn mapValues [pair-rdd f]
  (.mapValues pair-rdd (F1. f)))

(defn flatMapValues [pair-rdd f]
  (.flatMapValues pair-rdd (F1. f)))

;;---------------------------------------------------

(defn print-rdd [rdd]
  (dorun (map println (-> rdd (.collect)))))

(defn j-rdd [df]
  (-> df .rdd .toJavaRDD))