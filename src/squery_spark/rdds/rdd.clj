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
(defn rflat-map [f rdd]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn lflat-map [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn rmap [f rdd]
  (.map ^JavaRDD rdd (F1. f)))

(defn lmap [rdd f]
  (.map ^JavaRDD rdd (F1. f)))

;;reduce operation on the partitions is not deterministic
(defn lreduce [rdd f]
  (.reduce rdd (F2. f)))

(defn rreduce [f rdd]
  (.reduce rdd (F2. f)))

(defn void-map [rdd f]
  (.foreach rdd (VF. f)))

(defn rfilter [f rdd]
  (.filter rdd (F1. f)))

(defn lfilter [rdd f]
  (.filter rdd (F1. f)))

(defn lsort [rdd f]
  (.sortBy rdd (F1. f) true 1))

(defn rsort [f rdd]
  (.sortBy rdd (F1. f) true 1))

(defn lsort! [rdd f]
  (.sortBy rdd (F1. f) false 1))

(defn rsort! [f rdd]
  (.sortBy rdd (F1. f) false 1))

(defn random-split [rdd & weights]
  (.randomSplit rdd (double-array weights)))

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

(defn rcount [rdd]
  (.count rdd))

(defn rdistinct [rdd]
  (.distinct rdd))

(defn rlist [rdd]
  (.collect rdd))

(defn ltake [rdd n]
  (.take rdd n))

(defn rtake [n rdd]
  (.take rdd n))

;;java.util.List<T>	takeOrdered(int num)
;Returns the first k (smallest) elements from this RDD using the natural ordering for T while maintain the order.
;java.util.List<T>	takeOrdered(int num, java.util.Comparator<T> comp)
;Returns the first k (smallest) elements from this RDD as defined by the specified Comparator[T] and maintains the order.

(defn ltake-ordered
  ([rdd n] (.takeOrdered rdd n))
  ([rdd n comp] (.takeOrdered rdd n comp)))

(defn rtake-ordered
  ([n rdd] (.takeOrdered rdd n))
  ([n comp rdd] (.takeOrdered rdd n comp)))

;;java.util.List<T>	top(int num)
;Returns the top k (largest) elements from this RDD using the natural ordering for T and maintains the order.
;java.util.List<T>	top(int num, java.util.Comparator<T> comp)
;Returns the top k (largest) elements from this RDD as defined by the specified Comparator[T] and maintains the order.

(defn rtop
  ([n rdd] (.top rdd n))
  ([n comp rdd] (.top rdd n comp)))

(defn ltop
  ([rdd n] (.top rdd n))
  ([rdd n comp] (.top rdd n comp)))

(defn rfrenquencies [rdd]
  (.countByValue rdd))

(defn rfirst [rdd]
  (.first rdd))

(defn rmax [rdd comp]
  (.max rdd comp))

(defn rmin [rdd comp]
  (.min rdd comp))

;;---------------------------------------------------

(defn rprint [rdd]
  (dorun (map println (-> rdd (.collect)))))

(defn j-rdd [df]
  (-> df .rdd .toJavaRDD))