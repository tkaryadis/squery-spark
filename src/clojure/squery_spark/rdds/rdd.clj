(ns squery-spark.rdds.rdd
  (:refer-clojure :exclude [map reduce filter sort keys vals get
                            count distinct seq take frequencies first
                            max min print])
  (:require [clojure.core :as c])
  (:import (org.apache.spark.api.java.function FlatMapFunction PairFunction Function Function2 VoidFunction)
           (org.apache.spark.api.java JavaRDD JavaPairRDD)
           (scala Tuple2)))

;;-----------------------functions-----------------------------

;;TODO spark projects have those in java files, implementing seriaziableFn if problem maybe change
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

(defn kv [k v]
  (Tuple2. k v))

;;--------------------interop----------------------------------

;;from https://github.com/zero-one-group/geni
(defn to-vec [p]
  (->> (.productArity p)
       (c/range)
       (c/map #(.productElement p %))
       (c/into [])))

(defn kv-to-vec [p]
  [(.productElement p 0) (.productElement p 1)])


;;i can use this syntax also (._1 mytuple) (._2 mytuple) ...
(defn tget [tpl n]
  (.productElement tpl n))

;;-------------------tranformations----------------------------

(defn map [f rdd]
  (.map rdd (F1. f)))

(defn lmap [rdd f]
  (.map rdd (F1. f)))

(defn filter [f rdd]
  (.filter rdd (F1. f)))

(defn lfilter [rdd f]
  (.filter rdd (F1. f)))

(defn map-flat [f rdd]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn lmap-flat [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

;;aggregate(U zeroValue, Function2<U,T,U> seqOp, Function2<U,U,U> combOp)
(defn reduce
  ([f rdd] (.reduce rdd (F2. f)))
  ([f1 f2-combine init-value rdd] (.aggregate rdd init-value (F2. f1) (F2. f2-combine))))

(defn reduce-tree
  "tree has to do with internals of spark, to avoid memory problems, result is the same with reduce"
  ([f rdd] (.treeReduce rdd (F2. f)))
  ([f1 f2-combine init-value rdd] (.treeAggregate rdd init-value (F2. f1) (F2. f2-combine)))
  ([f1 f2-combine depth init-value rdd] (.treeAggregate rdd init-value (F2. f1) (F2. f2-combine) depth)))

;;reduce operation on the partitions is not deterministic
(defn lreduce [rdd f]
  (.reduce rdd (F2. f)))

(defn void-map [rdd f]
  (.foreach rdd (VF. f)))

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

;;-----------------------rdd-to-pairrdd----------------------------------

;;this is what i use to return vector from clojure function and auto become tutple2 for rdd-pair
(defn map-to-pair
  [f rdd]
  (.mapToPair rdd (Pair. f)))

;;special case of map-to-pair, [(f value), value], map-to-pair simpler
(defn to-kv [f rdd]
  (.keyBy rdd (F1. f)))

;;map_ to (Tuple. a1 a2)  dont produce a JavaPairRDD (rdd with tutples produce)so i have to use mapToPair for it
(defn lmap-to-pair [rdd f]
  (.mapToPair rdd (Pair. f)))

;;-----------------JavaPairRDD the pair is a scala tuple2-------------------

(defn group
  "[k1 [v1,v2 ...] ..]
   returns JavaPairRDD<K,Iterable<V>>"
  [pair-rdd]
  (.groupByKey pair-rdd))

;;cogroup(JavaPairRDD<K,W1> other1, JavaPairRDD<K,W2> other2, JavaPairRDD<K,W3> other3)

(defn group-reduce
  "2args
    [k1 (reduce f [v1,v2 ...]) ...] f should be associative and commutative
   4 args
    [k1 (reduce f1 f2 init [v1,v2 ...]) ...] f1 is used in the same partition, f2 will combine the results from many partitions
   "
  ([f pair-rdd] (.reduceByKey pair-rdd (F2. f)))
  ([f1 f2-combine init-value pair-rdd] (.aggregateByKey pair-rdd init-value (F2. f1) (F2. f2-combine))))

(defn group-reduce-fn
  "like group-reduce but instead of init value, i give a function to produce it from the first k,v"
  [f1-combiner f2-merger init-function pair-rdd]
  (.combineByKey pair-rdd (F1. init-function) (F2. f1-combiner)  (F2. f2-merger)))

(defn group-reduce-neutral
  "like group reduce, but instead of initial value, or initial-fn
   give a neutral value to start with like 0 for addition or 1 for mul etc"
  [f neutral-value pair-rdd]
  (.foldByKey pair-rdd neutral-value (F2. f)))

(defn group-count
  "[k1 (+ v1 v2 ...) ..] , special case of group-reduce where f=+
   returns java.util.Map<K,Long>"
  [pair-rdd]
  (.countByKey pair-rdd))

(defn cogroup
  "like group but does it in 'the union' of the rdfs"
  ([pair-rdd1 pair-rdd2] (.cogroup pair-rdd1 pair-rdd2))
  ([pair-rdd1 pair-rdd2 pair-rdd3] (.cogroup pair-rdd1 pair-rdd2 pair-rdd3)))

(defn map-values [f pair-rdd ]
  (.mapValues pair-rdd (F1. f)))

(defn lmap-values [pair-rdd f]
  (.mapValues pair-rdd (F1. f)))

(defn map-flat-values [f pair-rdd]
  (.flatMapValues pair-rdd (FlatMap. f)))

(defn lmap-flat-values
  "[k,v] => (k,f=[v1 v2 ...]) (flatmap)=> (k,v1) (k,v2) ..."
  [pair-rdd f]
  (.flatMapValues pair-rdd (FlatMap. f)))

(defn keys [pair-rdd]
  (.keys pair-rdd))

(defn vals [pair-rdd]
  (.values pair-rdd))

(defn get [pair-rdd k]
  (.lookup pair-rdd k))

(defn sample-by-key
  ([pair-rdd with-replacement? fractions-map] (.sampleByKey pair-rdd with-replacement? fractions-map))
  ([pair-rdd with-replacement? fractions-map seed] (.sampleByKey pair-rdd with-replacement? fractions-map seed)))

(defn sample-by-key-exact
  ([pair-rdd with-replacement? fractions-map] (.sampleByKeyExact pair-rdd with-replacement? fractions-map))
  ([pair-rdd with-replacement? fractions-map seed] (.sampleByKeyExact pair-rdd with-replacement? fractions-map seed)))

;;map-to-pair if rdd is not already in tuple2
(defn to-pair-rdd [rdd-tuple2-pairs]
  (JavaPairRDD/fromJavaRDD rdd-tuple2-pairs))


(defn count-by-key-approx
  ([pair-rdd timeout] (.countByKeyApprox pair-rdd timeout))
  ([pair-rdd timeout confidence] (.countByKeyApprox pair-rdd timeout confidence)))

;;TODO add more types

(defn join
  ([rdd rdd-other] (.join rdd rdd-other))
  ([rdd rdd-other npartitions] (.join rdd rdd-other npartitions)))

(defn zip
  "Zips this RDD with another one, returning key-value pairs with the first element in each RDD, second element in each RDD
   they must have the same number of elements else error"
  [rdd rdd-other]
  (.zip rdd rdd-other))

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
  (dorun (c/map (c/comp c/print (partial str " ") #(.toString %)) (-> rdd (.collect))))
  (c/println))

(defn print-pairs [paired-rdd]
  (dorun (c/map (c/comp c/print (partial str " ") #(.toString %)) (-> paired-rdd (.collect))))
  (c/println))

(defn collect [rdd]
  (.collect rdd))

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