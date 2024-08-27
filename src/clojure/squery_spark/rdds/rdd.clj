(ns squery-spark.rdds.rdd
  (:refer-clojure :exclude [map reduce filter sort keys vals
                            seq take frequencies
                            max min print println])
  (:require [clojure.core :as c])
  (:import (org.apache.spark.api.java.function FlatMapFunction PairFunction Function Function2 VoidFunction)
           (org.apache.spark.api.java JavaRDD JavaPairRDD)
           (scala Tuple2)))

;;Clojure wrappers are used only when it makes sense
;;1)a function arg is needed
;;2)clojure core library has different name (sometimes the diffrent name is kept)
;; examples where no wrappers  .distinct, .first, .count .zip etc

;;------------------------------functions----------------------------------

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

;;--------------------interop----------------------------------

;;from https://github.com/zero-one-group/geni ??maybe better way?
(defn v [p]
  (->> (.productArity p)
       (c/range)
       (c/mapv #(.productElement p %))))

(defn pv [pair]
  [(.productElement pair 0) (.productElement pair 1)])

(defn vp [pair-vector]
  (Tuple2. (c/get pair-vector 0) (c/get pair-vector 1)))

(defn p [k v]
  (Tuple2. k v))

(defn seq-pair
  "clojure map to a sequence of tuples(pairs)"
  [m]
  (c/map (fn [pair] (p (c/first pair) (c/second pair))) m))

;;i can use this syntax also (._1 mytuple) (._2 mytuple) ...
(defn pget [tpl n]
  (.productElement tpl n))

(defn p0 [tpl]
  (._1 tpl))

(defn p1 [tpl]
  (._2 tpl))

;;-------------------tranformations----------------------------

(defn map [f rdd]
  (.map rdd (F1. f)))

(defn lmap [rdd f]
  (.map rdd (F1. f)))

(defn filter [f rdd]
  (.filter rdd (F1. f)))

(defn lfilter [rdd f]
  (.filter rdd (F1. f)))

(defn flatmap
  "f must return iterator, and then its flatten clojure collections work as return types"
  [f rdd]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn lflatmap [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn map-partitions
  "rdd has many members, and each partition has some of this members
   map partition takes as argument all the members of a partition
   (iterator-seq arg-iterator) arg is iterator so i can convert to seq
   result of f is also an iterable collection(like vec map etc they work ok)
   Faster than map on each member(but custom more code)
     1)less f calls once per partition vs 1 per member of the rdd,
       also maybe i need to make enviroment for example with broadcast variables
       that costs for each call
     2)local aggregation on each partition, reduce or avoid the need
       for grouping-aggregate the rdd (main reason)
     3)reducing the rdd members locally, each partition might result to
       much less members than it had, also reduces the need for external filter"
  ([f1 rdd] (.mapPartitions rdd (FlatMap. f1)))
  ([f1 rdd preserves-partitioning?] (.mapPartitions rdd (FlatMap. f1) preserves-partitioning?)))

(defn map-partitions-indexed
  ([f2 rdd] (.mapPartitionsWithIndex rdd (F2. f2) true))
  ([f2 rdd preserves-partitioning?] (.mapPartitionsWithIndex rdd (F2. f2) preserves-partitioning?)))

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


;;for each go to each partition, and works in multithreaded way
;;so can lose sort order if used for printing
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

;;-----------------------rdd-to-pairrdd----------------------------------

(defn pair-rdd-from-pair [rdd-tuple2-pairs]
  "used to create a JavaPairRDD from an RDD that contains Tuple2 members"
  (JavaPairRDD/fromJavaRDD rdd-tuple2-pairs))

(defn pair-rdd
  "rdd with vecs of size 2, to rdd-pair"
  [rdd]
  (.mapToPair rdd (Pair. identity)))

(defn map-to-pair
  "f should return a collection, the first two members become the pair(scala.Tuple2),
   for example return a vector"
  [f rdd]
  (.mapToPair rdd (Pair. f)))

;;map_ to (Tuple. a1 a2)  dont produce a JavaPairRDD (rdd with tutples produce)so i have to use mapToPair for it
(defn lmap-to-pair [rdd f]
  (.mapToPair rdd (Pair. f)))

(defn map-to-pair-key [f rdd]
  "restricted case of map-to-pair, where i want to keep the value as it is, and i want to apply f to value to get the key
   returns pairRDD with tuple2=[f-return-value value]"
  (.keyBy rdd (F1. f)))


;;---------------------------JavaPairRDD(on pairs)-----------------------------

(defn group
  "group on a list
   [k1 [v1,v2 ...] ..]
   returns JavaPairRDD<K,Iterable<V>>"
  [pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.groupByKey pair-rdd)))


(defn group-reduce
  "group+reduce on the group
   first group by key so i can image [  [k1,[v1 v2 ...]   [k2,....]    ]
   f is applied to the values, its 2 args f, keeps the state, and takes the v1 etc as second argument
   2args
    f is 2 args function and is applied to value
   4 args init-value
    f1 is used inside the partition, f2 is used to combine the partitions, they are both reduction functions
    2 args state and next member
   4 args fn for init-value
     group-reduce with fn to produce the init-value from the first [k,v]
     init-value = init-function(first-pair-value when combining)
     init-function is used within the partition"
  ([f pair-rdd]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.reduceByKey pair-rdd (F2. f))))
  ([f2-seq f2-comb init-value-or-fn pair-rdd]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (if (fn? init-value-or-fn)
       (.combineByKey pair-rdd (F1. init-value-or-fn) (F2. f2-seq)  (F2. f2-comb))
       (.aggregateByKey pair-rdd
                        init-value-or-fn
                        (F2. f2-seq)
                        (F2. f2-comb))))))

(defn group-reduce-neutral
  "like group reduce, but instead of initial value, or initial-fn
   give a neutral value to start with like 0 for addition or 1 for mul etc"
  [f neutral-value pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.foldByKey pair-rdd neutral-value (F2. f))))

(defn group-count
  "[k1 (+ v1 v2 ...) ..] , special case of group-reduce where f=+
   returns java.util.Map<K,Long>"
  [pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.countByKey pair-rdd)))

(defn cogroup
  "gorup by key, and combine the values of each in a collection"
  ([pair-rdd1 pair-rdd2] (.cogroup pair-rdd1 pair-rdd2))
  ([pair-rdd1 pair-rdd2 pair-rdd3] (.cogroup pair-rdd1 pair-rdd2 pair-rdd3)))

(defn map-values
  "f takes as argument the pair, and the result will be the new-value,
   the key remains unchanged"
  [f pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.mapValues pair-rdd (F1. f))))

(defn lmap-values [pair-rdd f]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.mapValues pair-rdd (F1. f))))

(defn flatmap-values [f pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.flatMapValues pair-rdd (FlatMap. f))))

(defn lflatmap-values
  "[k,v] => (k,f=[v1 v2 ...]) (flatmap)=> (k,v1) (k,v2) ..."
  [pair-rdd f]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.flatMapValues pair-rdd (FlatMap. f))))

(defn keys [pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.keys pair-rdd)))

(defn vals [pair-rdd]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.values pair-rdd)))

(defn lookup [pair-rdd k]
  (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
    (.lookup pair-rdd k)))

(defn sample-by-key
  ([pair-rdd with-replacement? fractions-map]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.sampleByKey pair-rdd with-replacement? fractions-map)))
  ([pair-rdd with-replacement? fractions-map seed]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.sampleByKey pair-rdd with-replacement? fractions-map seed))))

(defn sample-by-key-exact
  ([pair-rdd with-replacement? fractions-map]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.sampleByKeyExact pair-rdd with-replacement? fractions-map)))
  ([pair-rdd with-replacement? fractions-map seed]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.sampleByKeyExact pair-rdd with-replacement? fractions-map seed))))

(defn count-by-key-approx
  ([pair-rdd timeout]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.countByKeyApprox pair-rdd timeout)))
  ([pair-rdd timeout confidence]
   (let [pair-rdd (if (instance? org.apache.spark.api.java.JavaPairRDD pair-rdd) pair-rdd (JavaPairRDD/fromJavaRDD pair-rdd))]
     (.countByKeyApprox pair-rdd timeout confidence))))

;;pairRDD to RDD-------------------------------------------------------------------------------------

(defn rdd [pair-rdd]
  (.toJavaRDD (.rdd pair-rdd)))

;;----------------------------------------------------------------------------------------------------

(defn seq [rdd]
  (.collect rdd))

(defn take
  "returns list not rdd, its action"
  [n rdd]
  (.take rdd n))

(defn take-asc
  ([n rdd] (.takeOrdered rdd n))
  ([n comp rdd] (.takeOrdered rdd n comp)))

(defn ltake-asc
  ([rdd n] (.takeOrdered rdd n))
  ([rdd n comp] (.takeOrdered rdd n comp)))

(defn take-dsc
  ([n rdd] (.top rdd n))
  ([n comp rdd] (.top rdd n comp)))

(defn ltake-dsc
  ([rdd n] (.top rdd n))
  ([rdd n comp] (.top rdd n comp)))

(defn frequencies [rdd]
  (.countByValue rdd))

(defn max [rdd comp]
  (.max rdd comp))

(defn min [rdd comp]
  (.min rdd comp))

;;---------------------------------------------------

(defn print [rdd]
  (dorun (c/map (c/comp c/print (partial str " ") #(.toString %)) (-> rdd (.collect))))
  (c/println))

(defn println [rdd]
  (dorun (c/map (c/comp c/print (partial str "\n") #(.toString %)) (-> rdd (.collect))))
  (c/println))

(defn print-pairs [paired-rdd]
  (dorun (c/map (c/comp c/print (partial str " ") #(.toString %)) (-> paired-rdd (.collect))))
  (c/println))

(defn collect [rdd]
  (.collect rdd))

(defn collect-pairs [rdd]
  (mapv #(v %) (.collect rdd)))

(defn df->rdd [df]
  "Uses the .rdd method of the dataframe, and then .toJavaRDD method of rdd"
  (-> df .rdd .toJavaRDD))

;;---------------------------------------------------dsl---------------------------------------

(def rdd-operators-mappings
  '[
    ;;clojure
    map squery-spark.rdds.rdd/map
    reduce squery-spark.rdds.rdd/reduce
    filter squery-spark.rdds.rdd/filter
    sort squery-spark.rdds.rdd/sort
    keys squery-spark.rdds.rdd/keys
    vals squery-spark.rdds.rdd/vals
    seq squery-spark.rdds.rdd/seq
    take squery-spark.rdds.rdd/take
    frequencies squery-spark.rdds.rdd/frequencies
    max squery-spark.rdds.rdd/max
    min squery-spark.rdds.rdd/min
    print squery-spark.rdds.rdd/print
    println squery-spark.rdds.rdd/println

    ;;not-clojure-optional to avoid refer
    p squery-spark.rdds.rdd/p
    v squery-spark.rdds.rdd/v
    vp squery-spark.rdds.rdd/vp
    pv squery-spark.rdds.rdd/pv
    seq-pair squery-spark.rdds.rdd/seq-pair
    pget squery-spark.rdds.rdd/pget
    p0 squery-spark.rdds.rdd/p0
    p1 squery-spark.rdds.rdd/p1
    lmap squery-spark.rdds.rdd/lmap
    lfilter squery-spark.rdds.rdd/lfilter
    flatmap squery-spark.rdds.rdd/flatmap
    lflatmap squery-spark.rdds.rdd/lflatmap
    reduce-tree squery-spark.rdds.rdd/reduce-tree
    lreduce squery-spark.rdds.rdd/lreduce
    void-map squery-spark.rdds.rdd/void-map
    lsort squery-spark.rdds.rdd/lsort
    sort-desc squery-spark.rdds.rdd/sort-desc
    lsort-desc squery-spark.rdds.rdd/lsort-desc
    random-split squery-spark.rdds.rdd/random-split
    map-partitions squery-spark.rdds.rdd/map-partitions
    map-partitions-indexed squery-spark.rdds.rdd/map-partitions-indexed
    map-to-pair squery-spark.rdds.rdd/map-to-pair
    map-vec-to-pair squery-spark.rdds.rdd/map-to-pair
    map-to-pair-key squery-spark.rdds.rdd/map-to-pair-key
    lmap-to-pair squery-spark.rdds.rdd/lmap-to-pair
    group squery-spark.rdds.rdd/group
    group-reduce squery-spark.rdds.rdd/group-reduce
    group-reduce-neutral squery-spark.rdds.rdd/group-reduce-neutral
    group-count squery-spark.rdds.rdd/group-count
    cogroup squery-spark.rdds.rdd/cogroup
    map-values squery-spark.rdds.rdd/map-values
    lmap-values squery-spark.rdds.rdd/lmap-values
    flatmap-values squery-spark.rdds.rdd/flatmap-values
    lflatmap-values squery-spark.rdds.rdd/lflatmap-values
    sample-by-key squery-spark.rdds.rdd/sample-by-key
    sample-by-key-exact squery-spark.rdds.rdd/sample-by-key-exact
    pair-rdd squery-spark.rdds.rdd/pair-rdd
    count-by-key-approx squery-spark.rdds.rdd/count-by-key-approx
    ;join-rdd squery-spark.rdds.rdd/join-rdd
    ;zip squery-spark.rdds.rdd/zip
    rdd squery-spark.rdds.rdd/rdd
    take-asc squery-spark.rdds.rdd/take-asc
    ltake-asc squery-spark.rdds.rdd/ltake-asc
    take-dsc squery-spark.rdds.rdd/take-dsc
    ltake-dsc squery-spark.rdds.rdd/ltake-dsc
    print-pairs squery-spark.rdds.rdd/print-pairs
    collect squery-spark.rdds.rdd/collect
    collect-pairs squery-spark.rdds.rdd/collect-pairs
    df->rdd squery-spark.rdds.rdd/df->rdd
    lookup squery-spark.rdds.rdd/lookup
    ])

;;30 sec 10billion times
;;if problem i use c/... or do a walk to use only the symbols really used inside the function
(def rdd-clojure-mappings
  '[
    map clojure.core/map
    reduce clojure.core/reduce
    filter clojure.core/filter
    sort clojure.core/sort
    keys clojure.core/keys
    vals clojure.core/vals
    seq clojure.core/seq
    take clojure.core/take
    frequencies clojure.core/frequencies
    max clojure.core/max
    min clojure.core/min
    print clojure.core/print
    println clojure.core/println
    ])

;;------------------------------------------macros---------------------------------------------

(defmacro r [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     ~@args))

(defmacro rlet [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     (let ~@args)))

(defmacro r-> [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     (-> ~@args)))

(defmacro r->> [& args]
  `(let ~squery-spark.rdds.rdd/rdd-operators-mappings
     (->> ~@args)))

;;(fn [word] (* (c/count word) -1))
(defmacro cfn [args body]
  `(let ~squery-spark.rdds.rdd/rdd-clojure-mappings
     (fn ~args ~body)))

(defn vec-to-t [v]
  (if (c/and (c/vector? v) (= (c/count v) 2))
    (let [a1 (vec-to-t (c/first v))
          a2 (vec-to-t (c/second v))]
      (p a1 a2))
    v))

(defn t-to-vec [t]
  (if (instance? Tuple2 t)
    (pv t)
    t))

(defn fix-args [args]
  (c/into [] (c/apply c/concat (c/map (c/fn [v]
                                    [v `(t-to-vec ~v)])
                                  args))))

(defmacro pfn [args body]
  `(let ~squery-spark.rdds.rdd/rdd-clojure-mappings
     (fn ~args
       (let ~(fix-args args)
         (vec-to-t ~body)))))