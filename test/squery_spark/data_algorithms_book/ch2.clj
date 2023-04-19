(ns squery-spark.data-algorithms-book.ch2
  (:refer-clojure :only [])
  (:use [squery-spark.datasets.queries]
        [squery-spark.datasets.stages]
        [squery-spark.datasets.operators]
        [squery-spark.datasets.rows]
        [squery-spark.datasets.utils]
        [squery-spark.rdds.rdd])
  (:refer-clojure)
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context get-java-spark-context]]
            [squery-spark.rdds.rdd :as r :refer [rlet r r-> r->> cfn cfn-p]]
            [squery-spark.datasets.stages]
            [clojure.pprint :refer [pprint]]
            [squery-spark.datasets.queries :refer [q]]
            [squery-spark.datasets.utils :refer [seq->rdd seq->pair-rdd seq->df show]]
            [clojure.string :as s]
            [clojure.core :as c])
  (:gen-class)
  (:import (org.apache.spark.rdd PairRDDFunctions)
           (org.apache.spark.sql Dataset)
           (org.apache.spark.sql.types DataTypes)))

(def spark (get-spark-session))
(def sc (get-java-spark-context spark))
(.setLogLevel (get-spark-context spark) "ERROR")


(def data-path "/home/white/IdeaProjects/squery-spark/data-used/data-algorithms/")
(defn -main [])

;;DNA is set of dna strings
;;dna strings are made from {A, C, G, T}
;;adenine (A), cytosine (C), guanine (G), and thymine (T).

;;perfomance on spark can vary for example using
;;Basic MapReduce or In-mapper combiner or Mapping partitions
;;goal is to reduce the shuffling time
;;goal is to do the operations locally and then combine the local results

;;sample.fasta, group by letters and sum the occurences, >seq... is like letter z once

(def lines (-> sc (.textFile (str data-path "/ch2/sample.fasta"))))

;;sol1
;;problem = not much locally, i can process the line more, instead of producing so many [letter,1] pairs
(r->> lines
      (map-flat (cfn [l]
                  (let [l (s/trim (s/lower-case l))]
                    (if (s/starts-with? l ">")
                      [(p "z" 1)]
                      (into [] (map (fn [c] (p (str c) 1)) l))))))
      pair-rdd
      (group-reduce +)
      print)

;;sol2, do max in line locally, less pairs, still map works per line not per partition
(r->> lines
      (map-flat (cfn [l]
                  (let [l (s/trim (s/lower-case l))]
                    (if (s/starts-with? l ">")
                      [(p "z" 1)]
                      (mapv (fn [v]
                              (p (get v 0) (get v 1)))
                            (frequencies (map str l)))))))
      pair-rdd
      (group-reduce +)
      print)

;;sol3, do max locally, but per-partition not per line, rdd is made from lines, but each partition contains many lines
;;  so here i map per partition taking as argument all the lines the partition has
(r->> lines
      (map-partitions (cfn [ls]
                        (let [letters (flatten (map (fn [l] (if (clojure.string/starts-with? l ">")
                                                          "z"
                                                          (map (comp s/lower-case str) l)))
                                                (iterator-seq ls)))]
                          (map (fn [v]
                                  (p (get v 0) (get v 1)))
                                (frequencies letters)))))
      pair-rdd
      (group-reduce +)
      print)

;;sol4 like sol3 but with nested reduce
(r->> lines
      (map-partitions (cfn [ls]
                        (seq-pair
                          ;;letters map(frequencies)
                          (reduce (fn [m line]
                                    (if (clojure.string/starts-with? line ">")
                                      (update m "z" (fnil inc 1))
                                      (reduce (fn [m c]
                                                (update m (s/lower-case c) (fnil inc 1)))
                                              m
                                              (map str line))))
                                  {}
                                  (iterator-seq ls)))))
      pair-rdd
      (group-reduce +)
      print)



(time (loop [x 10000000000]
        (when-not (zero? x)
                  (recur (dec x)))))

(time (loop [x 10000000000]
        (let [
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
              ]
          (when-not (zero? x)
            (recur (dec x))))))