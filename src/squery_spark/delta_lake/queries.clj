(ns squery-spark.delta-lake.queries
  (:require [squery-spark.utils.utils :refer [string-map]]
            [squery-spark.datasets.internal.common :refer [column string-keys-column-values]])
  (:import (io.delta.tables DeltaTable DeltaMergeBuilder)
           (org.apache.spark.sql Column)))

(defn update-fn [table args]
  (let [filters (filter #(not (map? %)) args)
        and-filters (reduce (fn [value this]
                              (.and value this))
                            (first filters)
                            (rest filters))
        fields (filter map? args)
        fields (apply (partial merge {}) fields)
        fields (string-map fields)]
    (if (empty? filters)
      (.update table fields)
      (.update table and-filters fields))))

(defmacro update- [table & args]
  (let [args (into [] args)]
    `(let ~squery-spark.datasets.operators/operators-mappings
       (squery-spark.delta-lake.queries/update-fn ~table ~args))))

(defn delete-fn [table filters]
  (if (empty? filters)
    (.delete ^DeltaTable table)
    (let [and-filters (reduce (fn [value this]
                                (.and value this))
                              (first filters)
                              (rest filters))]
      (.delete ^DeltaTable table ^Column and-filters))))

(defmacro delete
  ([table & args]
   (let [args (into [] args)]
     `(let ~squery-spark.datasets.operators/operators-mappings
        (squery-spark.delta-lake.queries/delete-fn ~table ~args)))))

(defn update-m [m]
  (partial #(.update %2 %1) (string-keys-column-values m)))

(defn insert-m [m]
  (partial #(.insert %2 %1) (string-keys-column-values m)))

(defn merge- [df1 df2 cond-col]
  (.merge df1 df2 (column cond-col)))

(defn if-matched
  ([builder cond-col action]
   (-> builder
       (.whenMatched (column cond-col))
       action))
  ([builder action]
   (-> builder
       (.whenMatched)
       action)))

(defn if-not-matched
  ([builder cond-col action]
   (-> builder
       (.whenNotMatched (column cond-col))
       action))
  ([builder action]
   (-> builder
       (.whenNotMatched)
       action)))