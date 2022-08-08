(ns squery-spark.datasets.queries
  (:require [squery-spark.datasets.internal.query :refer [pipeline]]
            [squery-spark.datasets.operators]
            [squery-spark.datasets.internal.common :refer [nested2]]
            [squery-spark.utils.utils :refer [string-map]])
  (:import (io.delta.tables DeltaTable)
           (org.apache.spark.sql Column)))

;;q
;; 1)translates to ->
;; 2)macro is used to convert [] to project, {} to add fields, () to fitlers etc
;;   and to create the enviroment overriding some of clojure.core, only inside the q scope
;;   to access clojure.core inside the q scope use alias like c/str
;; 3)functions is used for everything else that can run on runtime
(defmacro q [qform & qforms]
  (let [qforms (pipeline qforms)
        ;_ (prn "qforms" qforms)
        query (concat (list '->) (list qform) qforms)
        ;- (prn "query" query)
        ]
    `(let ~squery-spark.datasets.operators/operators-mappings
       ~query)))

(defmacro sq [arg]
  `(let ~squery-spark.datasets.operators/operators-mappings
     ~arg))

(defn update-fn [table args]
  (let [filters (filter #(not (map? %)) args)
        and-filters (reduce (fn [value this]
                               (.and value this))
                             (first filters)
                             (rest filters))
        fields (filter map? args)
        fields (apply (partial merge {}) fields)
        fields (string-map fields)]
    (.update table and-filters fields)))

(defmacro update- [table & args]
  (let [args (into [] args)]
    `(let ~squery-spark.datasets.operators/operators-mappings
       (squery-spark.datasets.queries/update-fn ~table ~args))))

;;delete()
;Delete data from the table.
;void	delete(org.apache.spark.sql.Column condition)
;Delete data from the table that match the given condition.

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
        (squery-spark.datasets.queries/delete-fn ~table ~args)))))
