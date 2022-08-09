(ns squery-spark.datasets.queries
  (:require [squery-spark.datasets.internal.query :refer [pipeline]]
            [squery-spark.datasets.operators]
            [squery-spark.utils.utils :refer [string-map]]
            [squery-spark.datasets.internal.common :refer [column]])
  (:import (io.delta.tables DeltaTable DeltaMergeBuilder)
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
    (if (empty? filters)
      (.update table fields)
      (.update table and-filters fields))))

(defmacro update- [table & args]
  (let [args (into [] args)]
    `(let ~squery-spark.datasets.operators/operators-mappings
       (squery-spark.datasets.queries/update-fn ~table ~args))))

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


;;;deltaTable.as("oldData")
;;  .merge(
;;    newData.as("newData"),
;;    "oldData.id = newData.id")
;;  .whenMatched()
;;  .update(
;;    new HashMap<String, Column>() {{
;;      put("id", functions.col("newData.id"));
;;    }})
;;  .whenNotMatched()
;;  .insertExpr(
;;    new HashMap<String, Column>() {{
;;      put("id", functions.col("newData.id"));
;;    }})
;;  .execute();

(defn mupdate [m]
  (partial #(.update %2 %1) (string-map m)))

(defn minsert [m]
  (partial #(.insert %2 %1) (string-map m)))

(defn tmerge [df1 df2 cond-col]
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