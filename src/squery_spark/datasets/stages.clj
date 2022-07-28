(ns squery-spark.datasets.stages
  (:refer-clojure :exclude [sort])
  (:require [clojure.core :as c]
            [squery-spark.datasets.internal.common :refer [columns single-maps]]
            [squery-spark.utils.utils :refer [string-map]])
  (:import (org.apache.spark.sql Dataset Column)
           [org.apache.spark.sql functions RelationalGroupedDataset]
           (java.util HashMap)))

;;project(select) stage can be 3 ways
;; col
;; add new coll
;;   anonymous
;;   with new name
;; [:CustomerID (lit 5) {:price (coll "UnitPrice")}]
;; (.select ^Dataset (into-array [(col "CustomerID")  (lit 5) (.as (col "UnitPrice") "price")]))
(defn select
  "[:CustomerID (lit 5) {:price (coll \"UnitPrice\")}]
   (.select ^Dataset (into-array [(col \"CustomerID\")  (lit 5) (.as (col \"UnitPrice\") \"price\")]))"
  [df & fields]
  (let [                                                    ;_ (prn "fields-before" fields)
        fields (columns fields)
        ;_ (prn "fields-after" fields)
        field-array (into-array Column fields)
        ;_ (prn "xx" field-array)
        ]
    (.select ^Dataset df field-array)))

;;	withColumns(java.util.Map<String,Column> colsMap)
(defn add-columns [df m]
  (let [m (reduce (fn [m1 k]
                    (let [v (get m k)
                          v (cond
                              (keyword? v)
                              (functions/col (name v))

                              (not (instance? org.apache.spark.sql.Column v))
                              (functions/lit v)

                              :else
                              v)]
                      (assoc m1 (name k) v)))
                  {}
                  (keys m))
        m (HashMap. m)]
    (.withColumns df m)))

(defn filter-columns [df fs]
  (let [and-filters (reduce (fn [value this]
                              (.and value this))
                            (first fs)
                            (rest fs))]
    (.filter df and-filters)))

(defn unset [df & col-names]
  (.drop df (into-array String (map name col-names))))

(defn sort
  "DataFrame orderBy"
  [df & cols]
  (prn cols)
  (.orderBy df (into-array Column (columns cols))))

(defn group
  "DataFrame group by"
  [df & cols-acc]
  (let [cols (filter #(not (map? %)) cols-acc)
        acc (filter #(map? %) cols-acc)
        acc-maps (single-maps (into [] acc))
        group (.groupBy df (into-array Column (columns cols)))
        acc (mapv (fn [m]
                    (let [field-name (name (first (keys m)))
                          acc-value (first (vals m))]
                      (.as acc-value field-name)))
                  acc-maps)
        acc-first ^Column (first acc)
        acc-rest (if (> (count acc) 1)
                   (into-array Column (rest acc))
                   (into-array Column []))]
    (if acc-first
      (.agg ^RelationalGroupedDataset group acc-first acc-rest)
      group)))

#_(defn cube [data-frame & cols]
  (.cube data-frame (ca cols)))


