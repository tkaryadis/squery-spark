(ns squery-spark.datasets.stages
  (:refer-clojure :exclude [sort])
  (:require [clojure.core :as c]
            [squery-spark.datasets.internal.common :refer [columns column-keyword column single-maps]]
            [squery-spark.utils.utils :refer [string-map]])
  (:import (org.apache.spark.sql Dataset Column)
           [org.apache.spark.sql functions RelationalGroupedDataset]
           (java.util HashMap)
           (org.apache.spark.sql.expressions Window WindowSpec)))

;;Operators for Datasets

;;project(select) stage can be 3 ways
;; col
;; add new coll
;;   anonymous
;;   with new name
;; [:CustomerID (lit 5) {:price (coll "UnitPrice")}]
;; (.select ^Dataset (into-array [(col "CustomerID")  (lit 5) (.as (col "UnitPrice") "price")]))
;;TODO select can take also arguments like "*" in spark
(defn select
  "[:CustomerID (lit 5) {:price (coll \"UnitPrice\")}]
   (.select ^Dataset (into-array [(col \"CustomerID\")  (lit 5) (.as (col \"UnitPrice\") \"price\")]))"
  [df & fields]
  (.select ^Dataset df (into-array Column (columns fields))))


;;TODO df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns,maybe {!field1 : field2}
(defn add-columns [df m]
  (let [;m
        #_(reduce (fn [m1 k]
                    (let [v (get m k)]
                      (assoc m1 (name k) (column v))))
                  {}
                  (keys m))
        ;m (HashMap. m)
        df (reduce (fn [df k]
                     (.withColumn df (name k) (column (c/get m k))))
                   df
                   (keys m))
        ]
    ;(.withColumns ^Dataset df m)
    df
    ))

(defn filter-columns [df fs]
  (let [and-filters (reduce (fn [value this]
                              (.and value this))
                            (first fs)
                            (rest fs))]
    (.filter df and-filters)))

(defn unset [df & cols]
  (let [cols (columns cols)]
    (reduce (fn [value this]
              (.drop value this))
            (.drop df (first cols))
            (rest cols))))

;;asc_nulls_last asc_nulls_first  desc_...
(defn sort
  "DataFrame orderBy"
  [df & cols]
  (let [cols (mapv (fn [col]
                     (let [desc? (and (keyword? col) (clojure.string/starts-with? (name col) "!"))
                           nl?   (and (keyword? col) (clojure.string/ends-with? (name col) "!"))
                           col (if desc? (keyword (subs (name col) 1)) col)
                           col (if nl? (keyword (subs (name col) 0 (dec (count (name col))))) col)
                           col (column-keyword col)
                           col (cond
                                 (and desc? nl?)
                                 (.desc_nulls_last ^Column col)

                                 desc?
                                 (.desc ^Column col)

                                 nl?
                                 (.asc_nulls_last ^Column col)

                                 :else
                                 col)]
                       col))
                   cols)]
    (.orderBy df (into-array Column cols))))

;;TODO FIX THE NIL THE DROP NO NEED
(defn group
  "Call ways
    No accumulators
    (group :field1 :field2 ...) = select and distinct
    Group and accumulators
    (group :field1 :field2 ...
           {:acField1 (acc-f :field)})
    No group (result will have only the acc fields)
    (group nil
           {:acField1 (acc-f :field)})"
  [df & cols-acc]
  (let [[cols acc]  (if (vector? (first cols-acc))
                      [(first cols-acc) (rest cols-acc)]
                      [(filter #(not (map? %)) cols-acc) (filter #(map? %) cols-acc)])
        first-col (first cols)
        acc-maps (single-maps (into [] acc))]
    (if (empty? acc-maps)
      (.distinct ^Dataset (apply (partial select df) cols))
      (let [group (.groupBy df (into-array Column (columns cols)))
            acc (mapv (fn [m]
                        (let [field-name (name (first (keys m)))
                              acc-value (first (vals m))]
                          (.as acc-value field-name)))
                      acc-maps)
            acc-first  (first acc)
            acc-rest (if (> (count acc) 1)
                       (into-array Column (rest acc))
                       (into-array Column []))]
        (if acc-first
          (if (nil? first-col)
            (unset (.agg ^RelationalGroupedDataset group acc-first acc-rest)
                   :NULL)
            (.agg ^RelationalGroupedDataset group acc-first acc-rest))
          group)))))

(defn count-s [df]
  (.count df))

(defn limit [df n]
  (.limit df n))

(defn join-eq
  "Equality join"
  ([df1 df2 col]
   (let [col (if (keyword? col) (name col) col)
         join-result (.join df1 df2 (.equalTo (.col df1 col) (.col df2 col)))]
     (.drop join-result (.col df2 col))))
  ([df1 df2 col1 col2]
   (let [col1 (if (keyword? col1) (name col1) col1)
         col2 (if (keyword? col2) (name col2) col2)]
     (.join df1 df2 (.equalTo (.col df1 col1) (.col df2 col2)))))
  ([df1 df2 col1 col2 join-type]
   (let [col1 (if (keyword? col1) (name col1) col1)
         col2 (if (keyword? col2) (name col2) col2)]
     (.join df1 df2 (.equalTo (.col df1 col1) (.col df2 col2)) (name join-type)))))


(defn join
  ([df1 df2 join-condition]
   (.join df1 df2 join-condition))
  ([df1 df2 join-condition join-type]
   (.join df1 df2 join-condition (name join-type))))

(defn union-with [df1 df2]
  (.union df1 df2))

(defn as [df new-alias]
  (.as df (name new-alias)))