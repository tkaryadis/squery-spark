(ns squery-spark.datasets.rows
  (:import (org.apache.spark.sql Row RowFactory)))


(defn row [& vls]
  (RowFactory/create (into-array Object vls)))

(defn get-field [row field]
  (.getAs row (name field)))

(defn print-rows [rows-list]
  (let [rows-list (if (instance? Row rows-list) [rows-list] rows-list)]
    (dorun (map (comp println #(.toString %)) rows-list))))

