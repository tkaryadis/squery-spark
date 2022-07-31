(ns squery-spark.datasets.rows)


(defn get-field [row field]
  (.getAs row (name field)))