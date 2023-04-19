(ns squery-spark.rdds.utils
  (:require [squery-spark.rdds.rdd :refer [map-partitions-indexed]]))

(defn read-csv-no-header [spark-context csv-path]
  (let [rdd (-> spark-context (.textFile csv-path))]
    (map-partitions-indexed (fn [idx iter]
                              (if (= idx 0)
                                (do (.next iter )
                                    iter)
                                iter))
                            rdd)))