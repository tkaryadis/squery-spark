(ns squery-spark.rdds.e08partitions
  (:require [clojure.test :refer :all]))

;;reduce the number of partitions(useful when data became smaller after transformations so no need many)
;; less partitions => shuffle faster
;;coalesce(number_of_partitions)