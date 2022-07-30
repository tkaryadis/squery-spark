(ns squery-spark.sql-book-squery.read-tables
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.sql-functions :refer [col]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all])
  (:refer-clojure)
  (:import (org.apache.spark.sql Dataset RelationalGroupedDataset Column)
           (java.util HashMap)))

(defn read-table [spark path filename]
  (-> spark
      (.read)
      (.format "csv")
      (.option "header" "true")
      (.option "inferSchema" "true")
      (.option "wholeFile" true)
      (.option "multiline" true)
      (.load (str path "dbo." filename ".csv"))))
