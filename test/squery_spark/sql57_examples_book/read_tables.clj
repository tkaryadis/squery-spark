(ns squery-spark.sql57_examples_book.read-tables
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all])
  (:refer-clojure))

(defn read-table [spark path filename]
  (-> spark
      (.read)
      (.format "csv")
      (.option "header" "true")
      (.option "inferSchema" "true")
      (.option "wholeFile" true)
      (.option "multiline" true)
      (.load (str path "dbo." filename ".csv"))))