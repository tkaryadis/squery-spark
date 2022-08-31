(ns squery-spark.test-mongo-spark-connector
  (:refer-clojure :only [])
  (:require
    [squery-spark.datasets.queries :refer :all]
    [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
    [squery-spark.datasets.stages :refer :all]
    [squery-spark.datasets.operators :refer :all]
    [squery-spark.datasets.schema :refer :all]
    [squery-spark.datasets.rows :refer :all]
    [squery-spark.datasets.utils :refer :all]
    [squery-spark.datasets.udf :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType StructType)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")

(def df (-> spark
            .read
            (.format "mongodb")
            (.option "database" "joy")
            (.option "collection" "messages")
            .load))

(show df)