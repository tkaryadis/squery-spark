(ns squery-spark.json-structs.jsontest
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all]
            [squery-spark.datasets.udf :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType)
           (java.util Arrays)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/json-structs/")

;;val df = spark.read.format("json")
;.load("/data/flight-data/json/2015-summary.json")

;;val peopleDF = spark.read.json(path)
(def json-df
  (-> spark
      .read
      (.option "multiline" "true")
      (.json (str data-path "json-collection.json"))))

(.printSchema json-df)
(show json-df false)


(q json-df
   ((= :name "takis"))
   {:pet-names (get :pets-details [:name 0])}
   (show false))