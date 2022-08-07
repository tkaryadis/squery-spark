(ns squery-spark.spark_definitive_book.ch04
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.rows :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark SparkContext)
           (org.apache.spark.sql.types ByteType)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;// in Scala
;val df = spark.range(500).toDF("number")
;df.select(df.col("number") + 10)

(q (.range spark 500)
   (todf :number)
   [(+ :number 10)]
   .show)


;// COMMAND ----------
;
;// in Scala
;spark.range(2).toDF().collect()

(q (.range spark 2)
   todf
   .collectAsList
   print-rows)

