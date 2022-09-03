(ns squery-spark.udaf_test
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:gen-class)
  (:import (accumulators AverageAcc ProductAcc)
           (org.apache.spark.sql Encoders)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(defudaf spark myAverage (AverageAcc.) (Encoders/LONG))

(q spark
   (.range 5)
   (todf "num")
   (group {:avg (myAverage :num)})
   show)

(defudaf spark myProduct (ProductAcc.) (Encoders/LONG))

(q spark
   (.range 5)
   (todf "num")
   {:num (+ 1 :num)}
   (group {:product (myProduct :num)})
   show)

(defn -main [])