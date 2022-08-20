(ns squery-spark.udftest
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
  (:gen-class))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")   ;;CHANGE THIS!!!

;;-----------------------------------------udf---------------------------------------------


;;val udfExampleDF = spark.range(5).toDF("num")
;def power3(number:Double):Double = number * number * number
;val power3udf = udf(power3(_:Double):Double)
;udfExampleDF.select(power3udf(col("num"))).show()
;spark.udf.register("power3", power3(_:Double):Double)
;udfExampleDF.selectExpr("power3(num)").show(2)

;;macros, they expand to def, second one has 1 less argument (doesnt need the number of arguments it takes)
;;fn metadata can help and might change in future
(defudf spark power3 (fn [x] (* x x x)) 1 :long)
(defudf spark power3-1 :long [x] (* x x x))

;;requires aot, main, and run with leinengen
(q spark
   (.range 5)
   (todf "num")
   {:way1 (power3 :num)
    :way2 (power3-1 :num)}
   show)

(defn -main [])