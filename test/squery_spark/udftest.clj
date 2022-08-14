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
  (:require [clojure.core :as c]
            [clojure.main :refer [demunge]]
            )
  (:import (org.apache.spark.sql functions Column Dataset)
           (org.apache.spark.sql.expressions Window WindowSpec))
  (:gen-class)
  )

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")   ;;CHANGE THIS!!!

(def t1 (-> spark (.range 10)))
(show t1)


;;-------------------------UDF-------------------------------------

#_(defudf spark myudf1 (fn [x] (* x 2)) 1 :long)
#_(defudf spark myudf2 :long [x] (* x 2))

#_(q t1
   {:a (myudf1 :id)}
   show)

#_(q t1
   {:a (myudf2 :id)}
   show)

(defnscala1 z (fn [x] (+ x 30)))

(q t1
   {:a [1 2 3]}
   [(map z :a)]
   show)

(q t1
   {:a [1 2 3]}
   [(map1 (fn [x] (+ x 40)) :a)]
   show)

(defn -main [])