(ns squery-spark.delta-lake-docs.p0quickstart
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (io.delta.tables DeltaTable)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/delta-lake-docs/")   ;;CHANGE THIS!!!

;;Write table (schema is inferred, and column is auto-named id)
(def data (-> spark (.range 5)))
(-> data .write (.format "delta") (.mode "overwrite") (.save (str data-path "/delta-table")))

;;Read table
(def df (-> spark .read (.format "delta") (.load (str data-path "/delta-table"))))
(show df)

;;Ovewrite table
(def data (-> spark (.range 5 10)))
(-> data .write (.format "delta") (.mode "overwrite") (.save (str data-path "/delta-table")))

(def delta-table (DeltaTable/forPath (str data-path "/delta-table")))

(println "Before update")
(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) .show)

(update- delta-table (= (mod :id 2) 0) {:id (+ :id 100)})

(println "After update")
(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) .show)

(delete delta-table (= (mod :id 2) 0))

(println "After delete")
(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) .show)



