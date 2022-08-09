(ns squery-spark.delta-lake-docs.p1ReadsWrites
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (io.delta.tables DeltaTable)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/delta-lake-docs/")   ;;CHANGE THIS!!!

;;Tables can be defined with metastore(needs config options on spark) or tables defined by path

;(try (-> spark (.table "default.mytable") delete) (catch Exception e ""))

(-> (DeltaTable/forPath (c/str data-path "/mytable")) delete)

;;-------------------Create--------------------------------------------------------------

;;i can also add  .partitionedBy("afield") etc
;;location is optional for tables defined in the metastore
;;if table not in metastore, drop table wont delete the table data

(-> (DeltaTable/createOrReplace spark)
    (table-columns [:some :col [:names :long false]])
    (.property "description" "mytable")
    (.tableName "default.mytable")
    (.location (str data-path "/mytable"))
    (.execute))

;;--------------Insert data, overwrite-----------------------------------------------------
(def my-schema (build-schema [:some :col [:names :long false]]))
(def mydf (seq->df spark [["Hello" nil 1]] my-schema))
(.show mydf)

(-> mydf .write (.format "delta") (.mode "overwrite") (.saveAsTable "default.mytable")) ;;metastore way, default database

;(-> mydf .write (.format "delta") (.mode "overwrite") (.save (str data-path "/delta-table"))) ;;with path


;;------------------Read data------------------------------------------------------------------

(-> spark (.table "default.mytable") .show)

;(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) .show)  ;;path

;;----------------------------------Insert data, append------------------------------------------
(def mydf2 (seq->df spark [["Helloz" "delta" 0]] my-schema))
(.show mydf2)

(-> mydf2 .write (.format "delta") (.mode "append") (.saveAsTable "default.mytable"))

(-> spark (.table "default.mytable") .show)

;;TODO skipped Control data location, Use generated columns




