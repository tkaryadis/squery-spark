(ns squery-spark.delta-lake-docs.p0quickstart
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all])
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


;;Dataset<Row> newData = spark.range(0, 20).toDF();
;
;deltaTable.as("oldData")
;  .merge(
;    newData.as("newData"),
;    "oldData.id = newData.id")
;  .whenMatched()
;  .update(
;    new HashMap<String, Column>() {{
;      put("id", functions.col("newData.id"));
;    }})
;  .whenNotMatched()
;  .insertExpr(
;    new HashMap<String, Column>() {{
;      put("id", functions.col("newData.id"));
;    }})
;  .execute();
;
;deltaTable.toDF().show();

(println "Before merge")
(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) (.show 100))

(def new-data (-> spark (.range 20)))

(println "Before merge1")
(.show new-data)

(sq-> (merge- (as delta-table :oldData)
              (as new-data :newData)
              (= :oldData.id :newData.id))
      (if-matched (update-m {:id (+ :newData.id 100)}))
      (if-not-matched (insert-m {:id :newData.id}))
      (.execute))

(println "After merge")
(-> spark .read (.format "delta") (.load (str data-path "/delta-table")) (.show 100))

;;;stopped on Read older versions of data using time travel