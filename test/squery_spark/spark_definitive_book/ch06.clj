(ns squery-spark.spark-definitive-book.ch06
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
           (org.apache.spark.sql.types DataTypes ArrayType)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;val df = spark.read.format("csv")
;  .option("header", "true")
;  .option("inferSchema", "true")
;  .load("/data/retail-data/by-day/2010-12-01.csv")
;df.printSchema()
;df.createOrReplaceTempView("dfTable")

(def df (-> spark .read (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str data-path "/retail-data/by-day/2010-12-01.csv"))))

(.printSchema df)
(show df)

;import org.apache.spark.sql.functions.lit
;df.select(lit(5), lit("five"), lit(5.0))

(q df [5 "five" 5.0] show)

;df.where(col("InvoiceNo").equalTo(536365))
;  .select("InvoiceNo", "Description")
;  .show(5, false)

(q df
   ((= :InvoiceNo 536365))
   [:InvoiceNo :Description]
   (show 5 false))

;;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
;  .show()

(q df
   ((contains? ["DOT"] :StockCode)
    (or (> :UnitPrice 600) (substring? "POSTAGE" :description)))
   show)

;;val DOTCodeFilter = col("StockCode") === "DOT"
;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
;  .where("isExpensive")
;  .select("unitPrice", "isExpensive").show(5)

(q df
   {:isExpensive (and (= :StockCode "DOT")
                      (or (> :UnitPrice 600) (substring? "POSTAGE" :description)))}
   ((true? :isExpensive))
   [:UnitPrice :isExpensive]
   (show 5))

