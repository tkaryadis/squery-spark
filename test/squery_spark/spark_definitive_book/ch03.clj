(ns squery-spark.spark_definitive_book.ch03
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
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark SparkContext)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;import spark.implicits._
;case class Flight(DEST_COUNTRY_NAME: String,
;                  ORIGIN_COUNTRY_NAME: String,
;                  count: BigInt)
;val flightsDF = spark.read
;  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
;val flights = flightsDF.as[Flight]


;;TODO CLASS + DATASET

;;flights
;  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
;  .map(flight_row => flight_row)
;  .take(5)

;;TODO UDF

;;flights
;  .take(5)
;  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
;  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

;;TODO UDF

;;val staticDataFrame = spark.read.format("csv")
;  .option("header", "true")
;  .option("inferSchema", "true")
;  .load("/data/retail-data/by-day/*.csv");
;staticDataFrame.createOrReplaceTempView("retail_data")
;val staticSchema = staticDataFrame.schema

(def static-data-frame
  (-> spark
      (.read)
      (.format "csv")
      (.option "header" "true")
      (.load (str data-path "retail-data/by-day/*.csv"))))

(def static-schema (.schema static-data-frame))

;;import org.apache.spark.sql.functions.{window, column, desc, col}
;staticDataFrame
;  .selectExpr(
;    "CustomerId",
;    "(UnitPrice * Quantity) as total_cost",
;    "InvoiceDate")
;  .groupBy(
;    col("CustomerId"), window(col("InvoiceDate"), "1 day"))
;  .sum("total_cost")
;  .show(5)

(q static-data-frame
   [:CustomerId :InvoiceDate {:total-cost (* :UnitPrice :Quantity)}]
   (group :CustomerId (window :InvoiceDate "1 day")
          {:total-cost (sum :total-cost)})
   (.show 5))


;;spark.conf.set("spark.sql.shuffle.partitions", "5")
(-> spark (.conf) (.set "spark.sql.shuffle.partitions" "5"))

;val streamingDataFrame = spark.readStream
;    .schema(staticSchema)
;    .option("maxFilesPerTrigger", 1)
;    .format("csv")
;    .option("header", "true")
;    .load("/data/retail-data/by-day/*.csv")

(def streaming-data-frame
  (-> spark
      (.readStream)
      (.schema static-schema)
      (.option "maxFilesPerTrigger" 1)
      (.format "csv")
      (.option "header" "true")
      (.load (str data-path "/retail-data/by-day/*.csv"))))

(println "Is Streaming = " (.isStreaming streaming-data-frame))

;;val purchaseByCustomerPerHour = streamingDataFrame
;  .selectExpr(
;    "CustomerId",
;    "(UnitPrice * Quantity) as total_cost",
;    "InvoiceDate")
;  .groupBy(
;    $"CustomerId", window($"InvoiceDate", "1 day"))
;  .sum("total_cost")

(def purchase-by-customer-per-hour
  (q streaming-data-frame
     [:CustomerId :InvoiceDate {:total-cost (* :UnitPrice :Quantity)}]
     (group :CustomerId (window :InvoiceDate "1 day")
            {:total-cost (sum :total-cost)})))

;;purchaseByCustomerPerHour.writeStream
;    .format("memory") // memory = store in-memory table
;    .queryName("customer_purchases") // the name of the in-memory table
;    .outputMode("complete") // complete = all the counts should be in the table
;    .start()

#_(prn "spark= " (.isStopped ^SparkContext (get-spark-context spark)))

;;TODO ERROR MicroBatchExecution: Query customer_purchases
#_(-> purchase-by-customer-per-hour
    (.writeStream)
    (.format "memory")
    (.queryName "customer_purchases")
    (.outputMode "complete")
    (.start))

;;spark.sql("""
;  SELECT *
;  FROM customer_purchases
;  ORDER BY `sum(total_cost)` DESC
;  """)
;  .show(5)

;;TODO in memory stream

;;staticDataFrame.printSchema()
(.printSchema static-data-frame)


;;// in Scala
;import org.apache.spark.sql.functions.date_format
;val preppedDataFrame = staticDataFrame
;  .na.fill(0)
;  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
;  .coalesce(5)

(def prepped-data-frame
  (q static-data-frame
     (.na)
     (.fill 0)
     {:day_of_week (date-to-string :InvoiceDate "EEEE")}
     (.coalesce 5)))


;// in Scala
;val trainDataFrame = preppedDataFrame
;  .where("InvoiceDate < '2011-07-01'")
;val testDataFrame = preppedDataFrame
;  .where("InvoiceDate >= '2011-07-01'")

(def train-data-frame
  (q prepped-data-frame ((< :InvoiceDate "2011-07-01"))))

(def test-data-frame
  (q prepped-data-frame ((>= :InvoiceDate "2011-07-01"))))


;trainDataFrame.count()
;testDataFrame.count()

(println (q train-data-frame (count-s)))
(println (q test-data-frame (count-s)))

;;TODO has more ML examples