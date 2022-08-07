(ns squery-spark.spark_definitive_book.ch02
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.rows :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;val myRange = spark.range(1000).toDF("number")
(def my-range (todf (.range spark 1000) "number"))
(.show my-range)

;;val divisBy2 = myRange.where("number % 2 = 0")
(def divis-by2 (q my-range ((= (mod :number 2) 0))))
(.show divis-by2)

;;divisBy2.count()
(println (q divis-by2 (count-s)))

;;val flightData2015 = spark
;  .read
;  .option("inferSchema", "true")
;  .option("header", "true")
;  .csv("/data/flight-data/csv/2015-summary.csv")

(def ^Dataset flight-data-2015
  (-> spark
      (.read)
      (.option "inferSchema" "true")
      (.option "header" "true")
      (.csv (str data-path "/flight-data/2015-summary.csv"))))

(.show flight-data-2015)

;;flightData2015.take(3)
(-> (take-rows flight-data-2015 3) print-rows)

;;flightData2015.sort("count").explain()
(q flight-data-2015 (sort :count) .explain)

;;spark.conf.set("spark.sql.shuffle.partitions", "5")
(-> spark (.conf) (.set "spark.sql.shuffle.partitions" "5"))

;;flightData2015.sort("count").take(2)
(q flight-data-2015 (sort :count) (take-rows 2) print-rows)

;;flightData2015.createOrReplaceTempView("flight_data_2015")
(.createOrReplaceGlobalTempView flight-data-2015 "flight_data_2015")

;;val sqlWay = spark.sql("""
;SELECT DEST_COUNTRY_NAME, count(1)
;FROM flight_data_2015
;GROUP BY DEST_COUNTRY_NAME
;""")

;;val dataFrameWay = flightData2015
;  .groupBy('DEST_COUNTRY_NAME)
;  .count()

(q flight-data-2015
   (group :DEST_COUNTRY_NAME
          {:nFlights (sum 1)})
   .show)

;;spark.sql("SELECT max(count) from flight_data_2015").take(1)
;;flightData2015.select(max("count")).take(1)

(q flight-data-2015
   (group nil {:maxFlights (max :count)})
   (take-rows 1)
   print-rows)

;;val maxSql = spark.sql("""
;SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
;FROM flight_data_2015
;GROUP BY DEST_COUNTRY_NAME
;ORDER BY sum(count) DESC
;LIMIT 5
;""")
;
;maxSql.show()

;;flightData2015
;.groupBy("DEST_COUNTRY_NAME")
;.sum("count")
;.withColumnRenamed("sum(count)", "destination_total")
;.sort(desc("destination_total"))
;.limit(5)
;.show()

(q flight-data-2015
   (group :DEST_COUNTRY_NAME
          {:destination_total (sum :count)})
   (sort :!destination_total)
   (limit 5)
   .show)




