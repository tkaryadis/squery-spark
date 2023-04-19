(ns squery-spark.spark_definitive_book.ch21
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.streams :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import
    (org.apache.spark.sql SparkSession Dataset)
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark SparkContext)
    (org.apache.spark.sql.streaming Trigger)
    (org.apache.spark.sql.types ByteType)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")

;;Streaming dont do auto schema inference,so to do it i have to say it by
;;  spark.sql.streaming.schemaInference to true

;;Make a Stream from data,using the schema we already know they have

;;Stream operations
;;1)specify source+options
;;2)specify query
;;3)specify sink+options
;;4)start streaming

;; .writeStream
;; .queryName("aname")      ;;i can use it to get the table  (.table spark "aname") for example to print contents
;; .format("memory")        ;;store in memory for developement only
;; .outputMode("complete")  ;;rewrite the results
;;  .start()                ;;starts streaming async

;;to print the results i can query the table in memory (print-stream activity-counts-stream 100 2000)
;;or i can use sink the console and print them there

;;because streams run async, i have to sleep else error (spark will terminate with stream running) !!

;;// in Scala
;val static = spark.read.json("/data/activity-data/")
;val dataSchema = static.schema

(def static (-> spark (.read) (.json (str data-path "/activity-data/"))))
(def data-schema (.schema static))

;// in Scala
;val streaming = spark.readStream.schema(dataSchema)
;  .option("maxFilesPerTrigger", 1).json("/data/activity-data")
;

;;streaming will be a Dataset
(def streaming (-> spark
                   .readStream
                   (.schema data-schema)
                   (.option "maxFilesPerTrigger" 1)         ;;i think means that every 1 file => trigger => sink
                   (.json (str data-path "/activity-data/"))))

;// in Scala
;val activityCounts = streaming.groupBy("gt").count()

(def activity-counts (q streaming
                        (group :gt
                               {:count (count-acc)})))

;// in Scala
;val activityQuery = activityCounts.writeStream.queryName("activity_counts")
;  .format("memory").outputMode("complete")
;  .start()

(def activity-query (-> activity-counts
                        .writeStream
                        (.queryName "activityCounts")       ;;random name
                        (.format "memory")                  ;;memory sink
                        (.outputMode "complete")            ;;ovewrite old results
                        .start))

;(prn (-> spark .streams .active))

;;activityQuery.awaitTermination()

;;block and wait the streaming to end
;;streams are supposed to run forever,so they run in background
;;this will start the stream in background,and here will not run
;;at all if driver program will exit after this

;(.awaitTermination activity-query)    ;;block and wait for the stream to end

;;the stream is now running(suppose not await above)
;;to not terminate the drive program i can write a loop with sleep
;;also i can query the data while the stream is running

;;get the table that the results are stored (name of the query we used)
(def activity-counts-stream (.table spark "activityCounts"))

;(print-stream activity-counts-stream 100 2000)

;;----------------filter-addFields-------------------------------------------------


;;val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
;  .where("stairs")
;  .where("gt is not null")
;  .select("gt", "model", "arrival_time", "creation_time")
;  .writeStream
;  .queryName("simple_transform")
;  .format("memory")
;  .outputMode("append")
;  .start()

(def simple-transform-q
  (q streaming
     {:stairs (find? "%stairs%" :gt)}
     ((true? :stairs) (some? :gt))
     [:gt :model :arrival_time :creation_time]))

(def simple-transform-q-stream
  (-> simple-transform-q
      .writeStream
      (.queryName "simple_transform")
      (.format "memory")
      (.outputMode "append")
      .start))

;(print-stream (.table spark "simple_transform") 100 2000)

;;-----------------------aggr------------------------------------------------------

;;;;val deviceModelStats = streaming.cube("gt", "model").avg()
;;.drop("avg(Arrival_time)")
;;.drop("avg(Creation_Time)")
;;.drop("avg(Index)")
;;.writeStream.queryName("device_counts").format("memory").outputMode("complete")
;;.start()

(def stream-q  (q streaming
                  (cube :gt :model)
                  (avg-s)
                  (unset (col "avg(Arrival_time)")
                         (col "avg(Creation_time)")
                         (col "avg(Index)"))
                  .writeStream
                  (.queryName "device_counts")
                  (.format "memory")
                  (.outputMode "complete")
                  .start))

;(print-stream (.table spark "device_counts") 10 2000)


;;--------------sink=console and set trigger time--------------------------------

;;activityCounts.writeStream.trigger(Trigger.ProcessingTime("100 seconds"))
;.format("console").outputMode("complete").start()

(q activity-counts
   .writeStream
   (.trigger (Trigger/ProcessingTime "1 seconds"))
   (.format "console")
   (.outputMode "complete")
   .start)

(Thread/sleep 100000)

;;done except last streaming dataset api