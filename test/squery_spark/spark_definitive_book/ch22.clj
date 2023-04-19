(ns squery-spark.spark_definitive_book.ch22
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

(def static (-> spark (.read) (.json (str data-path "/activity-data/"))))

(def streaming (-> spark
                   .readStream
                   (.schema (.schema static))
                   (.option "maxFilesPerTrigger" 10)
                   (.json (str data-path "/activity-data/"))))

;;// in Scala
;val withEventTime = streaming.selectExpr(
;"*",
;"cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

(def with-event-time
  (q streaming
     {:event_time (timestamp (div (double :creation_time)
                                  1000000000))}))

;;scala
;;withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
;.writeStream
;.queryName("events_per_window")
;.format("memory")
;.outputMode("complete")
;.start()

;;Tumbling windows (not overlaping windows each event in 1 window)
(def with-event-time-q (q with-event-time
                          (group (duration :event_time "10 minutes")
                                 {:count (count-acc)})
                          .writeStream
                          (.queryName "events_per_window")
                          (.format "memory")
                          (.outputMode "complete")
                          .start))

;(print-stream (.table spark "events_per_window") 100 1000)

;;sliding windows (overlaping, window-size=10, take every 5 minutes)
(def with-event-time-q1 (q with-event-time
                          (group (duration :event_time "10 minutes" "5 minutes")
                                 {:count (count-acc)})
                          .writeStream
                          (.queryName "events_per_window1")
                          (.format "memory")
                          (.outputMode "complete")
                          .start))

;(print-stream (.table spark "events_per_window1") 100 1000)

;;watermark => allows spark to finalize(close the window) after the time passes
;;             (no more data can be insert to a window, even if comes will discared)
;;i always use watermark, because without it spark cannot close the window,
;; and stores all those data in memory (with closed window it needs only the results)

;;in "append" mode the information wont be outputed until the window closes

(def with-event-time-q2 (q with-event-time
                           (water-mark :event_time "30 minutes")
                           (group (duration :event_time "10 minutes" "5 minutes")
                                  {:count (count-acc)})
                           .writeStream
                           (.queryName "events_per_window2")
                           (.format "memory")
                           (.outputMode "complete")
                           .start))

;;(print-stream (.table spark "events_per_window2") 100 1000)

;;dropping Duplicates

;;withEventTime
;.withWatermark("event_time", "5 seconds")
;.dropDuplicates("User", "event_time")
;.groupBy("User")
;.count()
;.writeStream
;.queryName("deduplicated")
;.format("memory")
;.outputMode("complete")
;.start()


(def with-event-time-q3
  (q with-event-time
     (water-mark :event_time "5 seconds")
     (drop-duplicates :user :event_time)
     (group :user
            {:count (count-acc)})
     .writeStream
     (.queryName "deduplicated")
     (.format "memory")
     (.outputMode "complete")
     .start))

(print-stream (.table spark "deduplicated") 100 1000)

;;done before Arbitrary Stateful Processing