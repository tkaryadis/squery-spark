(ns squery-spark.spark-definitive-book.ch30
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            )
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset Dataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType)
           (java.util Arrays)
           (org.graphframes GraphFrame)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")

(def bike-stations
  (-> spark
      .read
      (.option "header" "true")
      (.csv (str data-path "bike-data/201508_station_data.csv"))))

(def trip-data
  (-> spark
      .read
      (.option "header" "true")
      (.csv (str data-path "bike-data/201508_trip_data.csv"))))

(def station-vertices (q bike-stations
                        (rename {:id :name})
                        (select-distinct)))

(def trip-edges (q trip-data
                   (rename {:src "Start Station"
                            :dst "End Station"})))

;(show station-vertices)
;(show trip-edges)

;;vertices dataframe must have `id` field for the name of the node
;;edges must have src+dest fields

(def station-graph (GraphFrame. station-vertices trip-edges))
(.cache station-graph)

(prn (-> station-graph .vertices .count))
(prn (-> station-graph .edges .count))
(prn (q trip-data count-s))

(q station-graph
   .edges
   (group :src :dst
          {:count (count-acc)})
   (sort :!count)
   (show 10))

(q station-graph
   .edges
   ((or (= :src "Townsend at 7th") (= :dst "Townsend at 7th")))
   (group :src :dst
          {:count (count-acc)})
   (sort :!count)
   (show 10))


(def town-and-7th-edges
  (q station-graph
     .edges
     ((or (= :src "Townsend at 7th") (= :dst "Townsend at 7th")))))

(def subgraph (GraphFrame. (.vertices station-graph)
                           town-and-7th-edges))

;;TODO













