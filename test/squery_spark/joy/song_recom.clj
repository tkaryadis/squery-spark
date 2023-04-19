(ns squery-spark.joy.song-recom
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]]
            [squery-spark.joy.init-data :as mongo])
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

(def takis-vertices (load-collection spark :joy.takis-vertices))

(def takis-edges
  (let [edges (q (load-collection spark :joy.takis-edges)
                 (rename {:src :songid})
                 {:dst (window (offset :src 1) (ws-sort :timestamp))})]
    (q edges
       (union-with (q edges
                      (rename {:src :dst :dst :src}))))))

(def song-graph (GraphFrame. takis-vertices takis-edges))
(.cache song-graph)

(prn (-> song-graph .vertices .count))
(prn (-> song-graph .edges .count))

;;1854 adiaforos

(q song-graph
   (.find  "(a)-[ab]->(b)")
   ((= :a.id 3149) (not= :a.id :b.id))
   (group :ab.timestamp
          {:a (first-acc :a)
           :b (first-acc :b)})
   (group :b
          {:count (count-acc)}
          {:a (first-acc :a)})
   (sort :!count)
   (show 1000 false))