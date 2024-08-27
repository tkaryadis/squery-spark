(ns squery-spark.data-algorithms-book.ch6
  (:refer-clojure :only [])
  (:use [squery-spark.datasets.queries]
        [squery-spark.datasets.stages]
        [squery-spark.datasets.operators]
        [squery-spark.datasets.rows]
        [squery-spark.datasets.utils]
        [squery-spark.rdds.rdd])
  (:refer-clojure)
  (:require [squery-spark.state.connection :refer [get-spark-session get-spark-context get-java-spark-context]]
            [squery-spark.rdds.rdd :as r :refer [rlet r r-> r->> cfn pfn]]
            [squery-spark.datasets.stages]
            [clojure.pprint :refer [pprint]]
            [squery-spark.datasets.queries :refer [q]]
            [squery-spark.datasets.utils :refer [seq->rdd seq->pair-rdd seq->df show]]
            [squery-spark.rdds.utils :refer [read-csv-no-header]]
            [clojure.string :as s]
            [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset Dataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType)
           (java.util Arrays)
           (org.graphframes GraphFrame)
           (org.graphframes.lib PageRank)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")

;;v must have first the id
;;e default names  src,dst,relationship

;;src, dst, and

(def v (seq->df spark
                [["a", "Alice", 34]
                 [ "b", "Bob", 36]
                 ["c", "Charlie", 30]]
                [:id :name [:age DataTypes/LongType]]))

(def e (seq->df spark
                [ ["a", "b", "friend"]
                  ["b", "c", "follow"]
                  ["c", "b", "follow"]]
                [:src :dst :relationship]))

(def graph ^GraphFrame (GraphFrame. v e))

(show (.vertices graph))
(show (.edges graph))

(show (.inDegrees graph))

(prn (q (.edges graph)
        ((= :relationship "follow"))
        (count-s)))

;;each node will take a value
(def graph-ranked (-> ^PageRank (.pageRank ^GraphFrame graph)
                      (.resetProbability 0.01)
                      (.maxIter 20)
                      .run))


(q (.vertices graph-ranked)
   [:id :pagerank]
   show)

;;Algorithms examples

;;finding triangles
;; 2 edges connected = triad
;; 3 edges connected = triangle

(def v1 (seq->df spark
                 [["a" "Alice"34]
                  ["b" "Bob" 36]
                  ["c" "Charlie"30]
                  ["d" "David"29]
                  ["e" "Esther"32]
                  ["f" "Fanny"36]
                  ["g" "Gabby"60]]
                 [:id :name [:age DataTypes/LongType]]))

(def e1 (seq->df spark
                 [["a" "b" "friend"]
                  ["b" "c" "follow"]
                  ["c" "b" "follow"]
                  ["f" "c" "follow"]
                  ["e" "f" "follow"]
                  ["e" "d" "friend"]
                  ["d" "a" "friend"]
                  ["a" "e" "friend"]]
                 [:src :dst :relationship]))

(def g1 (GraphFrame. v1 e1))

(show (.vertices g1))
(show (.edges g1))

;;count the number of triangles
;angles passing through each vertex in this graph
(def g1-triangles (.run (.triangleCount g1)))

;;it shows 3 triangles but its actually 1 triangle counted 3 times
;;once for each vertice
(show g1-triangles)

;;Motif Finding

;;dataframe
;;with columns a,b,e1,e2 with values all the found results
;;each values is a StructType with the propertis of the node/edge
;;() for vertices, [] for edges
;;a,b,e1,e2 are all random variables names
;;a,b would find vertices,e1,e2 will find edges
(show (.find g1 "(a)-[e1]->(b); (b)-[e2]->(a)"))

;;finding triangles with motives
;;triagle is a e1> b e2> c e3> a  (3 nodes circle)
;;to avoid duplicates i will

(q (.find g1 "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
   show)