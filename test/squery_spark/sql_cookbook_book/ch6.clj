(ns squery-spark.sql-cookbook-book.ch6
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
  (:import (org.apache.spark.sql functions Column)
           (org.apache.spark.sql.expressions Window WindowSpec)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")   ;;CHANGE THIS!!!

(def emp (-> spark .read (.format "delta") (.load (str data-path "/emp"))))
(def dept (-> spark .read (.format "delta") (.load (str data-path "/dept"))))
(def bonus (-> spark .read (.format "delta") (.load (str data-path "/bonus"))))
(def bonus1 (-> spark .read (.format "delta") (.load (str data-path "/bonus1"))))
(def bonus2 (-> spark .read (.format "delta") (.load (str data-path "/bonus2"))))
(def t1 (seq->df spark [[1]] [[:id :long]]))

;;1
(q t1
   [{:a "Clojure+Spark"}]
   {:a (explode (split-str :a ""))}
   show)

;;2 skipped

;;3
(q t1
   [{:a "10,CLARK,MANAGER"}]
   {:ncommas (count-str (replace :a "[^,]" ""))}
   show)

;;4
(q emp
   [:ename :sal]
   {:stripped1 (replace :ename "[AEIOU]" "")
    :stripped2 (replace (string :sal) "0" "")}
   show)

;;5
(q emp
   [{:data (str :ename :sal)}]
   {:ename (replace :data "\\d" "")
    :sal (long (replace :data "\\D" ""))}
   show)

;;6
(q t1
   [{:data (explode ["CLARK"
                     "KING"
                     "MILLER"
                     "SMITH, $800.00"
                     "JONES, $2975.00"
                     "SCOTT, $3000.00"
                     "ADAMS, $1100.00"
                     "FORD, $3000.00"
                     "ALLEN30"
                     "WARD30"
                     "MARTIN30"
                     "BLAKE30"
                     "TURNER30"
                     "JAMES30"])}]
   ((not (re-find? "\\W" :data)))
   show)

;;7
(q t1
   [{:fullname (str "Stewie Griffin")}]
   {:initials (split-str :fullname " ")}
   {:initials (map1 (comp #(str % ".") (partial take-str 0 1)) :initials)}
   (show false))