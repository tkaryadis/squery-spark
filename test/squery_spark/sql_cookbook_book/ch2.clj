(ns squery-spark.sql-cookbook-book.ch2
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))

;;1
(q emp
   ((= :deptno 10))
   (sort :sal)
   [:ename :job :sal]
   show)

;;2
(q emp
   (sort :deptno :!sal)
   [:empno :deptno :sal :ename :job]
   show)

;;3
(q emp
   (sort (take-str -2 :job))
   [:ename :job]
   show)

;;4
(q emp
   {:data (str :ename " " :deptno)}
   {:numbers (replace :data "[A-Za-z]" "")
    :chars (replace :data "\\d" "")}
   (sort :numbers)
   ;;(sort :chars)
   show)

;;5
(q emp
   (sort (if- (nil? :comm) 1 0) :comm)
   show)

;;6
(q emp
   (sort (if- (= :job "SALESMAN") :comm :sal))
   show)






