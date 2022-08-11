(ns squery-spark.sql-cookbook-book.ch3
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")   ;;CHANGE THIS!!!

(def emp (-> spark .read (.format "delta") (.load (str data-path "/emp"))))
(def dept (-> spark .read (.format "delta") (.load (str data-path "/dept"))))
(def t1 (seq->df spark [[1]] [[:id :long]]))

;;1
(q emp
   ((= :deptno 10))
   [:ename :deptno]
   (union-with (q t1 ["----------" nil]))
   (union-with (q dept [:dname :deptno]))
   [{:ename_dname :ename} :deptno]
   show)

;;2
(q emp
   ((= :deptno 10))
   (join-eq dept :deptno)
   [:ename :loc]
   show)

;;3
(q (as emp :e1)
   ((= :job "CLERK"))
   [:ename :job :sal]
   (join (as emp :e2)  (and (= :e1.job :e2.job)
                            (= :e1.ename :e2.ename)
                            (= :e1.sal :e2.sal)))
   [:e2.empno :e2.ename :e2.job :e2.sal :e2.deptno]
   show)

;;4
(q dept
   [:deptno]
   (difference-with (q emp [:deptno]))
   show)