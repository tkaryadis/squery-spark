(ns squery-spark.sql-cookbook-book.ch10
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql.expressions Window)
           (org.apache.spark.sql functions Column RelationalGroupedDataset)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))
(def cnt (load-collection spark :cookbook.cnt))
(def cnt1 (load-collection spark :cookbook.cnt1))
(def range1 (q (load-collection spark :cookbook.range1) [:proj-id :proj-start :proj-end]))


;;1
(q range1
   {:next-row-start (window (offset :proj-start 1) (ws-sort :proj-id))}
   ((= :proj-end :next-row-start))
   show)

;;2
(q emp
   {:next-sal (window (offset :sal 1) (-> (ws-group :deptno)
                                          (ws-sort :hiredate)))}
   [:deptno :ename :sal :hiredate {:diff (- :sal :next-sal)}]
   show)

;;3, all group in a row, start-group end group, 1 row per group TODO
#_(q range1
   {:next-row-start (window (offset :proj-start 1) (ws-sort :proj-id))}
   {:same (= :proj-end :next-row-start)}
   )

;;4
(q emp
   [{:yr (year :hiredate)} {:cnt 1}]
   (union-with (q t1
                  [{:yr (explode (range 2005 2014))}]
                  [:yr {:cnt 0}]))
   (group :yr
          {:cnt (sum :cnt)})
   (sort :yr)
   show)

;;5
(q t1
   [{:id (explode (range 1 10))}]
   show)
















