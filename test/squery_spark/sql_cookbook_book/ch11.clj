(ns squery-spark.sql-cookbook-book.ch11
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
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset Encoders)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType StructType)
           (accumulators ProductAcc)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))
(def tests (load-collection spark :cookbook.tests))

;;1
(q emp
   {:rn (wfield (row-number) (wsort :sal))}
   ((<> :rn 1 5))
   [:sal]
   show)

;;2
(q emp
   {:rn (wfield (row-number) (wsort :ename))}
   ((odd? :rn))
   [:ename]
   show)

;;3
(q (as emp :e)
   ((or (= :deptno 10) (= :deptno 20)))
   (join (as dept :d)
         (= :e.deptno :d.deptno)
         :right_outer)
   (sort :d.deptno)
   [:e.ename :d.deptno :d.dname :d.loc]
   show)

;;4
(q tests
   [{:array (sort-array [:test1 :test2])}]
   (group :array {:count (count-a)})
   ((= :count 2))
   [{:test1 (get :array 0)} {:test2 (get :array 1)}]
   show)

;;4 alt self-join
(q (as tests :t1)
   (join (as tests :t2)
         (and (= :t1.test1 :t2.test2)
              (= :t1.test2 :t2.test1)
              (<= :t1.test1 :t2.test1)))
   (select-distinct :t1.test1 :t1.test2)
   show)












