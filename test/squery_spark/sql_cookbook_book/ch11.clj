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
(def emp2 (load-collection spark :cookbook.emp2))

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

;;5
(q emp
   {:rn (wfield (dense-rank) (wsort :!sal))}
   ((< :rn 6))
   show)

;;6
(q emp
   {:lower-sal (wfield (min :sal))}
   {:max-sal (wfield (max :sal))}
   ((or (= :sal :lower-sal) (= :sal :max-sal)))
   show)

;;6 alt
(q (as emp :e1)
   (join (q (as emp :e2)
            (group {:min-sal (min :sal)} {:max-sal (max :sal)}))
         (or (= :e1.sal :max-sal) (= :e1.sal :min-sal)))
   show)

;;7
(q emp2
   {:next-sal (wfield (offset :salary 1) (wsort :!date))}
   show)

;;8
(q emp
   [:ename
    :sal
    {:rewind (coalesce (wfield (offset :sal -1) (wsort :sal))
                       (wfield (min :sal)))}
    {:forward (coalesce (wfield (offset :sal 1) (wsort :sal))
                        (wfield (max :sal)))}]
   show)


;;9
(q emp
   [:sal {:rnk (wfield (dense-rank) (wsort :sal))}]
   show)

;;10
(q emp
   (select-distinct :job)
   show)

;;11
(q (as emp :e1)
   [:e1.deptno :e1.ename :e1.sal :e1.hiredate]
   {:latest-sal (wfield (first-a :sal) (-> (wgroup :deptno)
                                           (wsort :!hiredate)))}
   (sort :deptno)
   show)

;;12 skipped
















