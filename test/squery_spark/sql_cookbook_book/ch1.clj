(ns squery-spark.sql-cookbook-book.ch1
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
(q emp .show)

;;2
(q emp ((= :deptno 10)) show)

;;3
(q emp
   ((or (= :deptno 10)
        (some? :comm)
        (<= :sal 2000))
    (= :deptno 20))
   show)

;;4
(q emp
   [:ename :deptno :sal]
   show)

;;5
(q emp
   [{:salary :sal} {:commission :comm}]
   show)

;;6
(q emp
   [{:salary :sal} {:commission :comm}]
   ((< :salary 5000))
   show)

;;7
(q emp
   [{:msg (str :ename " works as a " :job)}]
   show)

;;8
(q emp
   {:status (cond (< :sal 2000) "under-paid"
                  (< :sal 4000) "ok"
                  :else "over-paid")}
   show)

;;9
(q emp
   (limit 5)
   show)

;;10
(q emp
   (sort (functions/rand))
   (limit 5)
   show)

;;11
(q emp
   ((nil? :comm))
   .show)

;;12
(q emp
   [{:comm (if-nil? :comm 0)}]
   .show)

;;12 (alt)
(q emp
   [{:comm (coalesce :comm 0)}]
   .show)

;;13
(q emp
   ((contains? [10 20] :deptno)
    (or (re-find? "I" :ename)
        (re-find? "ER" :job)))
   [:ename :job]
   show)