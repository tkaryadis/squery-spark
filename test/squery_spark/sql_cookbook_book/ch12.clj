(ns squery-spark.sql-cookbook-book.ch12
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
  (:require [clojure.core :as c]))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))
(def cnt (load-collection spark :cookbook.cnt))

;;ch12 1
;;pivot GENERAL without knowing the possible departments
(q emp
   (group :deptno {:cnt (sum 1)})
   {:deptno (str "deptno_" :deptno)}
   (group)
   (pivot :deptno)
   (agg (first-a :cnt))
   show)

;;ch12 1 manual alt
;;pivot manual, requires to know the possible departments
(q emp
   {:deptno_10 (if- (= :deptno 10) 1 0)
    :deptno_20 (if- (= :deptno 20) 1 0)
    :deptno_30 (if- (= :deptno 30) 1 0)}
   [:deptno_10 :deptno_20 :deptno_30]
   (group {:deptno_10 (sum :deptno_10)}
          {:deptno_20 (sum :deptno_20)}
          {:deptno_30 (sum :deptno_30)})
   show)

;;2 array way, general without knowing possible job types
(q emp
   [:job :ename]
   (group)
   (pivot :job)
   (agg (conj-each :ename))
   show)

;;2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
#_(q emp
   [{:clerks (if- (= :job "CLERK") :ename nil)
     :analysts (if- (= :job "ANALYST") :ename nil)
     :managers (if- (= :job "MANAGER") :ename nil)
     :presidents (if- (= :job "PRESIDENT") :ename nil)
     :salesmen (if- (= :job "SALESMAN") :ename nil)}]
   (sort )
   show)

;;3
(q t1
   (unset :_id)
   {:deptno_10 3 :deptno_20 5 :deptno_30 6}
   show)