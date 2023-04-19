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


;;ch12 1
;;pivot GENERAL without knowing the possible departments
(q emp
   (group :deptno {:cnt (sum 1)})
   {:deptno (str "deptno_" :deptno)}
   (group)
   (pivot :deptno)
   (agg (first-acc :cnt))
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
   (show false))

;;2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
;;ch12 2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
;;group+row number, group is like tricky way to take more than one for the accumulator
;;if diffrent row_number they belong to different gorups => i can take 1 from each group
(q emp
   [:job
    :ename
    {:rn (window (row-number) (-> (ws-group :job) (ws-sort :job)))}]
    (group :rn
           {:clerks (max (if- (= :job "CLERK") :ename nil))}
           {:analysts (max (if- (= :job "ANALYST") :ename nil))}
           {:managers (max (if- (= :job "MANAGER") :ename nil))}
           {:presidents (max (if- (= :job "PRESIDENT") :ename nil))}
           {:salesmen (max (if- (= :job "SALESMAN") :ename nil))})
    show)

;;2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
;; row-number without group
(q emp
   [{:clerks (if- (= :job "CLERK") :ename nil)}]
   {:rn (window (row-number) (ws-sort :clerks!))}
   (as :e1)
   (join (q emp
            [{:analysts (if- (= :job "ANALYST") :ename nil)}]
            {:rn (window (row-number) (ws-sort :analysts!))}
            (as :e2))
         (= :e1.rn :e2.rn))
   (join (q emp
            [{:managers (if- (= :job "MANAGER") :ename nil)}]
            {:rn (window (row-number) (ws-sort :managers!))}
            (as :e3))
         (= :e1.rn :e3.rn))
   (join (q emp
            [{:presidents (if- (= :job "PRESIDENT") :ename nil)}]
            {:rn (window (row-number) (ws-sort :presidents!))}
            (as :e4))
         (= :e1.rn :e4.rn))
   (join (q emp
            [{:salesmans (if- (= :job "SALESMAN") :ename nil)}]
            {:rn (window (row-number) (ws-sort :salesmans!))}
            (as :e5))
         (= :e1.rn :e5.rn))
   (unset :e1.rn :e2.rn :e3.rn :e4.rn :e5.rn)
   (drop-na "all")
   show)

;;3 ?
(q cnt1
   [{:deptno 10} {:counts :deptno_10}]
   (union-with (q cnt1 [{:deptno 20} {:counts :deptno_20}]))
   (union-with (q cnt1 [{:deptno 30} {:counts :deptno_30}]))
   show)

;;4 array way, safe even if lots of data
(q emp
   ((= :deptno 10))
   [{:emps (explode [:ename :job :sal ""])}]
   show)

;;5
(q emp
   {:rn (window (row-number) (-> (ws-sort :deptno) (ws-group :deptno)))}
   [{:deptno (if- (= :rn 1) :deptno "")} :ename]
   show)

;;6
(q emp
   (group :deptno
          {:sal (sum :sal)})
   (group {:d20_10_diff (sum (cond
                               (= :deptno 10) (- 0 :sal)
                               (= :deptno 20) :sal
                               :else 0))}
          {:d20_30_diff (sum (cond
                               (= :deptno 30) (- 0 :sal)
                               (= :deptno 20) :sal
                               :else 0))})
   show)

;;7 buckets of 5 size
(q emp
   {:rn (window (row-number) (ws-sort :empno))}
   (group [:GRP (+ (long (div :rn 5)) 1)]
          {:emps (conj-each [:empno :ename])})
   {:emps (explode :emps)}
   {:empno (first :emps)
    :ename (second :emps)}
   (unset :emps)
   show)


;;8 ,seperate in 4 buckets
(q emp
   {:rn (window (row-number) (ws-sort :empno))}
   (group [:GRP (+ (long (mod :rn 4)) 1)] :empno
          {:ename (first-acc :ename)})
   show)

;;8 alt with ntile window function
(q emp
   [{:GRP (window (bucket-size 4) (ws-sort :empno))}
    :empno
    :ename]
   show)


;;9 (spark repeat needs known int not a column so cant be used)
(q emp
   (group :deptno
          {:cnt (count-acc)})
   {:cnt (reduce (fn [v t] (str v "*")) "" (range :cnt))}
   (sort :deptno)
   show)

;;10
(q emp
   {:rn (window (row-number) (-> (ws-sort :deptno) (ws-group :deptno)))}
   (group :rn
          {:deptno_10 (max (if- (= :deptno 10) "*" ""))}
          {:deptno_20 (max (if- (= :deptno 20) "*" ""))}
          {:deptno_30 (max (if- (= :deptno 30) "*" ""))})
   (sort :!rn)
   (unset :rn)
   show)

;;11
(q emp
   {:max-dept (window (max :sal) (ws-group :deptno))
    :min-dept (window (min :sal) (ws-group :deptno))
    :max-job (window (max :sal) (ws-group :job))
    :min-job (window (min :sal) (ws-group :job))}
   [:deptno :ename :job :sal
    {:dept-status (cond (= :max-dept :sal) "max"
                        (= :min-dept :sal) "min"
                        :else "")}
    {:job-status (cond  (= :max-job :sal) "max"
                        (= :min-job :sal) "min"
                        :else "")}]
   ((or (not= :job-status "") (not= :dept-status "")))
   (sort :deptno :job)
   show)

;;12 union way
(q emp
   (group :job
          {:sal (sum :sal)})
   (union-with (q emp
                  (group {:sal (sum :sal)})
                  [{:job "total"} :sal]))
   show)

;;12 rollup way
(q emp
   (rollup :job)
   (agg {:sal (sum :sal)})
   {:job (if- (some? :job) :job "total")}
   show)

;;13 cube multiple groupings
;;union way would be big here because cube
(q emp
   (cube :deptno :job)
   (agg {:sal (sum :sal)})
   {:category (cond (and (nil? :deptno) (nil? :job)) "Total"
                    (nil? :deptno) "total by job"
                    (nil? :job) "total by deptno"
                    :else "total by dept and job")}
   (show false))

;;14
(q emp
   (cube :deptno :job)
   (agg {:sal (sum :sal)})
   {:dept-job (cond (and (nil? :deptno) (nil? :job)) [1 1]
                    (nil? :deptno) [1 0]
                    (nil? :job) [0 1]
                    :else [0 0])}
   (show false))

;;15 skipped simple cond with many cases
;;16 skipped simple cond with many cases
;;17 skipped bucket

;;18
(q emp
   {:deptno-cnt (window (count-acc) (ws-group :deptno))
    :job-cnt (window (count-acc) (ws-group :job))
    :total (window (count-acc))}
   show)

;;19,20  TODO