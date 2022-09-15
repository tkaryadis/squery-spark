(ns squery-spark.sql-cookbook-book.notes
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
(def tests (load-collection spark :cookbook.tests))
(def emp2 (load-collection spark :cookbook.emp2))
(def cnt (load-collection spark :cookbook.cnt))


;;2 ways to do self-subquery
;;6 ch11
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

;;7.6 window function, sort and sum cookbook
(q emp
   {:running-total (wfield (sum :sal) (wsort :sal :empno))}
   show)

;;8.3 working days between 2 dates cookbook
(q t1
   [{:date1 (date "2006-11-09" "yyyy-MM-dd")}
    {:date2 (date "2006-12-09" "yyyy-MM-dd")}]
   {:diff (days-diff :date2 :date1)}
   {:working-dates  (reduce (fn [v t]
                              (let [dt (add-days :date1 (int t))
                                    dt-n (day-of-week dt)]
                                (if- (and (not= dt-n 1) (not= dt-n 7))
                                     (conj v dt)
                                     v)))
                            (date-array [])
                            (range :diff))}
   {:working-days-count (count :working-dates)}
   (show false))

;;pagination ch8 cookbook
(q emp
   {:rn (wfield (row-number) (wsort :sal))}
   ((<> :rn 1 5))
   [:sal]
   show)


;;7 ch11, cookbook window function take the row in window after the offset
(q emp2
   {:next-sal (wfield (offset :salary 1) (wsort :!date))}
   show)

;;11 ch11 cookbook
(q (as emp :e1)
   [:e1.deptno :e1.ename :e1.sal :e1.hiredate]
   {:latest-sal (wfield (first-a :sal) (-> (wgroup :deptno)
                                           (wsort :!hiredate)))}
   (sort :deptno)
   show)

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

;;ch12 2 array way, general without knowing possible job types
(q emp
   [:job :ename]
   (group)
   (pivot :job)
   (agg (conj-each :ename))
   (show false))

;;ch12 2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
;;group+row number, group is like tricky way to take more than one for the accumulator
;;if diffrent row_number they belong to different gorups => i can take 1 from each group
(q emp
   [:job
    :ename
    {:rn (wfield (row-number) (-> (wgroup :job) (wsort :job)))}]
   (group :rn
          {:clerks (max (if- (= :job "CLERK") :ename nil))}
          {:analysts (max (if- (= :job "ANALYST") :ename nil))}
          {:managers (max (if- (= :job "MANAGER") :ename nil))}
          {:presidents (max (if- (= :job "PRESIDENT") :ename nil))}
          {:salesmen (max (if- (= :job "SALESMAN") :ename nil))})
   show)

;;ch12 2 alt assuming only those jobs ANALYST CLERK MANAGER PRESIDENT SALESMAN
;; without group
(q emp
   [{:clerks (if- (= :job "CLERK") :ename nil)}]
   {:rn (wfield (row-number) (wsort :clerks!))}
   (as :e1)
   (join (q emp
            [{:analysts (if- (= :job "ANALYST") :ename nil)}]
            {:rn (wfield (row-number) (wsort :analysts!))}
            (as :e2))
         (= :e1.rn :e2.rn))
   (join (q emp
            [{:managers (if- (= :job "MANAGER") :ename nil)}]
            {:rn (wfield (row-number) (wsort :managers!))}
            (as :e3))
         (= :e1.rn :e3.rn))
   (join (q emp
            [{:presidents (if- (= :job "PRESIDENT") :ename nil)}]
            {:rn (wfield (row-number) (wsort :presidents!))}
            (as :e4))
         (= :e1.rn :e4.rn))
   (join (q emp
            [{:salesmans (if- (= :job "SALESMAN") :ename nil)}]
            {:rn (wfield (row-number) (wsort :salesmans!))}
            (as :e5))
         (= :e1.rn :e5.rn))
   (unset :e1.rn :e2.rn :e3.rn :e4.rn :e5.rn)
   (drop-na "all")
   show)

