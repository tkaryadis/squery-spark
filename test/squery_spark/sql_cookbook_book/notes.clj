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
   {:lower-sal (window (min :sal))}
   {:max-sal (window (max :sal))}
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
   {:running-total (window (sum :sal) (ws-sort :sal :empno))}
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
   {:rn (window (row-number) (ws-sort :sal))}
   ((<> :rn 1 5))
   [:sal]
   show)


;;7 ch11, cookbook window function take the row in window after the offset
(q emp2
   {:next-sal (window (offset :salary 1) (ws-sort :!date))}
   show)

;;11 ch11 cookbook
(q (as emp :e1)
   [:e1.deptno :e1.ename :e1.sal :e1.hiredate]
   {:latest-sal (window (first-acc :sal) (-> (ws-group :deptno)
                                           (ws-sort :!hiredate)))}
   (sort :deptno)
   show)

;;ch12 pivots and the opposite

