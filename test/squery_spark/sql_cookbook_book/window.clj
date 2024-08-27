(ns squery-spark.sql-cookbook-book.window
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]]
            [squery-spark.utils.utils :refer [ISODate]])
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
(def emp2 (load-collection spark :cookbook.emp2))

;;NULLs values are ignored from aggregation functions
;; but count-acc it counts the lines, even if null
;; to avoid that if i want i use (count-acc :field) , and will not count the null values
;; conj-each they are not added => nil name is empty array

;;aggregate functions behave the same on nulls, for windows also

(q t1
   [{:name (explode ["Oranges"
                     "Oranges"
                     "Oranges"
                     "Apple"
                     "Peach"
                     nil
                     nil
                     nil
                     nil
                     nil])}]
   (group :name
          {:names (count-acc)})
   show)

(q t1
   [{:name (explode ["Oranges"
                     "Oranges"
                     "Oranges"
                     "Apple"
                     "Peach"
                     nil
                     nil
                     nil
                     nil
                     nil])}]
   (group :name
          {:names (count-acc :name)})
   show)


;;Window functions(also called olap functions,analytic functions)
;;group no fields => 1 aggregation per table
;;group with fields => 1 aggregation per group
;;window functions => 1 aggregation per row

;; 1 group per row, like do the count for each row
;; window without group/sort etc
(q emp
   (sort :deptno)
   [:ename :deptno {:cnt (window (count-acc))}]
   show)

;; window + group
;; each row gets a value, but belongs to the group that the row belongs in
;; if i am in row where deptNo=10, it will get the count-acc where deptNo=10
(q emp
   (sort :deptno)
   [:ename :deptno {:cnt (window (count-acc) (ws-group :deptno))}]
   show)

;; window + sort
;; in group row value defined in witch group the aggregation will run (the one where row_value=group_value)
;; in sort row value defines the limit of the window, and it means so-far, window = start to cur-row-value
;; (its like sum until the cur-row value)
;; the default = range between unbounded preceding and current row (i can change this by specifing the range)

(q emp
   ((= :deptno 10))
   show)

(q emp
   ((= :deptno 10))
   [:deptno :ename :hiredate :sal
    {:total2 (window (sum :sal))}
    {:running-total (window (sum :sal) (ws-sort :hiredate))}]
   show)

;; sort + rowsBetween, sort+rangeBetween
;;Window.unboundedPreceding  (i can see it as --oo)
; Window.currentRow          (i can see it as 0)
; Window.unboundedFollowing  (i can see it as +oo)

;; framing clause = is the window that range defines

;;  (Window/unboundedPreceding) ---> (Window/currentRow)
;; rowsBetween =>  all rows before the current_row + the current_row
;; rangeBetween =>  all rows with value(the sort value) <= current_row

;; i can see clearly the difference when i have duplicates on short key,
;; then rows doesnt include them, but range does include them

;;i use longs when i need other numbers, for example -1 in rowsBetween
;;  -1,CurrentRow means the window for those 2 rows

;;default behaviour so ws-range is not really needed
;;sum the salares of those that are hired before me (including me)
(q emp
   ((= :deptno 10))
   [:deptno :ename :hiredate :sal
    {:total2 (window (sum :sal))}
    {:running-total (window (sum :sal) (-> (ws-sort :hiredate)
                                           (ws-range (Window/unboundedPreceding)
                                                     (Window/currentRow))))}]
   show)

;;sum the salares of those that are hired after me (including me)
(q emp
   ((= :deptno 10))
   [:deptno :ename :hiredate :sal
    {:total2 (window (sum :sal))}
    {:running-total (window (sum :sal) (-> (ws-sort :hiredate)
                                           (ws-range (Window/currentRow)
                                                     (Window/unboundedFollowing))))}]
   show)

;;like the above but with the default range and opposite sorting
(q emp
   ((= :deptno 10))
   [:deptno :ename :hiredate :sal
    {:total2 (window (sum :sal))}
    {:running-total (window (sum :sal) (ws-sort :!hiredate))}]
   show)

(q emp
   ((= :deptno 10))
   [:deptno :ename :sal
    ;; until currentRow value
    {:total1 (window (sum :sal) (-> (ws-sort :hiredate)
                                    (ws-range (Window/unboundedPreceding)
                                              (Window/currentRow))))}
    ;; previous row + currentRow
    {:total2 (window (sum :sal) (-> (ws-sort :hiredate)
                                    (ws-rows -1
                                             (Window/currentRow))))}
    ;;currentRow and all after values
    {:total3 (window (sum :sal) (-> (ws-sort :hiredate)
                                    (ws-range (Window/currentRow)
                                              (Window/unboundedFollowing))))}
    ;;current + 1 following
    {:total4 (window (sum :sal) (-> (ws-sort :hiredate)
                                    (ws-rows (Window/currentRow)
                                             1)))}]
   show)

;;“What is the number of employees in each department?
;; How many different types of employees are in each department (e.g.,
;how many clerks are in department 10)?
; How many total employees are in table EMP?”

(q emp show)

(q emp
   {:emp-dept (window (count-acc) (ws-group :deptno))
    :empt-dept (window (count-acc) (ws-group :deptno :job))
    :emp-total (window (count-acc))}
   show)


;;Perfomance

;;i could avoid them with subqueries+add-fields(not spark suppoterd)
;;or selfjoins with subqueries(spark supported)
;;but window functions are faster and simpler in code

;;self-join example to do what window function does
(q (as emp :e1)
   (join (q (as emp :e2)
            [{:cnt (count-acc)}]))
   show)

;;--------------------other window accumulators-----------------------------

;;7 ch11, cookbook window function take the row in window after the offset
(q emp2
   {:next-sal (window (offset :salary 1) (ws-sort :!date))}
   show)
