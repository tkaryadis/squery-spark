(ns squery-spark.sql-cookbook-book.window-appA
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
;;  act on a group but dont return 1 value per group like group-by
;;  they return multiple values for each group

;;all aggregate functions can be also used as window functions (ansi sql)

;; group was
;;  1 acc on table (if no group defined)
;;  1 acc on each group

;; window functions are
;;   1 acc on each row, in which row we are it matters (i use the values from the row inside the window definition)
;; i use them when i want to add information to a row, that requires aggregation function
;; (same row info is not enough, i need to aggregate)

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
;;Window.unboundedPreceding
; Window.currentRow
; Window.unboundedFollowing

;; framing clause = is the window that range defines

;; rowsBetween =>  lower= 2 upper=5  this means   [row-2rows,row+5rows]  => position of row + 2 position offsets set range
;; rangeBetween =>  lower = 100  upper = 500 this means that [row-100,row+500] => value of row + 2 value offsets set the range

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

;;A Framing Finale