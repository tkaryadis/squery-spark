(ns squery-spark.sql-cookbook-book.init-data
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (io.delta.tables DeltaTable)
           (java.text SimpleDateFormat)
           (java.sql Date)
           (java.time Instant)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/sql-cookbook-book/")   ;;CHANGE THIS!!!

(def emp-df (seq->df spark [
                            [7369, "SMITH", "CLERK",7902,"17-12-2005",800,nil,20]
                            [7499, "ALLEN", "SALESMAN",7698,"20-02-2006", 1600,300,30]
                            [7521 "WARD" "SALESMAN" 7698 "22-02-2006"  1250 500 30]
                            [7566, "JONES", "MANAGER", 7839 "02-04-2006" , 2975,nil, 20]
                            [7654, "MARTIN", "SALESMAN", 7698 "28-09-2006" , 1250, 1400, 30]
                            [7698, "BLAKE", "MANAGER", 7839 , "01-05-2006" , 2850,nil, 30]
                            [7782, "CLARK", "MANAGER" ,7839, "09-06-2006"  ,2450,nil,10]
                            [7788, "SCOTT" ,"ANALYST", 7566, "09-12-2007" , 3000,nil, 20]
                            [7839,"KING" ,"PRESIDENT" , nil "17-11-2006" , ,5000, nil,10]
                            [7844, "TURNER", "SALESMAN" ,7698 ,"08-09-2006" , 1500 ,0 ,30]
                            [7876, "ADAMS", "CLERK", 7788, "12-01-2008" , 1100,nil ,20]
                            [7900, "JAMES" "CLERK", 7698, "03-12-2006"  ,950,nil ,30]
                            [7902,"FORD" "ANALYST" ,7566 ,"03-12-2006"  ,3000,nil ,20]
                            [7934 "MILLER" "CLERK" 7782 "23-01-2007"  1300 nil 10]
                            ]
                     [[:empno :long]
                      :ename
                      :job
                      [:mgr :long]
                      :hiredate
                      [:sal :long]
                      [:comm :long]
                      [:deptno :long]]))

(-> (DeltaTable/createOrReplace spark)
    (table-columns [[:empno :long]
                    :ename
                    :job
                    [:mgr :long]
                    [:hiredate :date]
                    [:sal :long]
                    [:comm :long]
                    [:deptno :long]])
    (.property "description" "emp")
    (.tableName "default.emp")
    (.location (str data-path "/emp"))
    (.execute))


(-> (q emp-df {:HIREDATE (date :HIREDATE "dd-mm-yyyy")})
    .write
    (.format "delta")
    (.mode "overwrite")
    (.saveAsTable "default.emp"))

(def dept-df (seq->df spark [[10 "ACCOUNTING"  "NEW YORK"]
                             [20 "RESEARCH"  "DALLAS" ]
                             [30 "SALES" "CHICAGO" ]
                             [40 "OPERATIONS" "BOSTON"]]
                      [[:deptno :long]
                       :dname
                       :loc]))

(-> (DeltaTable/createOrReplace spark)
    (table-columns [[:deptno :long]
                    :dname
                    :loc])
    (.property "description" "dept")
    (.tableName "default.dept")
    (.location (str data-path "/dept"))
    (.execute))

(-> dept-df
    .write
    (.format "delta")
    (.mode "overwrite")
    (.saveAsTable "default.dept"))