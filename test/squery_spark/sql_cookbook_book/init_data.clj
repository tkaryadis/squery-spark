(ns squery-spark.sql-cookbook-book.init-data
  (:refer-clojure :only [])
  (:use squery-mongo-core.operators.operators
        squery-mongo-core.operators.qoperators
        squery-mongo-core.operators.uoperators
        squery-mongo-core.operators.stages
        squery-mongo-core.operators.options
        squery-mongo.driver.cursor
        squery-mongo.driver.document
        squery-mongo.driver.settings
        squery-mongo.driver.transactions
        squery-mongo.driver.utils
        squery-mongo.arguments
        squery-mongo.commands
        squery-mongo.macros
        flatland.ordered.map)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (java.text SimpleDateFormat)
           (java.sql Date)
           (java.time Instant)
           (com.mongodb MongoClientSettings)
           (com.mongodb.client MongoClients)))

;;using squery-mongo



(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(def emp-docs [
               {:empno 7369 :ename "SMITH" :job  "CLERK" :mgr 7902
                :hiredate "17-12-2005" :sal 800 :comm nil :deptno 20 }
               {:empno 7499 :ename "ALLEN" :job  "SALESMAN" :mgr 7698
                :hiredate "20-02-2006" :sal 1600 :comm 300 :deptno 30 }
               {:empno 7521 :ename "WARD" :job  "SALESMAN" :mgr 7698
                :hiredate "22-02-2006" :sal 1250 :comm 500 :deptno 30 }
               {:empno 7566 :ename "JONES" :job  "MANAGER" :mgr 7839
                :hiredate "02-04-2006" :sal 2975 :comm nil :deptno 20 }
               {:empno 7654 :ename "MARTIN" :job  "SALESMAN" :mgr 7698
                :hiredate "28-09-2006" :sal 1250 :comm 1400 :deptno 30 }
               {:empno 7698 :ename "BLAKE" :job  "MANAGER" :mgr 7839
                :hiredate "01-05-2006" :sal 2850 :comm nil :deptno 30 }
               {:empno 7782 :ename "CLARK" :job  "MANAGER" :mgr 7839
                :hiredate "09-06-2006" :sal 2450 :comm nil :deptno 10 }
               {:empno 7788 :ename "SCOTT" :job  "ANALYST" :mgr 7566
                :hiredate "09-12-2007" :sal 3000 :comm nil :deptno 20 }
               {:empno 7839 :ename "KING" :job  "PRESIDENT" :mgr nil
                :hiredate "17-11-2006" :sal 5000 :comm nil :deptno 10 }
               {:empno 7844 :ename "TURNER" :job "SALESMAN"  :mgr 7698
                :hiredate "08-09-2006" :sal 1500 :comm 0 :deptno 30 }
               {:empno 7876 :ename "ADAMS" :job "CLERK"  :mgr 7788
                :hiredate "12-01-2008" :sal 1100 :comm nil :deptno 20 }
               {:empno 7900 :ename "JAMES" :job "CLERK"  :mgr 7698
                :hiredate "03-12-2006" :sal 950 :comm nil :deptno 30 }
               {:empno 7902 :ename "FORD" :job "ANALYST"  :mgr 7566
                :hiredate "03-12-2006" :sal 3000 :comm nil :deptno 20 }
               {:empno 7934 :ename "MILLER" :job "CLERK"  :mgr 7782
                :hiredate "23-01-2007" :sal 1300 :comm nil :deptno 10 }
             ])

(try (drop-collection :cookbook.emp) (catch Exception e ""))
(insert :cookbook.emp emp-docs)
(update- :cookbook.emp (uq {:hiredate (date-from-string :hiredate "%d-%m-%Y")}))

(def dept-docs [
               {:deptno 10 :dname "ACCOUNTING" :loc "NEW YORK"  }
                {:deptno 20 :dname "RESEARCH" :loc "DALLAS"  }
                {:deptno 30 :dname "SALES" :loc "CHICAGO"  }
                {:deptno 40 :dname "OPERATIONS" :loc "BOSTON"  }
])

(try (drop-collection :cookbook.dept) (catch Exception e ""))
(insert :cookbook.dept dept-docs)

(def bonus-docs [{:empno 7369 :received "14-03-2005" :type 1}
                 {:empno 7900 :received "14-03-2005" :type 2}
                 {:empno 7788 :received "14-03-2005" :type 3}])

(try (drop-collection :cookbook.bonus) (catch Exception e ""))
(insert :cookbook.bonus bonus-docs)
(update- :cookbook.bonus (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def bonus1-docs [{:empno 7934 :received "17-03-2005" :type 1}
                   {:empno 7934 :received "15-02-2005" :type 2}
                   {:empno 7839 :received "15-02-2005" :type 3}
                   {:empno 7782 :received "15-02-2005" :type 1}])

(try (drop-collection :cookbook.bonus1) (catch Exception e ""))
(insert :cookbook.bonus1 bonus1-docs)
(update- :cookbook.bonus1 (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def bonus2-docs [{:empno 7934 :received "17-03-2005" :type 1}
                   {:empno 7934 :received "15-02-2005" :type 2}])

(try (drop-collection :cookbook.bonus2) (catch Exception e ""))
(insert :cookbook.bonus2 bonus2-docs)
(update- :cookbook.bonus2 (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def t1-docs [{:_id 1}])
(try (drop-collection :cookbook.t1) (catch Exception e ""))
(insert :cookbook.t1 t1-docs)


(def nulls [{:_id 1} {:_id nil} {:_id 2}])
(try (drop-collection :cookbook.nulls) (catch Exception e ""))
(insert :cookbook.nulls nulls)


(def tests [
        {:test1 20 :test2 20}
        {:test1 150 :test2 25 }
        {:test1 20 :test2 20}
        {:test1 60 :test2 30}
        {:test1 70 :test2 90}
        {:test1 80 :test2 130}
        {:test1 90 :test2 70}
        {:test1 100 :test2 50}
        {:test1 110 :test2 55}
        {:test1 120 :test2 60}
        {:test1 130 :test2 80}
        {:test1 140 :test2 70}
        ])

(try (drop-collection :cookbook.tests) (catch Exception e ""))
(insert :cookbook.tests tests)

(def emp2 [{"ename" "SMITH", "salary" 800, "date" "17-12-1980"}
           {"ename" "ALLEN", "salary" 1600, "date" "20-02-1981"}
           {"ename" "WARD", "salary" 1250, "date" "22-02-1981"}
           {"ename" "JONES", "salary" 2975, "date" "02-04-1981"}
           {"ename" "BLAKE", "salary" 2850, "date" "01-05-1981"}
           {"ename" "CLARK", "salary" 2450, "date" "09-06-1981"}
           {"ename" "TURNER", "salary" 1500, "date" "08-09-1981"}
           {"ename" "MARTIN", "salary" 1250, "date" "28-09-1981"}
           {"ename" "KING", "salary" 5000, "date" "17-11-1981"}
           {"ename" "JAMES", "salary" 950, "date" "03-12-1981"}
           {"ename" "FORD", "salary" 3000, "date" "03-12-1981"}
           {"ename" "MILLER", "salary" 1300, "date" "23-01-1982"}
           {"ename" "SCOTT", "salary" 3000, "date" "09-12-1982"}
           {"ename" "ADAMS", "salary" 1100, "date" "12-01-1983"}])

(try (drop-collection :cookbook.emp2) (catch Exception e ""))
(insert :cookbook.emp2 emp2)
(update- :cookbook.emp2 (uq {:date (date-from-string :date "%d-%m-%Y")}))

(def cnt [{:deptno 10 :cnt 3}
          {:deptno 20 :cnt 5}
          {:deptno 30 :cnt 6}])

(try (drop-collection :cookbook.cnt) (catch Exception e ""))
(insert :cookbook.cnt cnt)

(def cnt1 [{:deptno_10 3 :deptno_20 5 :deptno_30 6}])

(try (drop-collection :cookbook.cnt1) (catch Exception e ""))
(insert :cookbook.cnt1 cnt1)


(def range1 [{:proj-id 1 :proj-start "01-01-2020" :proj-end "02-01-2020" }
             {:proj-id 2 :proj-start "02-01-2020" :proj-end "03-01-2020" }
             {:proj-id 3 :proj-start "03-01-2020" :proj-end "04-01-2020" }
             {:proj-id 4 :proj-start "04-01-2020" :proj-end "05-01-2020" }
             {:proj-id 5 :proj-start "06-01-2020" :proj-end "07-01-2020" }
             {:proj-id 6 :proj-start "16-01-2020" :proj-end "17-01-2020" }
             {:proj-id 7 :proj-start "17-01-2020" :proj-end "18-01-2020" }
             {:proj-id 8 :proj-start "18-01-2020" :proj-end  "19-01-2020"}
             {:proj-id 9 :proj-start "19-01-2020" :proj-end "20-01-2020" }
             {:proj-id 10 :proj-start "21-01-2020" :proj-end "22-01-2020" }
             {:proj-id 11 :proj-start "26-01-2020" :proj-end "27-01-2020" }
             {:proj-id 12 :proj-start "27-01-2020" :proj-end "28-01-2020" }
             {:proj-id 13 :proj-start "28-01-2020" :proj-end "29-01-2020" }
             {:proj-id 14 :proj-start "29-01-2020" :proj-end "30-01-2020" }
             ])

(try (drop-collection :cookbook.range1) (catch Exception e ""))
(insert :cookbook.range1 range1)
(update- :cookbook.range1 (uq {:proj-start (date-from-string :proj-start "%d-%m-%Y")
                               :proj-end (date-from-string :proj-end "%d-%m-%Y")}))

