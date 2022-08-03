(ns squery-spark.sql-book-squery.advanced
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.sql-book-squery.read-tables :refer [read-table]])
  (:refer-clojure)
  (:require [clojure.core :as c]))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")
(def df (partial read-table spark (str  data-path "/data-test/sql-examples/")))

;;Read dfs
(def shippers (df "Shippers"))
(def categories (df "Categories"))
(def employees (df "Employees"))
(def orders (df "Orders"))
(def suppliers (df "Suppliers"))
(def products (df "Products"))
(def order-details (df "OrderDetails"))
(def customers (df "Customers"))
(def mytest (df "mytest"))


;;32
(q customers
   (join-eq (q orders
               ((= (year :OrderDate) 2016)))
            :CustomerID)
   (join-eq (q order-details
               (group :OrderID
                      {:totalAmount (sum (* :UnitPrice :Quantity))})
               ((> :totalAmount 10000)))
            :OrderID)
   [:CustomerID :CompanyName :totalAmount]
   (sort :!totalAmount)
   .show)

;;32 alt (case where each order can contain multiple companies)
(q customers
   (join-eq (q orders
               ((= (year :OrderDate) 2016)))
            :CustomerID)
   (join-eq order-details :OrderID)
   (group :CustomerID :OrderID :CompanyName
          {:totalAmount (sum (* :UnitPrice :Quantity))})
   ((> :totalAmount 10000))
   [:CustomerID :CompanyName :totalAmount]
   (sort :!totalAmount)
   .show)


;;33
(q customers
   (join-eq (q orders
               ((= (year :OrderDate) 2016)))
            :CustomerID)
   (join-eq order-details :OrderID)
   (group :CustomerID :CompanyName
          {:totalAmount (sum (* :UnitPrice :Quantity))})
   ((> :totalAmount 15000))
   [:CustomerID :CompanyName :totalAmount]
   (sort :!totalAmount)
   .show)

;;34
(q customers
   (join-eq (q orders
               ((= (year :OrderDate) 2016)))
            :CustomerID)
   (join-eq order-details :OrderID)
   (group :CustomerID :CompanyName
          {:totalAmount (sum (* :UnitPrice :Quantity))}
          {:totalAmountDiscount (sum (* (* :UnitPrice (- 1 :Discount)) :Quantity))})
   ((> :totalAmountDiscount 10000))
   [:CustomerID :CompanyName :totalAmount :totalAmountDiscount]
   (sort :!totalAmountDiscount)
   .show)

;;35
(q orders
   ((= :OrderDate (last-day-of-month :OrderDate)))
   (sort :EmployeeID :OrderID :OrderDate)
   [:EmployeeID :OrderID :OrderDate]
   .show)

;;36
(q orders
   (join-eq (q order-details
               (group :OrderID
                      {:items (sum 1)}))
            :OrderID)
   (sort :!items)
   (limit 10)
   [:OrderID :items]
   .show)

;;37
(q orders
   (.sample 0.02)
   .show)
