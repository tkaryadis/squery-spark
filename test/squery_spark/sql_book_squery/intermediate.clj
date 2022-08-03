(ns squery-spark.sql-book-squery.intermediate
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


;;Examples from book
;;SQL Practice Problems: 57 beginning, intermediate, and advanced challenges for you to solve using a “learn-by-doing” approach

;;20
(q products
   (join-eq categories :CategoryID)
   (group :CategoryName
          {:sum (sum 1)})
   (sort :!sum)
   .show)

;;21
(q customers
   (group :Country :City
          {:TotalCustomer (sum 1)})
   (sort :!TotalCustomer)
   .show)

;;22
(q products
   ((< :UnitsInStock :ReorderLevel))
   [:ProductID :ProductName :UnitsInStock :ReorderLevel]
   .show)

;;23
(q products
   ((<= (+ :UnitsInStock :UnitsOnOrder) :ReorderLevel) (= :Discontinued 0))
   [:ProductID :ProductName :UnitsInStock :ReorderLevel :Discontinued]
   .show)

;;24
(q customers
   (sort :Region! :CustomerID)
   [:CustomerID :CompanyName :Region]
   (.show 100))

;;24 alternative
(q customers
   (sort (if- (nil? :Region) 1 0) :CustomerID)
   [:CustomerID :CompanyName :Region]
   (.show 100))

;;25
(q orders
   (group :ShipCountry
          {:avg (avg :Freight)})
   (sort :!avg)
   (limit 3)
   (.show 100))

;;26
(q orders
   ((= (year :OrderDate) 2015))
   (group :ShipCountry
          {:avg (avg :Freight)})
   (sort :!avg)
   (limit 3)
   (.show 100))


;;28
(q orders
   {:max-date (wfield (max :OrderDate))}
   ((<= (days-diff :max-date :OrderDate) 365))
   (group :ShipCountry
          {:avg (avg :Freight)})
   (sort :!avg)
   (limit 3)
   (.show 100))


;;29
(q employees
   (join-eq orders :EmployeeID)
   (join-eq order-details :OrderID)
   (join-eq products :ProductID)
   (sort :OrderID :ProductID)
   [:EmployeeID :LastName :OrderID :ProductName :Quantity]
   .show)

;;30
(q customers
   (join-eq orders :CustomerID :CustomerID :left_anti)
   .show)

;;31
;;customers that didn't use EmployeeID=4 to take their order
(q customers
   (join orders
         (and (= [customers :CustomerID] [orders :CustomerID]) (= :EmployeeID 4))
         :left_anti)
   .show)


