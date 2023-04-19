(ns squery-spark.sql57_examples_book.intermediate
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.sql57_examples_book.read-tables :refer [read-table]])
  (:refer-clojure)
  (:require [clojure.core :as c]))

;;Book
;;SQL Practice Problems: 57 beginning, intermediate, and advanced challenges for you to solve
;;using a “learn-by-doing” approach , by Sylvia Moestl Vasilik

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")   ;;CHANGE THIS!!!
(def df-read (partial read-table spark (str  data-path "/data-used/sql57-examples-book/")))

;;Read dfs
(def shippers (df-read "Shippers"))
(def categories (df-read "Categories"))
(def employees (df-read "Employees"))
(def orders (df-read "Orders"))
(def suppliers (df-read "Suppliers"))
(def products (df-read "Products"))
(def order-details (df-read "OrderDetails"))
(def customers (df-read "Customers"))
(def mytest (df-read "mytest"))


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
   {:max-date (window (max :OrderDate))}
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
(q (as customers :c)
   (join (as orders :o)
         (and (= :c.CustomerID :o.CustomerID) (= :EmployeeID 4))
         :left_anti)
   .show)