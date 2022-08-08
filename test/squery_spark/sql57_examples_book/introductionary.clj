(ns squery-spark.sql57_examples_book.introductionary
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.sql57_examples_book.read-tables :refer [read-table]])
  (:refer-clojure))

;;Book
;;SQL Practice Problems: 57 beginning, intermediate, and advanced challenges for you to solve
;;using a “learn-by-doing” approach , by Sylvia Moestl Vasilik

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")    ;;CHANGE THIS!!!
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

;;1
(.printSchema shippers)
(q shippers
   .show)

;;2
(q categories
   [:CategoryName :Description]
   .show)

;;3
(q employees
   ((= :Title "Sales Representative"))
   [:FirstName :LastName :HireDate]
   .show)

;;4
(q employees
   ((= :Title "Sales Representative")
    (= :Country "USA"))
   [:FirstName :LastName :HireDate]
   .show)

;;5
(q orders
   ((= :EmployeeID 5))
   [:OrderID :OrderDate]
   .show)

;;6
(q suppliers
   ((not= :ContactTitle "Marketing Manager"))
   [:SupplierID :ContactName :ContactTitle]
   .show)

;;7
(q products
   ((re-find? "(?i)queso" :ProductName))
   .show)

;;8
(q orders
   ((or (= :ShipCountry "France")
        (= :ShipCountry "Belgium")))
   .show)

;;8 (alt)
(q orders
   ((contains? ["France" "Belgium"] :ShipCountry))
   .show)

;;9
(q orders
   ((contains? ["Brazil" "Mexico" "Argentina" "Venezuela"]
               :ShipCountry))
   .show)

;;10
(q employees
   [:FirstName :LastName :Title :BirthDate]
   (sort :BirthDate)
   .show)

;;10 desc
(q employees
   [:FirstName :LastName :Title :BirthDate]
   (sort :!BirthDate)
   .show)

;;11
(q employees
   [:FirstName :LastName :Title (date-to-string :BirthDate "yyyy-mm-dd")]
   (sort :!BirthDate)
   .show)

;;12
(q employees
   [:FirstName :LastName]
   {:FullName (str :FirstName " " :LastName)}
   .show)

;;13
(q order-details
   [:OrderID :ProductID :UnitPrice :Quantity]
   {:TotalPrice (* :UnitPrice :Quantity)}
   (sort :OrderID :ProductID)
   .show)

;;14
(prn (q customers
        (count-s)))

;;14 alter
(q customers
   (group nil
          {:TotalCustomers (count-a :CustomerID)})
   .show)

;;15
(q orders
   (sort :OrderDate)
   [:OrderDate]
   (limit 1)
   .show)

;;16
(q customers
   (group :Country)
   (sort :Country)
   .show)

;;17
(q customers
   (group :ContactTitle
          {:sum (count-a)})
   (sort :!sum)
   .show)

;;17-alt
(q customers
   (group :ContactTitle
          {:sum (sum 1)})
   (sort :!sum)
   .show)


;;18
(q products
   (join suppliers :SupplierID)
   (sort :ProductID)
   [:ProductID :ProductName {:Supplier :CompanyName}]
   (.show))

;;19
(q orders
   ((< :OrderID 10300))
   (join shippers :ShipVia :ShipperID)
   (sort :OrderID)
   [:OrderID
    {:OrderDate (date-to-string :OrderDate "yyyy-mm-dd")}
    {:Shipper :CompanyName}]
   .show)












