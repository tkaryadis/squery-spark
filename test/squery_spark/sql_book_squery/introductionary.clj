(ns squery-spark.sql-book-squery.introductionary
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.sql-functions :refer [col]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.sql-book-squery.read-tables :refer [read-table]])
  (:refer-clojure)
  (:import (org.apache.spark.sql functions Dataset RelationalGroupedDataset Column)
           (java.util HashMap)))

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












