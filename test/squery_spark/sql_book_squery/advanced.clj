(ns squery-spark.sql-book-squery.advanced
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.sql-book-squery.read-tables :refer [read-table]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import [org.apache.spark.sql functions Column]
           (org.apache.spark.sql.expressions Window WindowSpec)))

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

;;38
(q order-details
   ((>= :Quantity 60))
   (group :OrderID :Quantity
          {:count (sum 1)})
   ((> :count 1))
   (sort :OrderID)
   .show)

;;39 ;;40 (spark way is like 40 solution)
(q order-details
   (join-eq (q order-details
               ((>= :Quantity 60))
               (group :OrderID :Quantity
                      {:count (sum 1)})
               ((> :count 1))
               (group :OrderID))
            :OrderID)
   (sort :OrderID :Quantity)
   .show)

;;41
(q orders
   ((>= :ShippedDate :RequiredDate ))
   [:OrderID :OrderDate :RequiredDate :ShippedDate :EmployeeID]
   (sort :OrderID)
   (.show 100))

;;42
(q orders
   ((>= :ShippedDate :RequiredDate ))
   [:EmployeeID]
   (join-eq employees :EmployeeID)
   (group :EmployeeID :LastName
          {:totalLateOrders (sum 1)})
   (sort :!totalLateOrders)
   .show)

;;43
(q orders
   {:totalOrders (wfield (sum 1) (wgroup :EmployeeID))}
   ((>= :ShippedDate :RequiredDate ))
   [:EmployeeID :totalOrders]
   (join-eq employees :EmployeeID)
   (group :EmployeeID :LastName
          {:totalLateOrders (sum 1)}
          {:totalOrders (first :totalOrders)})
   (sort :!totalOrders)
   .show)

;;44 ;;45
(q orders
   {:totalOrders (wfield (sum 1) (wgroup :EmployeeID))}
   (join-eq employees :EmployeeID)
   (group :EmployeeID :LastName
          {:totalLateOrders (sum (if- (>= :ShippedDate :RequiredDate )
                                      1
                                      0))}
          {:totalOrders (first :totalOrders)})
   (sort :!totalOrders)
   .show)

;;46 47
(q orders
   {:totalOrders (wfield (sum 1) (wgroup :EmployeeID))}
   (join-eq employees :EmployeeID)
   (group :EmployeeID :LastName
          {:totalLateOrders (sum (if- (>= :ShippedDate :RequiredDate )
                                      1
                                      0))}
          {:totalOrders (first :totalOrders)})
   {:percentage  (if- (= :totalOrders 0)
                     0
                     (format-number (* 100 (div :totalLateOrders :totalOrders)) 2))}
   (sort :!totalOrders)
   .show)

;;48 49
(q orders
   ((= (year :OrderDate) 2016))
   (join-eq order-details :OrderID)
   (join-eq customers :CustomerID)
   (group :CustomerID
          {:TotalOrderAmount (sum (* :UnitPrice :Quantity))}
          {:CompanyName (first :CompanyName)})
   {:CustomerGroup (if- (> :TotalOrderAmount 10000)
                        "Very High"
                        (if- (> :TotalOrderAmount 5000)
                             "High"
                             (if- (> :TotalOrderAmount 1000)
                                  "Medium"
                                  "Low")))}
   [:CustomerID :CompanyName :TotalOrderAmount :CustomerGroup]
   (sort :CustomerID)
   (.show 100))

;;50  ;;51(skipped)
(q orders
   ((= (year :OrderDate) 2016))
   (join-eq order-details :OrderID)
   (group :CustomerID
          {:TotalOrderAmount (sum (* :UnitPrice :Quantity))})
   {:CustomerGroup (if- (> :TotalOrderAmount 10000)
                     "Very High"
                     (if- (> :TotalOrderAmount 5000)
                       "High"
                       (if- (> :TotalOrderAmount 1000)
                         "Medium"
                         "Low")))
    :Total (wfield (sum 1))}
   (group :CustomerGroup
          {:TotalInGroup (sum 1)}
          {:Total (first :Total)})
   {:pencentage (div :TotalInGroup :Total)}
   (.show 100))


;;52
(q customers
   [:Country]
   (union-with (q suppliers
                  [:Country]))
   (group :Country)
   (sort :Country)
   (.show 100))

;;53
(q customers
   [{:CustomerCountry :Country}]
   (unset :Country)
   (join-eq (q suppliers
               [{:SupplierCountry :Country}]
               (unset :Country))
            :CustomerCountry :SupplierCountry :fullouter)
   (group :SupplierCountry :CustomerCountry)
   (.show 100))

;;54
(q customers
   (group [{:CustomerCountry :Country}]
          {:TotalCustomers (sum 1)})
   (join-eq (q suppliers
               (group [{:SupplierCountry :Country}]
                      {:TotalSuppliers (sum 1)}))
            :CustomerCountry :SupplierCountry :fullouter)
   [{:Country (if-nil? :SupplierCountry :CustomerCountry)}
    {:TotalSuppliers (if-nil? :TotalSuppliers 0)}
    {:TotalCustomers (if-nil? :TotalCustomers 0)}]
   (sort :Country)
   (.show 100))

;;55
(q orders
   ((some? :ShipCountry))
   (sort :OrderID)
   (group :ShipCountry
          {:customer-id (first :CustomerID)}
          {:order-id (first :OrderID)}
          {:order-date (first :OrderDate)})
   (.show 100))

(def orders_next (df "Orders"))

;;56  TODO ALIAS
(q orders
   [:CustomerID {:InitialOrderDate :OrderDate} {:InitialOrderID :OrderID}]
   (join (q orders_next
            [:CustomerID
             {:NextOrderDate :OrderDate}
             {:NextOrderID :OrderID}])
         (and (= [orders :CustomerID] [orders_next :CustomerID])
              (< :InitialOrderDate :NextOrderDate)
              (> (days-diff :NextOrderDate :InitialOrderDate) 0)
              (<= (days-diff :NextOrderDate :InitialOrderDate) 5)))
   {:daysDiff (days-diff :NextOrderDate :InitialOrderDate)}
   (unset [orders :CustomerID])
   (sort :CustomerID)
   (.show 100))

;;57 TODO
#_(q orders
   (group :CustomerID (functions/window (col "OrderDate") "5 days")
          {:sum (sum 1)})
   (sort :CustomerID)
   (.show 100 false))

