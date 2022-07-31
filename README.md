# SQuery-spark

A Clojure library for apache spark    
~2x less code than java    
~1.5x less code than SQL, but simpler and programmable not queries in strings    

SQuery goal is to be simpler than SQL and the same time programmable  

## Example

```
(q df
   ((> :InvoiceNo 536365) (> :Quantity 2))
   {:afield "10"}
   (unset :afield)
   (group :InvoiceNo
           {:sum (sum :UnitPrice)}
           {:avg (avg :UnitPrice)})
   [{:sumavg (div :sum :avg)}]
   .show)
```

The above using Java interop

```
(-> df
   (.filter ^Dataset (.and (.gt (col "InvoiceNo") 536365)
                           (.gt (col "Quantity") 6)))
   (.withColumns (HashMap. {"afield" (lit 10)}))
   (.drop (col "afield"))
   (.group (into-array [(col "InvoiceNo")]))
   (.agg (.as (sum "UnitPrice") "sum")
         (.as (avg "UnitPrice") "avg"))
   (.select (into-array [(div (col "sum") (col "avg"))]))
   (.show))
```


## Usage

For overview `test.query_spark.qtest.clj`  
For SQL examples implemented in SQuery see the folder `squery-spark.sql-book-squery`

Don't use yet.  
It's under construction.For now a small subset of the Dataset api's are implemented.    
SQuery-spark follows the syntax of [cmql](https://cmql.org/documentation/) (made for MongoDB).  

## License

Copyright © 2022 Takis Karyadis  
Distributed under the Eclipse Public License version 1.0
