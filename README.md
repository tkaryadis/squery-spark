# SQuery-spark

Query language for Apache-Spark.    

Clojure is used because    
 1. runs on JVM    
 2. its a general programming language
 3. has macros and its ideal language to make DSL's     
 4. its dynamic simple and practical    
 5. its functional allowing natural data processing   
 6. it has tree syntax, and can represent more complex pipelines   

This allows Clojure to be simpler from all alternatives Java,Scala,Python.  
And because it's a general programming language we don't have to write queries
in strings like in SQL.

## Design goals

1. to have on Clojure syntax, and Clojure names, Clojure way  
   (see [cMQL](https://cmql.org/documentation/) the MongoDB query language that is based on Clojure syntax)
2. use macro to use the Clojure operators inside the queries without namespace qualified names
3. be simple as compact and simple as possible
4. be programmable, no code in strings like SQL
5. be simpler than all alternatives, Java,Scala,Python, including SQL

Overall to feel as Clojure was a query language
 
## Example

```
(q df
   ((> :InvoiceNo 536365) (> :Quantity 2))
   {:afield "10"}
   (unset :afield)
   (group :InvoiceNo
           {:sum (sum :UnitPrice)}
           {:avg (avg :UnitPrice)})
   [{:sumavg (div :sum :avg)}])
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
   (.select (into-array [(div (col "sum") (col "avg"))])))
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
