# SQuery-spark

Querying and data processing Clojure library, for Apache-Spark, and Delta Lake.

Clojure is used because    
 1. runs on JVM    
 2. its a general programming language
 3. has macros and its ideal language to make DSL's     
 4. its dynamic simple and practical    
 5. its functional allowing natural data processing   
 6. it has tree syntax, and can represent more complex nested pipelines   

This allows Clojure to be simpler from all alternatives Java,Scala,Python.  
And because it's a general programming language we don't have to write queries
in strings like in SQL.

## Design goals

1. to have Clojure like syntax, and Clojure names,
   (see [cMQL](https://cmql.org/documentation/) the MongoDB query language that is based on Clojure syntax)
2. use macro to use the Clojure operators inside the queries without namespace qualified names
3. be simple as compact and simple as possible
4. be programmable, no code in strings like SQL
5. be simpler than all alternatives, Java,Scala,Python, including SQL

Overall to feel as Clojure was a query language for spark.

## Example

Example is very simple, in more complicated queries difference is much bigger

SQuery

```
(q df
   {:isExpensive (and (= :StockCode "DOT")
                      (or (> :UnitPrice 600) (substring? "POSTAGE" :description)))}
   ((true? :isExpensive))
   [:UnitPrice :isExpensive]
   (show 5))
```

Scala

```
val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
.where("isExpensive")
.select("unitPrice", "isExpensive").show(5)
```

Python
```
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)
```

SQL
```
SELECT UnitPrice, (StockCode = 'DOT' AND
(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
FROM dfTable
WHERE (StockCode = 'DOT' AND
(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
```

## Learn

For overview `test.query_spark.qtest.clj`  

See test/squery_spark, for now 2 books code is written in SQuery

1. SQL Practice Problems: 57 beginning, intermediate, and advanced challenges for you to solve
   using a “learn-by-doing” approach , by Sylvia Moestl Vasilik  (completed)
2. Spark: The Definitive Guide: Big Data Processing Made Simple (under construction)  

## Usage

Don't use yet. It's under construction and constantly changes. Use it for testing only.

## License

Copyright © 2022 Takis Karyadis  
Distributed under the Eclipse Public License version 1.0
