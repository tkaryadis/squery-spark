# SQuery-spark

Querying and data processing Clojure library, for Apache-Spark, and Delta Lake.

# Rationale

SQL is a domain specific language but its not a general programming language
Python/Scala/Java are general programming languages, but not domain specific languages
Use one or them, or combine them with code and queries in strings is problematic.

Clojure can do both, in simple way, its dynamic and functional running on JVM

Clojure is used because
1. we can make domain specific language with it so we don't need SQL
   (has macros and its homoiconic making it ideal language to make DSL's ) 
2. its general programming language also 
3. runs on JVM like spark
4. its dynamic simple and practical    
5. its functional allowing natural data processing   
6. it has tree syntax, and nesting is supported from the language syntax
   (not just line pipelines, but tree pipelines also)

This allows Clojure to be simpler and more powerful from all alternatives Java,Scala,Python,SQL. 

## Design goals

1. to have Clojure like syntax, and Clojure names,
   (see [cMQL](https://cmql.org/documentation/) the MongoDB query language that is based on Clojure syntax)
2. use macro to use the Clojure operators inside the queries without namespace qualified names
3. be simple as compact and simple as possible
4. be programmable, not code in strings like SQL
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
