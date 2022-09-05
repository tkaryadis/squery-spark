# SQuery-spark

Querying and data processing Clojure library, for Apache-Spark

# Rationale

SQL is a domain specific language, not a general programming language     
Python/Scala/Java are general programming languages, but not domain specific languages      

Clojure can do both, in simple way, simpler than all alternatives.   

**Clojure is used because**
1. using Clojure macros, we can make a DSL   
2. its general programming language also
3. DSL code and normal Clojure code can be combined 
3. runs on JVM like spark
4. its dynamic simple and practical    
5. its functional allowing natural data processing   
6. its syntax provides support for nested pipelines(tree pipelines) not just vertical pipelines

This allows Clojure to be simpler and also more powerful from all alternatives Java,Scala,Python,SQL. 

## Design goals

1. to have Clojure like syntax, and Clojure names,  
   (see [cMQL](https://cmql.org/documentation/) the MongoDB query language that is based on Clojure syntax)
2. use macro to use the Clojure operators inside the queries without the need for namespace qualified names
3. be simple as compact and simple as possible
4. be programmable, not code in strings like SQL
5. be simpler than all alternatives, Java,Scala,Python, including SQL

Overall to feel as Clojure was a query language for spark.  

## Example

Example is very simple, in more complicated queries difference is much bigger  

SQuery

```
(q df
   {:isExpensive 
    (and (= :StockCode "DOT") (or (> :UnitPrice 600) (substring? "POSTAGE" :description)))}
   ((true? :isExpensive))
   [:UnitPrice :isExpensive])
```

Scala

```
val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
.where("isExpensive")
.select("unitPrice", "isExpensive")
```

Python
```
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive")
```

SQL
```
SELECT UnitPrice, (StockCode = 'DOT' AND
(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
FROM dfTable
WHERE (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
```

## SQuery mixed with Clojure

Example of getting the working days and their count from a difference of 2 dates
```
(q t1
   [{:date1 (date "2006-11-09" "yyyy-MM-dd")}
    {:date2 (date "2006-12-09" "yyyy-MM-dd")}]
   {:diff (days-diff :date2 :date1)}
   {:working-dates  (reduce (fn [v t]
                              (let [dt (add-days :date1 (int t))
                                    dt-n (day-of-week dt)]
                                (if- (and (not= dt-n 1) (not= dt-n 7))
                                  (conj v dt)
                                  v)))
                            (date-array [])
                            (range :diff))}
   {:working-days-count (count :working-dates)}
   (show false))
```

## Learn

Examples are added by solving SQL problems with SQuery.    
Databases used for data storage is MongoDB but you can use any.   
MongoDB is used because SQuery run on MongoDB also so with almost same syntax both are queried. 

1. SQL Practice Problems: 57 beginning, intermediate, and advanced challenges for you to solve
   using a “learn-by-doing” approach , by Sylvia Moestl Vasilik  (completed)
2. Spark: The Definitive Guide: Big Data Processing Made Simple (partially)  
3. SQL Cookbook: Query Solutions and Techniques for All SQL Users (partially)

*squery is work in progress, so some examples might give compile errors if squery changed

## Usage

Don't use yet. It's under construction and constantly changes. Use it for testing only.  
For now the SQL using the dataframe API is supported, and RDD support is added.

## License

Copyright © 2022 Takis Karyadis  
Distributed under the Eclipse Public License version 1.0
