(ns squery-spark.delta-lake.schema
  (:require [squery-spark.datasets.schema :refer [schema-types]])
  (:import (org.apache.spark.sql.types DataTypes StructType Metadata StructField)
           (io.delta.tables DeltaTableBuilder)))

;;;;DeltaTable.createOrReplace(spark)
;;  .addColumn("id", "INT")
;;  .addColumn("firstName", "STRING")
;;  .addColumn("middleName", "STRING")
;;  .addColumn(
;;    DeltaTable.columnBuilder("lastName")
;;      .dataType("STRING")
;;      .comment("surname")
;;      .build())
;;  .addColumn("lastName", "STRING", comment = "surname")
;;  .addColumn("gender", "STRING")
;;  .addColumn("birthDate", "TIMESTAMP")
;;  .addColumn("ssn", "STRING")
;;  .addColumn("salary", "INT")
;;  .property("description", "table with people data")
;;  .location("/tmp/delta/people10m")
;;  .execute()

;;DeltaTable.columnBuilder("lastName")
;      .dataType("STRING")
;      .comment("surname")
;      .build()

;;structField=metadata of Column
;;structType=array of structFiels = header
;;default string-type ,null true and no metadata
(defn table-columns [^DeltaTableBuilder table-builder cols-info]
  "posible col-info = [[name0] [name1 type1] [name2 type2 false] [name3 type3 false metadata]]"
  (reduce (fn [table-builder col-info]
            (let [col-name (cond
                             (keyword? col-info)
                             (name col-info)

                             (keyword? (first col-info))
                             (name (first col-info))

                             :else
                             (first col-info))
                  col-type (if (and (coll? col-info) (> (count col-info) 1))
                             (get schema-types (second col-info) (second col-info))
                             DataTypes/StringType)
                  col-nullable? (if (and (coll? col-info) (> (count col-info) 2))
                                 (nth col-info 2)
                                 true)
                  col-meta (if (and (coll? col-info) (> (count col-info) 3))
                                (nth col-info 2)
                                (Metadata/empty))
                  struct-field (StructField. col-name col-type col-nullable? col-meta)]
              (.addColumn table-builder struct-field)))
          table-builder
          cols-info))