(ns squery-spark.datasets.schema
  (:import (org.apache.spark.sql.types DataTypes Metadata StructField StructType)))

(def schema-types
  {:binary    DataTypes/BinaryType
   :boolean   DataTypes/BooleanType
   :byte      DataTypes/ByteType
   :date      DataTypes/DateType
   :double    DataTypes/DoubleType
   :float     DataTypes/FloatType
   :integer   DataTypes/IntegerType
   :int       DataTypes/IntegerType
   :long      DataTypes/LongType
   :null      DataTypes/NullType
   :short     DataTypes/ShortType
   :string    DataTypes/StringType
   :timestamp DataTypes/TimestampType})

(defn array-type
  "create spark array type"
  ([element-type nullable?]
   (if (keyword? element-type)
     (DataTypes/createArrayType (get schema-types element-type) nullable?)
     (DataTypes/createArrayType element-type nullable?)))
  ([element-type] (array-type element-type true)))


;;structField=metadata of Column
;;structType=array of structFiels = header
;;default string-type ,null true and no metadata
(defn build-schema [cols-info]
  "posible col-info = [[name0] [name1 type1] [name2 type2 false] [name3 type3 false metadata]]
   or :name0 or 'name0'  keywords and strings no need for vector"
  (let [structfields
        (reduce (fn [fields col-info]
                  (conj fields (StructField. (cond
                                               (keyword? col-info)
                                               (name col-info)

                                               (string? col-info)
                                               col-info

                                               (keyword? (first col-info))
                                               (name (first col-info))

                                               :else
                                               (first col-info))

                                             (if (and (coll? col-info) (>= (count col-info) 2))
                                               (get schema-types (second col-info) (second col-info))
                                               DataTypes/StringType)
                                             (if (and (coll? col-info) (>= (count col-info) 3))
                                               (nth col-info 2)
                                               true)
                                             (Metadata/empty))))
                []
                cols-info)]
    (StructType. (into-array structfields))))


(defn header [df]
  (into [] (.columns df)))

(defn todf
  ([df & col-names]
   (.toDF df (into-array String (map name col-names))))
  ([df] (.toDF df)))