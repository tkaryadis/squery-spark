(ns squery-spark.datasets.operators
  (:refer-clojure :exclude [+ inc - dec * mod
                            = not= > >= < <=
                            and or not
                            if-not cond
                            into type cast boolean double int long string?  nil? some? true? false?
                            string? int? decimal? double? boolean? number? rand
                             get get-in assoc assoc-in dissoc
                            concat conj contains? range reverse count take subvec empty?
                            fn map filter reduce
                            first last merge max min
                            str subs re-find re-matcher re-seq replace identity])
  (:require [clojure.core :as c]
            [squery-spark.datasets.internal.common :refer [column column columns]])
  (:import [org.apache.spark.sql functions Column]))

;;---------------------------Arithmetic-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn abs [col]
  (functions/abs (column col)))

(defn + [col1 col2]
  (.plus (column col1) (column col2)))

(defn inc [col]
  (.plus (column col) 1))

(defn - [col1 col2]
  (.minus (column col1) (column col2)))

(defn dec [col]
  (.minus (column col) 1))

(defn * [col1 col2]
  (.multiply  (column col1) (column col2)))

(defn mod [col]
  (.mod (column col)))

(defn pow [col1 col2]
  (functions/pow (column col1) (column col2)))

(defn exp [col]
  (functions/exp (column col)))

;;TODO ln missing

(defn log
  ([col] (functions/log (column col)))
  ([base col] (functions/log base (column col))))

(defn ceil [col]
  (functions/ceil (column col)))

(defn floor [col]
  (functions/floor (column col)))

(defn round
  ([col] (functions/round (column col)))
  ([col scale] (functions/round (column col) scale)))

(defn trunc  [date-col string-format]
  (functions/trunc (column date-col) string-format))

(defn sqrt [col]
  (functions/sqrt (column col)))

(defn div [col1 col2]
  (.divide  (column col1) (column col2)))


;;---------------------------Comparison-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;TODO SEE WHEN COLUMN OR column

(defn = [col1 col2]
  (.equalTo (column col1) (column col2)))

(defn not= [col1 col2]
  (.notEqual (column col1) (column col2)))

(defn > [col1 col2]
  (.gt (column col1) (column col2)))

(defn >= [col1 col2]
  (.geq (column col1) (column col2)))

(defn < [col1 col2]
  (.lt (column col1) (column col2) ))

(defn <= [col1 col2]
  (.leq (column col1) (column col2)))


;;---------------------------Boolean----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


(defn and [col1 col2]
  (.and (column col1) (column col2)))

(defn or [col1 col2]
  (.or (column col1) (column col2)))

(defn not [col]
  (functions/not (column col)))

;;TODO not?
;;TODO nor?


;;---------------------------Conditional------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;people.select(when(people("gender") === "male", 0)
;     .when(people("gender") === "female", 1)
;     .otherwise(2))

;;otherwise(Object value)
;;when(Column condition, Object value)

(defn if- [col-condition value else-value]
  (.otherwise (functions/when (column col-condition) value) else-value))

(defn if-not [col-condition value else-value]
  (.otherwise (functions/when (functions/not ^Column (column col-condition)) value) else-value))



;;---------------------------Literal----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn lit [v]
  (functions/lit v))


;;---------------------------Types and Convert------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;type predicates
(defn true? [col]
  (.equalTo (column col) true))

(defn false? [col]
  (.equalTo (column col) false))

(defn nil?
  "(= :field nil) Doesn't work use this only for nil"
  [col]
  (.isNull (column col)))

(defn not-nil? [col]
  (.isNotNull (column col)))

;;convert
(defn cast [col to-type]
  (.cast (column col) (if (keyword? to-type)
                        (name to-type)
                        to-type)))

(defn date [col]
  (functions/to_date (column col)))




;;---------------------------Arrays-----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn contains? [values col]
  (.isin (column col) (into-array values)))


;;-----------------SET (arrays/objects and nested)--------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;---------------------------Arrays(set operations)-------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;---------------------------Arrays(map/filter/reduce)----------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;---------------------------Accumulators-----------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn sum [col]
  (functions/sum (column col)))

(defn avg [col]
  (functions/avg (column col)))

(defn max [col]
  (functions/max (column col)))

(defn min [col]
  (functions/min (column col)))

(defn count-a
  ([] (functions/count (lit 1)))
  ([col] (functions/count (column col))))

(defn conj-each [col]
  (functions/collect_list ^Column (column col)))

(defn conj-set [col]
  (functions/collect_set ^Column (column col)))

;;---------------------------windowField------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn re-find? [match col]
  (.rlike (column col) match))


(defn concat
  "works on strings, binary and arrays"
  [& cols]
  (functions/concat (into-array ^Column (columns cols))))

(defn str
  "concat just for strings"
  [& cols]
  (apply concat cols))


;;--------------------------Dates-------------------------------------------

(defn date-to-string [col date-format]
  (functions/date_format (column col) date-format))

(defn year [col]
  (functions/year (column col)))

(defn month [col]
  (functions/month (column col)))

(defn days-diff [col-end col-start]
  (functions/datediff (column col-end)  (column col-start)))


;;--------------------------------------------------------------------------
(defn desc [col]
  (.desc (column col)))

(defn asc [col]
  (.asc (column col)))

;;---------------------------------------------------------

(def operators-mappings
  '[+ squery-spark.datasets.operators/+
    inc squery-spark.datasets.operators/inc
    - squery-spark.datasets.operators/-
    dec squery-spark.datasets.operators/dec
    * squery-spark.datasets.operators/*
    = squery-spark.datasets.operators/=
    not= squery-spark.datasets.operators/not=
    > squery-spark.datasets.operators/>
    >= squery-spark.datasets.operators/>=
    < squery-spark.datasets.operators/<
    <= squery-spark.datasets.operators/<=
    and squery-spark.datasets.operators/and
    or squery-spark.datasets.operators/or
    if- squery-spark.datasets.operators/if-
    if-not squery-spark.datasets.operators/if-not
    true? squery-spark.datasets.operators/true?
    false? squery-spark.datasets.operators/false?
    nil? squery-spark.datasets.operators/nil?
    not-nil? squery-spark.datasets.operators/not-nil?
    date squery-spark.datasets.operators/date
    re-find? squery-spark.datasets.operators/re-find?
    contains? squery-spark.datasets.operators/contains?
    date-to-string squery-spark.datasets.operators/date-to-string
    year squery-spark.datasets.operators/year
    month squery-spark.datasets.operators/month
    concat squery-spark.datasets.operators/concat
    str squery-spark.datasets.operators/str

    ;;accumulators
    sum squery-spark.datasets.operators/sum
    avg squery-spark.datasets.operators/avg
    max squery-spark.datasets.operators/max
    min squery-spark.datasets.operators/min
    count-a squery-spark.datasets.operators/count-a
    conj-each squery-spark.datasets.operators/conj-each
    conj-set  squery-spark.datasets.operators/conj-set

    ;;Not clojure overides

    abs squery-spark.datasets.operators/abs
    pow squery-spark.datasets.operators/pow
    exp squery-spark.datasets.operators/exp
    ;ln  squery-spark.datasets.operators/ln
    log squery-spark.datasets.operators/log
    ceil squery-spark.datasets.operators/ceil
    floor squery-spark.datasets.operators/floor
    round squery-spark.datasets.operators/round
    trunc squery-spark.datasets.operators/trunc
    sqrt squery-spark.datasets.operators/sqrt
    mod squery-spark.datasets.operators/mod
    div squery-spark.datasets.operators/div
    lit squery-spark.datasets.operators/lit
    cast squery-spark.datasets.operators/cast


    asc squery-spark.datasets.operators/asc
    desc squery-spark.datasets.operators/desc

    ;;stages
    sort squery-spark.datasets.stages/sort
    group squery-spark.datasets.stages/group
    unset squery-spark.datasets.stages/unset
    count-s squery-spark.datasets.stages/count-s
    limit squery-spark.datasets.stages/limit
    join squery-spark.datasets.stages/join

    ])