(ns squery-spark.datasets.operators
  (:refer-clojure :exclude [+ inc - dec * mod
                            = not= > >= < <=
                            and or not
                            if-not cond
                            into type cast boolean double int long string?  nil? some? true? false?
                            string? int? decimal? double? boolean? number? rand
                            let get get-in assoc assoc-in dissoc
                            concat conj contains? range reverse count take subvec empty?
                            fn map filter reduce
                            first last merge max min
                            str subs re-find re-matcher re-seq replace identity])
  (:require [clojure.core :as c]
            [squery-spark.datasets.internal.common :refer [column-keyword]])
  (:import [org.apache.spark.sql functions]))

;;---------------------------Arithmetic-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn abs [col]
  (functions/abs (column-keyword col)))

(defn + [col1 col2]
  (.plus (column-keyword col1) (column-keyword col2)))

(defn inc [col]
  (.plus (column-keyword col) 1))

(defn - [col1 col2]
  (.minus (column-keyword col1) (column-keyword col2)))

(defn dec [col]
  (.minus (column-keyword col) 1))

(defn * [col1 col2]
  (.multiply  (column-keyword col1) (column-keyword col2)))

(defn mod [col]
  (.mod (column-keyword col)))

(defn pow [col1 col2]
  (functions/pow (column-keyword col1) (column-keyword col2)))

(defn exp [col]
  (functions/exp (column-keyword col)))

;;TODO ln missing

(defn log
  ([col] (functions/log (column-keyword col)))
  ([base col] (functions/log base (column-keyword col))))

(defn ceil [col]
  (functions/ceil (column-keyword col)))

(defn floor [col]
  (functions/floor (column-keyword col)))

(defn round
  ([col] (functions/round (column-keyword col)))
  ([col scale] (functions/round (column-keyword col) scale)))

(defn trunc  [date-col string-format]
  (functions/trunc (column-keyword date-col) string-format))

(defn sqrt [col]
  (functions/sqrt (column-keyword col)))

(defn div [col1 col2]
  (.divide  (column-keyword col1) (column-keyword col2)))


;;---------------------------Comparison-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;TODO cmp

(defn = [col1 col2]
  (.equalTo (column-keyword col1) (column-keyword col2)))

(defn not= [col1 col2]
  (.notEqual (column-keyword col1) (column-keyword col2)))

(defn > [col1 col2]
  (.gt (column-keyword col1) (column-keyword col2)))

(defn >= [col1 col2]
  (.geq (column-keyword col1) (column-keyword col2)))

(defn < [col1 col2]
  (.lt (column-keyword col1) (column-keyword col2) ))

(defn <= [col1 col2]
  (.leq (column-keyword col1) (column-keyword col2)))


;;---------------------------Boolean----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


(defn and [col1 col2]
  (.and (column-keyword col1) (column-keyword col2)))

(defn or [col1 col2]
  (.or (column-keyword col1) (column-keyword col2)))

;;TODO not?
;;TODO nor?


;;---------------------------Conditional------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;TODO NOT CONDITIONAL? IN SPARK?
;;for example if column-keyword=true do this


;;---------------------------Literal----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn lit [v]
  (functions/lit v))


;;---------------------------Types and Convert------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn cast [col to-type]
  (.cast (column-keyword col) (if (keyword? to-type)
                        (name to-type)
                        to-type)))

(defn true? [col]
  (.equalTo (column-keyword col) true))

(defn false? [col]
  (.equalTo (column-keyword col) false))

(defn nil? [col]
  (.isNull (column-keyword col)))

(defn not-nil? [col]
  (.isNotNull (column-keyword col)))


;;---------------------------Arrays-----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


#_(defn contains? [col1 & values]
  (.isin col1 (into-array values)))

#_(defmacro includes? [col1 value]
  `(.contains ~col1 ~value))


;;-----------------SET (arrays/objects and nested)--------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;??

;;---------------------------Arrays(set operations)-------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;---------------------------Arrays(map/filter/reduce)----------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;??

;;---------------------------Accumulators-----------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn sum [col]
  (functions/sum (column-keyword col)))

(defn avg [col]
  (functions/avg (column-keyword col)))

;;---------------------------windowField------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


(defn desc [col]
  (.desc (column-keyword col)))

(defn asc [col]
  (.asc (column-keyword col)))

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
    true? squery-spark.datasets.operators/true?
    false? squery-spark.datasets.operators/false?
    nil? squery-spark.datasets.operators/nil?
    not-nil? squery-spark.datasets.operators/not-nil?

    ;;accumulators
    sum squery-spark.datasets.operators/sum
    avg squery-spark.datasets.operators/avg

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

    ])