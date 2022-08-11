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
            [squery-spark.datasets.internal.common :refer [column column columns]]
            [squery-spark.datasets.schema :refer [schema-types]]
            [squery-spark.utils.utils :refer [nested2]])
  (:import [org.apache.spark.sql functions Column]
           (org.apache.spark.sql.expressions Window WindowSpec)))

;;Operators for columns

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

(defn mod [col other]
  (.mod (column col) (column other)))

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

(defn and [& cols]
  (nested2 #(.and (column %1) (column %2))  cols))

(defn or [& cols]
  (nested2 #(.or (column %1) (column %2)) cols))

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

(defn if- [col-condition col-value col-else-value]
  (.otherwise (functions/when (column col-condition) (column col-value)) (column col-else-value)))

(defn if-not [col-condition col-value col-else-value]
  (.otherwise (functions/when (functions/not (column col-condition)) (column col-value)) (column col-else-value)))



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

(defn if-nil? [col nil-value-col]
  (if- (nil? col) (column nil-value-col) (column col)))

(defn first-not-nil [& cols]
  (functions/coalesce (into-array ^Column (columns cols))))

(defn some? [col]
  (.isNotNull (column col)))

;;convert
(defn cast [col to-type]
  (.cast (column col) (c/cond
                        (c/string? to-type)
                        (c/get schema-types (c/keyword to-type))

                        (c/keyword? to-type)
                        (c/get schema-types to-type)

                        :else
                        to-type)))

(defn date
  ([col] (functions/to_date (column col)))
  ([col string-format] (functions/to_date (column col) string-format)))

(defn col [c]
  (functions/col (if (keyword? c) (name c) c)))

(defn format-number [col d]
  (functions/format_number (column col) d))


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

(defn first [col]
  (functions/first (column col)))

(defn last [col]
  (functions/last (column col)))

(defn count-distinct [& cols]
  (if (c/> (c/count cols) 1)
    (functions/count_distinct (column (c/first cols)) (into-array Column (columns (c/rest cols))))
    (functions/count_distinct (column (c/first cols)) (into-array Column []))))

;;---------------------------windowField------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;val windowSpec = Window
;                  .partitionBy("CustomerId", "date")
;                  .orderBy(col("Quantity").desc)
;                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

;;(.over (functions/rank) (.orderBy
;                                         (Window/orderBy (into-array Column []))
;                                         (into-array Column [(col :id)])))

(defn wfield
  ([acc-fun window-spec] (.over (column acc-fun) window-spec))
  ([acc-fun] (.over (column acc-fun))))

(defn wspec []
  "Call example (add field here)
     {'myfield'  (wfield (rank) (-> (wspec) (sort :price)))}"
  (Window/partitionBy (into-array Column [])))

(defn rank []
  (functions/rank))

(defn wgroup
  ([& args]
   (c/let [[wspec cols] (if (instance? WindowSpec (c/first args))
                        [(c/first args) (c/rest args)]
                        [nil args])
         cols (columns cols)]
     (if (c/nil? wspec)
       (Window/partitionBy (into-array Column cols))
       (.partitionBy wspec (into-array Column cols))))))

(defn wsort
  ([& args]
   (c/let [[wspec cols] (if (instance? WindowSpec (c/first args))
                        [(c/first args) (c/rest args)]
                        [nil args])
         cols (columns cols)]
     (if (c/nil? wspec)
       (Window/orderBy (into-array Column cols))
       (.orderBy wspec (into-array Column cols))))))

(defn wrange
  ([wspec start end]
   (.rangeBetween wspec start end))
  ([start end]
   (Window/rangeBetween start end)))

(defn wrows
  ([wspec start end]
   (.rowsBetween wspec start end))
  ([start end]
   (Window/rowsBetween start end)))

(defn window
  ([col duration] (functions/window (column col) duration))
  ([col duration slide] (functions/window (column col) duration slide)))


;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn re-find? [match col]
  (.rlike (column col) match))


(defn concat
  "works on strings, binary and arrays"
  [& cols]
  (functions/concat (into-array Column (columns cols))))

(defn str
  "concat just for strings"
  [& cols]
  (apply concat cols))

(defn count-str [col]
  (functions/length (column col)))

(defn take-str
  ([start-int len-int col] (functions/substring (column col) start-int len-int))
  ([start-int col] (functions/substring (column col) start-int Integer/MAX_VALUE)))

(defn replace [col match-col replacement-col]
  (functions/regexp_replace (column col) (column match-col) (column replacement-col)))

;;--------------------------Dates-------------------------------------------

(defn date-to-string [col date-format]
  (functions/date_format (column col) date-format))

(defn year [col]
  (functions/year (column col)))

(defn month [col]
  (functions/month (column col)))

(defn day-of-month [col]
  (functions/dayofmonth (column col)))

(defn last-day-of-month [col]
  (functions/last_day (column col)))

(defn days-diff [col-end col-start]
  (functions/datediff (column col-end)  (column col-start)))


;;--------------------------------------------------------------------------
(defn desc [col]
  (.desc (column col)))

(defn asc [col]
  (.asc (column col)))

(defn todf
  ([df & col-names]
   (.toDF df (into-array String (c/map name col-names))))
  ([df] (.toDF df)))


;;---------------------------------------------------------

;;TODO no need to override clojure, i can have internal names with other names
;;TODO possible bug if this enviroment is moved with a macro to another place for example -> does it
(def operators-mappings
  '[
    ;;Clojure overrides
    + squery-spark.datasets.operators/+
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
    if-nil? squery-spark.datasets.operators/if-nil?
    first-not-nil squery-spark.datasets.operators/first-not-nil
    some? squery-spark.datasets.operators/some?
    date squery-spark.datasets.operators/date
    format-number squery-spark.datasets.operators/format-number
    re-find? squery-spark.datasets.operators/re-find?
    contains? squery-spark.datasets.operators/contains?
    date-to-string squery-spark.datasets.operators/date-to-string
    year squery-spark.datasets.operators/year
    month squery-spark.datasets.operators/month
    day-of-month squery-spark.datasets.operators/day-of-month
    last-day-of-month squery-spark.datasets.operators/last-day-of-month
    concat squery-spark.datasets.operators/concat
    str squery-spark.datasets.operators/str
    take-str squery-spark.datasets.operators/take-str
    count-str squery-spark.datasets.operators/count-str
    replace squery-spark.datasets.operators/replace

    ;;accumulators
    sum squery-spark.datasets.operators/sum
    avg squery-spark.datasets.operators/avg
    max squery-spark.datasets.operators/max
    min squery-spark.datasets.operators/min
    count-a squery-spark.datasets.operators/count-a
    conj-each squery-spark.datasets.operators/conj-each
    conj-set  squery-spark.datasets.operators/conj-set
    wfield squery-spark.datasets.operators/wfield
    rank  squery-spark.datasets.operators/rank
    wgroup squery-spark.datasets.operators/wgroup
    wsort squery-spark.datasets.operators/wsort
    wrange squery-spark.datasets.operators/wrange
    wrows squery-spark.datasets.operators/wrows
    window squery-spark.datasets.operators/window
    first squery-spark.datasets.operators/first
    last squery-spark.datasets.operators/last
    count-distinct squery-spark.datasets.operators/count-distinct

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
    todf squery-spark.datasets.operators/todf

    ;;stages
    sort squery-spark.datasets.stages/sort
    group squery-spark.datasets.stages/group
    unset squery-spark.datasets.stages/unset
    count-s squery-spark.datasets.stages/count-s
    limit squery-spark.datasets.stages/limit
    join squery-spark.datasets.stages/join
    union-with squery-spark.datasets.stages/union-with
    union-by-name squery-spark.datasets.stages/union-by-name
    as squery-spark.datasets.stages/as
    intersection-with squery-spark.datasets.stages/intersection-with
    difference-with squery-spark.datasets.stages/difference-with
    difference-all-with squery-spark.datasets.stages/difference-all-with

    ;;delta
    merge- squery-spark.delta-lake.queries/merge-

    ])

(def core-operators-mappings
  '[
    + clojure.core/+
    inc clojure.core/inc
    - clojure.core/-
    dec clojure.core/dec
    * clojure.core/*
    = clojure.core/=
    not= clojure.core/not=
    > clojure.core/>
    >= clojure.core/>=
    < clojure.core/<
    <= clojure.core/<=
    ;and clojure.core/and          ;;TODO
    ;or clojure.core/or
    ;if-not clojure.core/if-not
    true? clojure.core/true?
    false? clojure.core/false?
    nil? clojure.core/nil?
    some? clojure.core/some?
    contains? clojure.core/contains?
    concat clojure.core/concat
    str clojure.core/str


    ])