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
                            first second last merge max min
                            str subs re-find re-matcher re-seq replace identity
                            long-array])
  (:require [clojure.core :as c]
            [squery-spark.datasets.internal.common :refer [column column columns]]
            [squery-spark.datasets.schema :refer [schema-types]]
            [squery-spark.utils.utils :refer [nested2 nested3]]
            [squery-spark.datasets.schema :refer [array-type]]
            [squery-spark.datasets.internal.common :refer [column]]
            [squery-spark.utils.interop :refer [clj->scala1]]
            [erp12.fijit.collection :refer [to-scala-seq to-scala-list]])
  (:import [org.apache.spark.sql functions Column Dataset]
           (org.apache.spark.sql.expressions Window WindowSpec)
           (scala Function1 Function2)))

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
  "rounds up 0.5=1.0"
  ([col] (functions/round (column col)))
  ([col scale] (functions/round (column col) scale)))

(defn bround
  "rounds down 0.5 =0"
  [col]
  (functions/bround (column col)))

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

(defn =safe [col1 col2]
  (.eqNullSafe (column col1) (column col2)))

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

(defn cond [& args]
  (if (c/<= (c/count args) 4)
    (if (c/= (c/count args) 4)     ;;the normal case cond with 2>= cases
      (if- (c/first args) (c/second args) (c/nth args 3))
      (throw (Exception. "Wrong number of arguments, cond requires >=4  arguments and even")))
    (if- (c/first args) (c/second args) (apply cond (rest (rest args))))))


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

(defn coalesce
  "returns the first not nil value"
  [& cols]
  (functions/coalesce (into-array Column (columns cols))))

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

(defn string [col]
  (.cast (column col) (c/get schema-types :string)))

(defn long [col]
  (.cast (column col) (c/get schema-types :long)))

(defn long-array
  ([col] (cast (column col) (array-type :long)))
  ([] (cast (column []) (array-type :long))))

(defn string-array
  ([col] (cast (column col) (array-type :string)))
  ([] (cast (column []) (array-type :string))))

(defn date
  ([col] (functions/to_date (column col)))
  ([col string-format] (functions/to_date (column col) string-format)))

(defn timestamp
  ([col] (functions/to_timestamp (column col)))
  ([col string-format] (functions/to_timestamp (column col) string-format)))

(defn col [c]
  (functions/col (if (keyword? c) (name c) c)))

(defn ->col [c]
  (squery-spark.datasets.internal.common/column c))


(defn format-number [col d]
  (functions/format_number (column col) d))

(defn array [& cols]
  (functions/array (into-array Column (columns cols))))

;;---------------------Structs----------------------------------------------

(defn get [col index-key]
  (c/reduce (c/fn [v t]
              (if (c/number? t)
                (functions/element_at v (c/int (c/inc t)))
                (.getField v (name t))))
            (column col)
            (if (c/vector? index-key) index-key [index-key])))


(defn assoc [col & pairs]
  (c/reduce (c/fn [v t]
              (.withField v (c/first t) (column (c/second t))))
            (column col)
            (c/partition 2 pairs)))

(defn dissoc [col & fields]
  (.dropFields ^Column (column col) (clj->scala1 fields)))


;;---------------------------Map--------------------------------------------

;;mget bellow on arrays

(defn massoc [col & pairs]
  (c/reduce (c/fn [v t]
              (functions/map_concat
                (into-array Column [v
                                    (functions/map
                                      (into-array Column [(column (c/first t)) (column (c/second t))]))])))
            (column col)
            (c/partition 2 pairs)))

;;---------------------------Arrays-----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn contains?
  "Works in all cases
  (q t1
     [{:a1 [1 2 3]} {:a2 1}]
     {:b (contains? :a1 1)
      :c (contains? [1 2 3] :a2)
      :d (contains? :a1 :a2)}
     show)
  "
  [array-col value-col]
  (c/cond
    (c/vector? array-col)                                   ;;array not column/keyword
    (.isin (column value-col) (c/into-array array-col))

    (c/and  (c/not (instance? org.apache.spark.sql.Column value-col)) ;;value not column/keyword
            (c/not (c/keyword? value-col)))
    (functions/array_contains (column array-col) value-col)

    :else                                                   ;;both columns
    (.gt (functions/size
               (functions/array_intersect (column array-col)
                                          (functions/array (c/into-array Column [(column value-col)]))))
             0)))

(defn explode [col]
  (functions/explode (column col)))

(defn explode-outer
  "like explode but explodes even if array/map empty, using null"
  [col]
  (functions/explode_outer (column col)))

;;TODO if possible make the result of f to be always columns, to allow return f clojure data also
(defn map [f col]
  (functions/transform (column col) (reify Function1 (apply [_ x] (f x)))))

(defn map-keys [col]
  (functions/map_keys (column col)))

(defn map-values [col]
  (functions/map_values (column col)))

#_(defn reduce
  "col arguments, and must return col also"
  [f initial-col col-collection]
  (functions/aggregate (column col-collection)
                       (column initial-col)
                       (reify Function2 (apply [_ x y] (f x y)))))

(defn col-f [f x y]
  (let [result (f x y)]
    (column result)))

(defn reduce
  "col arguments, and must return col also"
  [f initial-col col-collection]
  (functions/aggregate (column col-collection)
                       (column initial-col)
                       (reify Function2 (apply [_ x y] (col-f f x y)))))

(defn mget [col index-key]
  (c/reduce (c/fn [v t]
              (if (c/number? t)
                (functions/element_at v (c/int (c/inc t)))
                (functions/element_at v t)))
            (column col)
            (if (c/vector? index-key) index-key [index-key])))

(defn first [col-collection]
  (get col-collection 0))

(defn second [col-collection]
  (get col-collection 1))

(defn conj [col-array col-new-member]
  (functions/concat (into-array Column [(column col-array) (functions/array (into-array Column [col-new-member]))])))

(defn sort-array
  ([col desc?] (functions/sort_array (column col) desc?))
  ([col] (functions/sort_array (column col))))

;;sequence(Column start, Column stop)
;Generate a sequence of integers from start to stop, incrementing by 1 if start is less than or equal to stop, otherwise -1.
;static Column	sequence(Column start, Column stop, Column step)
;Generate a sequence of integers from start to stop, incrementing by step.

(defn range
  ([start-col end-col] (functions/sequence (column start-col) (column end-col)))
  ([start-col end-col step-int] (functions/sequence (column start-col) (column end-col) (c/int step-int))))


(defn count [col-array]
  (functions/size (column col-array)))


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

(defn first-a [col]
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

(defn rank
  "same order = same number, but the counter
   still increases on duplicates like 1 1 3"
  []
  (functions/rank))

(defn dense-rank
  "same order = same number, but the counter
   doesn't increase like 1 1 2"
  []
  (functions/dense_rank))

(defn row-number
  "row number inside the window"
  []
  (functions/row_number))

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

(defn re-find? [match-regex-string col]
  (.rlike (column col) match-regex-string))

(defn re-find
  ([match-regex-string col] (functions/regexp_extract (column col) match-regex-string (c/int 0)))
  ([match-regex-string col grou-idx-number] (functions/regexp_extract (column col) match-regex-string (c/int grou-idx-number))))

(defn concat
  "works on strings, binary and arrays"
  [& cols]
  (functions/concat (into-array Column (columns cols))))

(defn str
  "concat just for strings"
  [& cols]
  (apply concat cols))

;;array_join(Column column, String delimiter)
(defn join-str
  ([delimiter-string col] (functions/array_join (column col) delimiter-string))
  ([col] (functions/array_join (column col) "")))

(defn count-str [col]
  (functions/length (column col)))

(defn take-str
  ([start-int len-int col] (functions/substring (column col) start-int len-int))
  ([start-int col] (functions/substring (column col) start-int Integer/MAX_VALUE)))


(defn replace [col match-col-or-string replacement-col-or-string]
  (if (c/and (c/string? match-col-or-string) (c/string? replacement-col-or-string))
    (functions/regexp_replace (column col) match-col-or-string replacement-col-or-string)
    (functions/regexp_replace (column col) (column match-col-or-string) (column replacement-col-or-string))))

(defn split-str
  ([col pattern-string] (functions/split (column col) pattern-string))
  ([col pattern-string limit-int] (functions/split (column col) pattern-string limit-int)))

(defn substring? [str col]
  (.contains ^Column (column col) str))

(defn capitalize [col]
  (functions/initcap (column col)))

(defn lower-case [col]
  (functions/lower (column col)))

(defn upper-case [col]
  (functions/upper (column col)))

(defn trim
  ([col trim-string] (functions/trim (column col) trim-string))
  ([col] (functions/trim (column col))))

(defn triml
  ([col trim-string] (functions/ltrim (column col) trim-string))
  ([col] (functions/ltrim (column col))))

(defn trimr
  ([col trim-string] (functions/rtrim (column col) trim-string))
  ([col] (functions/rtrim (column col))))

(defn padl
  "Result will be a string of lenght=len-int,
   if the column is smaller pad-string will be added
   on the left"
  [col len-int pad-string]
  (functions/lpad (column col) (c/int len-int) pad-string))

(defn padr [col len-int pad-string]
  (functions/rpad (column col) (c/int len-int) pad-string))


(defn translate
  "replaces characters(no need for full match), based on index,
   index 2 of string-match with index 2 of string-replacement"
  [col string-match string-replacement]
  (functions/translate (column col) string-match string-replacement))



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

(defn current-date []
  (functions/current_date))

(defn current-timestamp []
  (functions/current_timestamp))

(defn add-days [col-start column-or-number]
  (if (c/number? column-or-number)
    (functions/date_add (column col-start) (c/int column-or-number))
    (functions/date_add (column col-start) (column column-or-number))))

(defn sub-days [col-start column-or-number]
  (if (c/number? column-or-number)
    (functions/date_sub (column col-start) (c/int column-or-number))
    (functions/date_sub (column col-start) (column column-or-number))))

#_(defn add-months [col-start column-or-number]
  (if (c/number? column-or-number)
    (functions/add_months (column col-start) (c/int column-or-number))
    (functions/add_months (column col-start) (column column-or-number))))

#_(defn sub-months [col-start column-or-number]
  (if (c/number? column-or-number)
    (functions/sub_months (column col-start) (c/int column-or-number))
    (functions/sub_months (column col-start) (column column-or-number))))

(defn months-between [start-col end-col]
  (functions/months_between (column start-col) (column end-col)))




;;-----------------------------Statistics-----------------------------------

(defn corr
  ([df-functions col1 col2] (.corr df-functions (.toString (column col1)) (.toString (column col2))))
  ([col1 col2] (functions/corr (column col1) (column col2))))



;;--------------------------------------------------------------------------
(defn desc [col]
  (.desc (column col)))

(defn asc [col]
  (.asc (column col)))

(defn todf
  ([df & col-names]
   (.toDF df (into-array String (c/map name col-names))))
  ([df] (.toDF df)))

(defn soundex [col]
  (functions/soundex (column col)))


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
    =safe squery-spark.datasets.operators/=safe
    not= squery-spark.datasets.operators/not=
    > squery-spark.datasets.operators/>
    >= squery-spark.datasets.operators/>=
    < squery-spark.datasets.operators/<
    <= squery-spark.datasets.operators/<=
    and squery-spark.datasets.operators/and
    or squery-spark.datasets.operators/or
    not squery-spark.datasets.operators/not
    if- squery-spark.datasets.operators/if-
    cond squery-spark.datasets.operators/cond
    if-not squery-spark.datasets.operators/if-not
    true? squery-spark.datasets.operators/true?
    false? squery-spark.datasets.operators/false?
    nil? squery-spark.datasets.operators/nil?
    if-nil? squery-spark.datasets.operators/if-nil?
    coalesce squery-spark.datasets.operators/coalesce
    some? squery-spark.datasets.operators/some?
    date squery-spark.datasets.operators/date
    timestamp squery-spark.datasets.operators/timestamp
    col squery-spark.datasets.operators/col
    ->col squery-spark.datasets.operators/->col
    format-number squery-spark.datasets.operators/format-number
    re-find? squery-spark.datasets.operators/re-find?
    re-find squery-spark.datasets.operators/re-find
    contains? squery-spark.datasets.operators/contains?
    first squery-spark.datasets.operators/first
    second squery-spark.datasets.operators/second
    explode squery-spark.datasets.operators/explode
    explode-outer squery-spark.datasets.operators/explode-outer
    map squery-spark.datasets.operators/map
    map-keys squery-spark.datasets.operators/map-keys
    map-values squery-spark.datasets.operators/map-values
    conj squery-spark.datasets.operators/conj
    sort-array squery-spark.datasets.operators/sort-array
    range squery-spark.datasets.operators/range
    count squery-spark.datasets.operators/count
    reduce squery-spark.datasets.operators/reduce
    get squery-spark.datasets.operators/get
    assoc squery-spark.datasets.operators/assoc
    dissoc squery-spark.datasets.operators/dissoc
    mget squery-spark.datasets.operators/mget
    massoc squery-spark.datasets.operators/massoc
    date-to-string squery-spark.datasets.operators/date-to-string
    year squery-spark.datasets.operators/year
    month squery-spark.datasets.operators/month
    day-of-month squery-spark.datasets.operators/day-of-month
    last-day-of-month squery-spark.datasets.operators/last-day-of-month
    days-diff squery-spark.datasets.operators/days-diff
    current-date squery-spark.datasets.operators/current-date
    current-timestamp squery-spark.datasets.operators/current-timestamp
    add-days squery-spark.datasets.operators/add-days
    sub-days squery-spark.datasets.operators/sub-days
    ;;add-months squery-spark.datasets.operators/add-months
    ;;sub-months squery-spark.datasets.operators/sub-months
    months-between squery-spark.datasets.operators/months-between
    concat squery-spark.datasets.operators/concat
    str squery-spark.datasets.operators/str
    join-str squery-spark.datasets.operators/join-str
    take-str squery-spark.datasets.operators/take-str
    count-str squery-spark.datasets.operators/count-str
    replace squery-spark.datasets.operators/replace
    split-str squery-spark.datasets.operators/split-str
    substring? squery-spark.datasets.operators/substring?
    capitalize squery-spark.datasets.operators/capitalize
    lower-case squery-spark.datasets.operators/lower-case
    upper-case squery-spark.datasets.operators/upper-case
    trim squery-spark.datasets.operators/trim
    triml squery-spark.datasets.operators/triml
    trimr squery-spark.datasets.operators/trimr
    padl squery-spark.datasets.operators/padl
    padr squery-spark.datasets.operators/padr
    translate squery-spark.datasets.operators/translate

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
    dense-rank squery-spark.datasets.operators/dense-rank
    row-number squery-spark.datasets.operators/row-number
    wgroup squery-spark.datasets.operators/wgroup
    wsort squery-spark.datasets.operators/wsort
    wrange squery-spark.datasets.operators/wrange
    wrows squery-spark.datasets.operators/wrows
    window squery-spark.datasets.operators/window
    first-a squery-spark.datasets.operators/first-a
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
    bround squery-spark.datasets.operators/bround
    trunc squery-spark.datasets.operators/trunc
    sqrt squery-spark.datasets.operators/sqrt
    mod squery-spark.datasets.operators/mod
    div squery-spark.datasets.operators/div
    lit squery-spark.datasets.operators/lit
    cast squery-spark.datasets.operators/cast
    string squery-spark.datasets.operators/string
    long squery-spark.datasets.operators/long
    long-array squery-spark.datasets.operators/long-array
    string-array squery-spark.datasets.operators/string-array
    asc squery-spark.datasets.operators/asc
    desc squery-spark.datasets.operators/desc
    todf squery-spark.datasets.operators/todf
    soundex squery-spark.datasets.operators/soundex

    ;;statistics
    corr squery-spark.datasets.operators/corr

    ;;stages
    sort squery-spark.datasets.stages/sort
    group squery-spark.datasets.stages/group
    agg squery-spark.datasets.stages/agg
    unset squery-spark.datasets.stages/unset
    count-s squery-spark.datasets.stages/count-s
    limit squery-spark.datasets.stages/limit
    join squery-spark.datasets.stages/join
    union-with squery-spark.datasets.stages/union-with
    union-by-name squery-spark.datasets.stages/union-by-name
    union-all-with squery-spark.datasets.stages/union-all-with
    as squery-spark.datasets.stages/as
    intersection-with squery-spark.datasets.stages/intersection-with
    difference-with squery-spark.datasets.stages/difference-with
    difference-all-with squery-spark.datasets.stages/difference-all-with
    pivot squery-spark.datasets.stages/pivot
    describe squery-spark.datasets.stages/describe
    stat squery-spark.datasets.stages/stat
    approx-quantile squery-spark.datasets.stages/approx-quantile
    stat  squery-spark.datasets.stages/stat
    na    squery-spark.datasets.stages/na
    drop-na squery-spark.datasets.stages/drop-na
    fill-na squery-spark.datasets.stages/fill-na
    replace-na squery-spark.datasets.stages/replace-na

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