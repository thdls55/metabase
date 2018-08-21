(ns metabase.query-processor.mbql
  (:refer-clojure :exclude [< <= > >= = != and or not filter count distinct sum min max + - / *])
  (:require [clojure
             [core :as core]
             [set :as set]
             [pprint :as pprint]]
            [medley.core :as m]
            [metabase.query-processor.util :as qputil]
            [metabase.sync.interface :as sync]
            [metabase.util.schema :as su]
            [puppetlabs.i18n.core :refer [tru]]
            [schema.core :as s]
            [toucan.db :as db]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                       MBQL Protocol & Print Method Impls                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

(defprotocol MBQL
  "Classes that implement MBQL are any sort of MBQL clause."
  (describe [this]
    "Return the canonical form for this MBQL clause, for REPL usage and debug usage.")
  (required-features [this]
    "Return a set of driver features that use of this clause requires."))

(extend-protocol MBQL
  nil
  (describe [_] nil)
  (required-features [_] nil)

  java.lang.Number
  (describe [this] this)
  (required-features [_] nil)

  java.lang.String
  (describe [this] this)
  (required-features [_] nil)

  java.lang.Boolean
  (describe [this] this)
  (required-features [_] nil)

  java.sql.Time
  (describe [this] this)
  (required-features [_] nil)

  java.sql.Timestamp
  (describe [this] this)
  (required-features [_] nil))

;; MBQL clauses print as their normalizations
(do (clojure.core/defmethod print-method metabase.query_processor.mbql.MBQL [s writer]
      (print-method (describe s) writer))
    (clojure.core/defmethod pprint/simple-dispatch metabase.query_processor.mbql.MBQL [s]
      (pprint/write-out (describe s)))
    (doseq [m [print-method pprint/simple-dispatch]]
      (prefer-method m metabase.query_processor.mbql.MBQL clojure.lang.IRecord)
      (prefer-method m metabase.query_processor.mbql.MBQL java.util.Map)
      (prefer-method m metabase.query_processor.mbql.MBQL clojure.lang.IPersistentMap)))


;;; ---------------------------------------------- MBQL Clause Options -----------------------------------------------

(defprotocol ClauseOptions
  (options [this]
    "Return an map of additional options and optionsrmation for this MBQL clause.")
  (add-options [this options]
    "Add additional options to this object."))

#_(defn- ClauseOptionsDefaults [map->options-fn]
  {:options     :options
   :add-options (fn [this new-options]
                  (assoc this :options (map->options-fn (merge (:options this)
                                                               new-options))))})

(defn- describe-options [options]
  (when (seq options)
    (m/filter-vals identity (into {} options))))

(defn- append-options-description-if-non-empty [obj options]
  (let [options (describe-options options)]
    (if (seq options)
      (conj (vec obj) options)
      obj)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                Unit Types Etc.                                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(def datetime-field-units
  "Valid units for a `DateTimeField`."
  #{:default :minute :minute-of-hour :hour :hour-of-day :day :day-of-week :day-of-month :day-of-year
    :week :week-of-year :month :month-of-year :quarter :quarter-of-year :year})

(def relative-datetime-value-units
  "Valid units for a `RelativeDateTimeValue`."
  #{:minute :hour :day :week :month :quarter :year})

(def DatetimeFieldUnit
  "Schema for datetime units that are valid for `DateTimeField` forms."
  (s/named (apply s/enum datetime-field-units) "Valid datetime unit for a field"))

(def DatetimeValueUnit
  "Schema for datetime units that valid for relative datetime values."
  (s/named (apply s/enum relative-datetime-value-units) "Valid datetime unit for a relative datetime"))

(def binning-strategies
  "Valid binning strategies for a `BinnedField`"
  #{:num-bins :bin-width :default})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    Parsing                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn token->mbql-fn [token]
  (let [token (qputil/normalize-token token)
        f     (core/or (ns-resolve 'metabase.query-processor.mbql (symbol (name token)))
                       (throw (Exception. (str (tru "Invalid MBQL fn: {0}" token)))))]
    (when (:mbql (meta f))
      @f)))

(s/defn ^:private parse-mbql-clause [[f & args]]
  (apply (token->mbql-fn f) (for [arg args]
                              (if (sequential? arg)
                                (parse-mbql-clause arg)
                                arg))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Fields                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

;; All 'Fields' should implement MBQL, ClauseOptions, Field, and CoerceToField

(defprotocol Field
  "Classes that implement `Field` represent objects that represent Fields, such as a Field ID or literal."
  (field [this]
    "Return the underlying `metabase.models.field.FieldInstance` object this refers to, if applicable."))

(defprotocol CoerceToField
  "Protocol for coercing non-MBQL things into Fields."
  (->field [this] [this options]
    "Coerce this to an object that implements Field, or throw an Exception."))

;; optional extra options about a Field.
(s/defrecord FieldOptions [fk-field            :- (s/maybe (s/protocol Field))
                           datetime-unit       :- (s/maybe DatetimeFieldUnit)
                           remapped-from       :- (s/maybe s/Str)
                           remapped-to         :- (s/maybe s/Str)
                           field-display-name  :- (s/maybe s/Str)
                           binning-strategy    :- (s/maybe (apply s/enum binning-strategies))
                           binning-param       :- (s/maybe s/Num)
                           fingerprint         :- (s/maybe sync/Fingerprint)]
  nil
  :load-ns true
  MBQL
  (describe [this] (describe-options this))
  (required-features [_] nil))

(s/defn ^:private add-field-options :- {options FieldOptions, s/Any s/Any}
  [obj field-options]
  (assoc obj :options (map->FieldOptions (merge (:options obj) field-options))))


;;; ---------------------------------------------------- Field ID ----------------------------------------------------

(s/defrecord FieldID [id      :- su/IntGreaterThanZero
                      options :- (s/maybe FieldOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:field-id id] options))
  (required-features [_] nil)
  ClauseOptions
  (options [_] options)
  (add-options [this new-options] (add-field-options this new-options))
  CoerceToField
  (->field [this] this)
  Field
  (field [_]
    (db/select-one 'Field :id id)))

(s/defn ^:mbql field-id :- FieldID
  ([id]
   (field-id id nil))
  ([id options]
   (map->FieldID {:id id, :options (when options (map->FieldOptions options))})))

(s/defn ^:mbql fk-> :- FieldID
  "Reference a Field that belongs to another Table. `dest-field-id` is the ID of the Field in the other Table;
  `fk-field` is the Field in the query's source Table that should be used to perform the join.

  `fk->` is so named because you can think of it as \"going through\" the FK Field to get to the dest Field:

    (fk-> 100 200) ; refer to Field 200, which is part of another Table; join to the other table via our foreign key
                   ; 100."
  [fk-field dest-field-id]
  (add-options (field-id dest-field-id)
               {:fk-field (->field fk-field)}))


;;; ------------------------------------------- Coercing Objects to Field --------------------------------------------

(extend-protocol CoerceToField
  java.lang.Integer
  (->field [this] (field-id this))
  java.lang.Long
  (->field [this] (field-id this))
  clojure.lang.Sequential
  (->field [this] (->field (parse-mbql-clause this))))


;;; ------------------------------------------------- Expression Ref -------------------------------------------------

(s/defrecord ExpressionRef [expression-name :- su/NonBlankString
                            options         :- (s/maybe FieldOptions)]
  nil
  :load-ns true
  MBQL
  (describe [_] (append-options-description-if-non-empty [:expression expression-name] options))
  (required-features [_] nil)
  ClauseOptions
  (options [_] options)
  (add-options [this new-options] (add-field-options this new-options))
  CoerceToField
  (->field [this] this)
  ;; expression ref does not reference an actual Field in the DB
  Field
  (field [_] nil))

(s/defn ^:mbql expression :- ExpressionRef
  [expression-name]
  (map->ExpressionRef {:expression-name (name expression-name)}))


;;; ------------------------------------------------- Field Literal --------------------------------------------------

(s/defrecord FieldLiteral [field-name :- su/NonBlankString
                           options   :- (s/maybe FieldOptions)]
  nil
  :load-ns true
  MBQL
  (describe [_] (append-options-description-if-non-empty [:field-literal field-name] options))
  (required-features [_] nil)
  ClauseOptions
  (options [_] options)
  (add-options [this new-options] (add-field-options this new-options))
  CoerceToField
  (->field [this] this)
  ;; TODO - what should we do *here*?
  Field
  (field [_] nil))

(s/defn ^:mbql field-literal :- FieldLiteral
  [field-name]
  (map->FieldLiteral {:field-name (name field-name)}))


;;; ------------------------------------------------- Field Methods --------------------------------------------------

(s/defn has-datetime-unit?
  [field :- (s/protocol Field)]
  (:datetime-unit (options field)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Aggregations                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

;; all aggregations should implement MBQL, ClauseOptions, Aggregation

(defprotocol Aggregation
  "Classes that implement `Aggregation` represent objects that can be used in the MBQL `:aggregation` clause.")

;; extra options about aggregations.
(s/defrecord AggregationOptions [custom-name :- (s/maybe su/NonBlankString)]
  nil
  :load-ns true
  MBQL
  (describe [this] (when (seq this)
                     (m/filter-vals identity (into {} this))))
  (required-features [_] nil))

(s/defn ^:private add-aggregation-options :- {options AggregationOptions, s/Any s/Any}
  [obj aggregation-options]
  (assoc obj :options (map->AggregationOptions (merge (:options obj) aggregation-options))))


;;; --------------------------------------------- Aggregation Subclauses ---------------------------------------------

(s/defrecord CumulativeCountAggregation [options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:cum-count] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql cum-count :- CumulativeCountAggregation
  ([]
   (map->CumulativeCountAggregation {})))


(s/defrecord CountAggregation [field   :- (s/maybe (s/protocol Field))
                               options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (filterv identity [:count
                                      (when field (describe field))
                                      (describe-options options)]))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql count :- CountAggregation
  ([]
   (map->CountAggregation {}))
  ([field]
   (map->CountAggregation {:field (->field field)})))



(s/defrecord AverageAggregation [field   :- (s/maybe (s/protocol Field))
                                 options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:avg (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql avg :- AverageAggregation
  [field]
  (map->AverageAggregation {:field (->field field)}))


(s/defrecord CumulativeSumAggregation [field   :- (s/maybe (s/protocol Field))
                                       options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:cum-sum (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql cum-sum :- CumulativeSumAggregation
  [field]
  (map->CumulativeSumAggregation {:field (->field field)}))


(s/defrecord DistinctCountAggregation [field   :- (s/maybe (s/protocol Field))
                                       options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:distinct (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql distinct :- DistinctCountAggregation
  [field]
  (map->DistinctCountAggregation {:field (->field field)}))


(s/defrecord MaxAggregation [field   :- (s/maybe (s/protocol Field))
                             options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:max (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql max :- MaxAggregation
  [field]
  (map->MaxAggregation {:field (->field field)}))


(s/defrecord MinAggregation [field   :- (s/maybe (s/protocol Field))
                             options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:min (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql min :- MinAggregation
  [field]
  (map->MinAggregation {:field (->field field)}))


(s/defrecord StandardDeviationAggregation [field   :- (s/maybe (s/protocol Field))
                                           options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:stddev (describe field)] options))
  (required-features [_] #{:standard-deviation-aggregations})
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql stddev :- StandardDeviationAggregation
  [field]
  (map->StandardDeviationAggregation {:field (->field field)}))


(s/defrecord SumAggregation [field   :- (s/maybe (s/protocol Field))
                             options :- (s/maybe AggregationOptions)]
  nil
  :load-ns true
  MBQL
  (describe [this] (append-options-description-if-non-empty [:sum (describe field)] options))
  (required-features [_] nil)
  ClauseOptions
  (options [this] options)
  (add-options [this new-options] (add-aggregation-options this new-options))
  Aggregation)

(s/defn ^:mbql sum :- SumAggregation
  [field]
  (map->SumAggregation {:field (->field field)}))


;;; --------------------------------------------- Aggregation Top-Level ----------------------------------------------

(s/defrecord Aggregations [aggregations :- [(s/protocol Aggregation)]]
  nil
  :load-ns true
  MBQL
  (describe [this] {:aggregation (map describe aggregations)})
  (required-features [_] (reduce set/union (map required-features aggregations))))

(s/defn ^:mbql aggregation :- Aggregations
  [ag-or-ags]
  (map->Aggregations
   {:aggregations
    (if (sequential? (first ag-or-ags)) ; multiple aggregations
      (map parse-mbql-clause ag-or-ags)
      [(parse-mbql-clause ag-or-ags)])}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    Breakout                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+

(s/defrecord Breakout [fields :- [(s/protocol Field)]]
  nil
  :load-ns true
  MBQL
  (describe [_]
    {:breakout (map describe fields)}))

(s/defn ^:mbql breakout :- (s/maybe Breakout)
  [& fields]
  (when (seq fields)
    (map->Breakout {:fields (map ->field fields)})))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Filter                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Filter subclauses should implement MBQL, Filter

(defprotocol Filter)


;;; ---------------------------------------------------- Compound ----------------------------------------------------

(s/defrecord AndFilter [subclauses :- [(s/protocol Filter)]]
  nil
  :load-ns true
  MBQL
  (describe [this] (vec (cons :and (map describe subclauses))))
  (required-features [_] nil)
  Filter)

(s/defn and :- (s/protocol Filter)
  [& subclauses]
  (if (core/= (core/count subclauses) 1)
    (first subclauses)
    (map->AndFilter {:subclauses (map parse-mbql-clause subclauses)})))


(s/defrecord OrFilter [subclauses :- [(s/protocol Filter)]]
  nil
  :load-ns true
  MBQL
  (describe [this] (vec (cons :or (map describe subclauses))))
  (required-features [_] nil)
  Filter)

(s/defn or :- (s/protocol Filter)
  [& subclauses]
  (if (core/= (core/count subclauses) 1)
    (first subclauses)
    (map->OrFilter {:subclauses (map parse-mbql-clause subclauses)})))


(s/defrecord NotFilter [subclause :- (s/protocol Filter)]
  nil
  :load-ns true
  MBQL
  (describe [this] [:not (describe subclause)])
  (required-features [_] nil)
  Filter)

(s/defn not :- (s/protocol Filter)
  [subclause]
  (if (instance? NotFilter subclause)
    (:subclause subclause)
    (map->NotFilter {:subclause subclause})))



;;; ---------------------------------------------------- Equality ----------------------------------------------------

(defprotocol ^:private EqualityComparable
  "Classes implementing `EqualityComparable` are comparible in one of the different quality MB equality filters.")

(extend-protocol EqualityComparable
  nil
  java.lang.Number
  java.lang.String
  java.lang.Boolean
  java.sql.Time
  java.sql.Timestamp)

(def ^:private EqualityComparableValue
  (s/cond-pre
   (s/protocol Field)
   (s/protocol EqualityComparable)))


(s/defrecord EqualsFilter [x :- EqualityComparableValue
                           y :- EqualityComparableValue]
  nil
  :load-ns true
  MBQL
  (describe [this] [:= (describe x) (describe y)])
  (required-features [_] nil)
  Filter)

(s/defn = :- (s/protocol Filter)
  "Filter subclause. With a single value, return results where `x = y`. With two or more values, return results where
  `x` matches any of the other values (the equivalent of SQL `IN`):

   NOTE: For legacy compatibility, `x` is assumed to be a Field, and integers in that position will be treated as
   such.

     (= f v)
     (= f v1 v2) ; same as (or (= f v1) (= f v2))"
  ([x y]
   (map->EqualsFilter {:x (if (integer? x)
                            (->field x)
                            x)
                       :y y}))
  ([x y & more]
   (apply or (for [y (cons y more)]
               (= x y)))))



;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Limit                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+

(s/defrecord Limit [limit :- su/IntGreaterThanZero]
  MBQL
  (describe [_]
    {:limit limit}))

(s/defn ^:mbql limit :- Limit
  [limit]
  (map->Limit {:limit limit}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                               Table/Source Table                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defprotocol Table
  "Classes that implement `Table` represent a Table in some way, perhaps by ID or by literal name."
  (table [this]
    "Return the `metabase.models.table.TableInstance` object this refers to, if applicable."))

(s/defrecord SourceTable [id :- su/IntGreaterThanZero]
  clojure.lang.Named
  (getName [this]
    (:name (table this)))
  MBQL
  (describe [_] {:source-table id})
  Table
  (table [_]
    (db/select-one 'Table :id id)))

(s/defn ^:mbql source-table :- SourceTable
  [id]
  (map->SourceTable {:id id}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Expressions                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(s/defrecord Expression []) ; TODO


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   MBQLQuery                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+

(defprotocol ^:private MBQLQueryMethods
  (has-aggregations? [this])
  (has-limit? [this]))

#_{{aggregations :aggregation, :keys [limit page]} :query}

(s/defrecord MBQLQuery [source-table :- SourceTable
                        aggregation  :- (s/maybe Aggregations)
                        breakout     :- (s/maybe Breakout)
                        limit        :- (s/maybe Limit)]
  MBQL
  (describe [this]
    this
    (into {} (for [[_ v] this
                   :when v]
               (describe v))))
  MBQLQueryMethods
  (has-aggregations? [this]
    ;; TODO
    false)
  (has-limit? [this]
    (boolean limit)
    ))

(s/defn ^:private parse-top-level :- (s/maybe (s/protocol MBQL))
  [k args]
  (let [args (if (sequential? args)
               args
               [args])]
    (apply (token->mbql-fn k) args)))

(s/defn query :- MBQLQuery
  [m :- su/Map]
  (map->MBQLQuery
   (into {} (for [[k v] m
                  :let [k (normalize-token k)
                        clause (parse-top-level k v)]
                  :when clause]
              [k clause]))))
