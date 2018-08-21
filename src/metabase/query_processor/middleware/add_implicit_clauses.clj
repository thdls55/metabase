(ns metabase.query-processor.middleware.add-implicit-clauses
  "Middlware for adding an implicit `:fields` and `:order-by` clauses to certain queries."
  (:require [clojure.tools.logging :as log]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [mbql :as mbql]
             [util :as qputil]]
            [puppetlabs.i18n.core :refer [tru]]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.query_processor.mbql.SourceTable))

(s/defn ^:private fetch-field-ids-for-souce-table-id :- [(s/protocol mbql/Field)]
  [source-table :- SourceTable]
  (map :id (db/select [Field :id]
                      :table_id        (:id source-table)
                      :active          true
                      :visibility_type [:not-in ["sensitive" "retired"]]
                      :parent_id       nil
                      {:order-by [[:position :asc]
                                  [:id :desc]]})))

(defn- should-add-implicit-fields? [{:keys [fields breakout source-table], aggregations :aggregation}]
  (and source-table ; if query is using another query as its source then there will be no table to add nested fields for
       (not (or (seq aggregations)
                (seq breakout)
                (seq fields)))))

(defn- add-implicit-fields [{:keys [source-table], :as inner-query}]
  (if-not (should-add-implicit-fields? inner-query)
    inner-query
    ;; this is a structured `:rows` query, so lets add a `:fields` clause with all fields from the source table +
    ;; expressions
    (let [inner-query (assoc inner-query :fields-is-implicit true)
          fields      (fetch-field-ids-for-souce-table-id source-table)
          expressions (for [[expression-name] (:expressions inner-query)]
                        (mbql/expression (name expression-name)))]
      (when-not (seq fields)
        (log/warn (tru "Table ''{0}'' has no Fields associated with it." (name source-table))))
      (assoc inner-query
        :fields (concat fields expressions)))))



(defn- add-implicit-breakout-order-by
  "`Fields` specified in `breakout` should add an implicit ascending `order-by` subclause *unless* that field is
  *explicitly* referenced in `order-by`."
  [{breakout-fields :breakout, order-by :order-by, :as inner-query}]
  (let [order-by-fields                   (set (map (comp #(select-keys % [:field-id :fk-field-id]) :field) order-by))
        implicit-breakout-order-by-fields (remove (comp order-by-fields #(select-keys % [:field-id :fk-field-id]))
                                                  breakout-fields)]
    (cond-> inner-query
      (seq implicit-breakout-order-by-fields) (update :order-by concat (for [field implicit-breakout-order-by-fields]
                                                                         {:field field, :direction :ascending})))))

(defn- add-implicit-clauses-to-inner-query [inner-query]
  (cond-> (add-implicit-fields (add-implicit-breakout-order-by inner-query))
    ;; if query has a source query recursively add implicit clauses to that too as needed
    (:source-query inner-query) (update :source-query add-implicit-clauses-to-inner-query)))

(defn- maybe-add-implicit-clauses [query]
  (if-not (qputil/mbql-query? query)
    query
    (update query :query add-implicit-clauses-to-inner-query)))

(defn add-implicit-clauses
  "Add an implicit `fields` clause to queries with no `:aggregation`, `breakout`, or explicit `:fields` clauses.
   Add implicit `:order-by` clauses for fields specified in a `:breakout`."
  [qp]
  (comp qp maybe-add-implicit-clauses))
