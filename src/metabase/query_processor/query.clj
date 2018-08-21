(ns metabase.query-processor.query
  (:require [metabase.query-processor.mbql :as mbql]
            [schema.core :as s]
            [medley.core :as m]
            [metabase.query-processor.util :as qputil]
            [metabase.util.schema :as su])
  (:import metabase.query_processor.mbql.MBQLQuery))

(s/defrecord NativeQuery [query :- s/Any])

(s/defn native-query :- NativeQuery [m]
  (map->NativeQuery (m/map-keys qputil/normalize-token m)))


(s/defrecord Query [database :- su/IntGreaterThanZero
                    query    :- (s/cond-pre
                                 MBQLQuery
                                 NativeQuery)])


(s/defn parse-query :- Query
  [query]
  (let [query (m/map-keys qputil/normalize-token query)]
    (map->Query
     {:database (:database query)
      :query    (case (qputil/normalize-token (:type query))
                  :query  (mbql/query   (:query query))
                  :native (native-query (:native query)))})))
