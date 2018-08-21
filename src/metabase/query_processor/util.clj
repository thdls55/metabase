(ns metabase.query-processor.util
  "Utility functions used by the global query processor and middleware functions."
  (:require [buddy.core
             [codecs :as codecs]
             [hash :as hash]]
            [cheshire.core :as json]
            [clojure
             [string :as str]
             [walk :as walk]]
            [metabase.util :as u]
            [metabase.util.schema :as su]
            [schema.core :as s]))

(defn datetime-field?
  "Is FIELD a `DateTime` field?"
  [{:keys [base-type special-type]}]
  (or (isa? base-type :type/DateTime)
      (isa? special-type :type/DateTime)))

(defn query->remark
  "Generate an approparite REMARK to be prepended to a query to give DBAs additional information about the query being
  executed. See documentation for `mbql->native` and [issue #2386](https://github.com/metabase/metabase/issues/2386)
  for more information."  ^String [{{:keys [executed-by query-hash query-type], :as info} :info}]
  (str "Metabase" (when info
                    (assert (instance? (Class/forName "[B") query-hash))
                    (format ":: userID: %s queryType: %s queryHash: %s"
                            executed-by query-type (codecs/bytes->hex query-hash)))))


(s/defn normalize-token :- s/Keyword
  "Convert a string or keyword in various cases (`lisp-case`, `snake_case`, or `SCREAMING_SNAKE_CASE`) to a lisp-cased
  keyword."
  [token :- su/KeywordOrString]
  (-> (name token)
      str/lower-case
      (str/replace #"_" "-")
      keyword))

;;; ---------------------------------------------------- Hashing -----------------------------------------------------

(defn- select-keys-for-hashing
  "Return QUERY with only the keys relevant to hashing kept.
  (This is done so irrelevant info or options that don't affect query results doesn't result in the same query
  producing different hashes.)"
  [query]
  {:pre [(map? query)]}
  (let [{:keys [constraints parameters], :as query} (select-keys query [:database :type :query :native :parameters
                                                                        :constraints])]
    (cond-> query
      (empty? constraints) (dissoc :constraints)
      (empty? parameters)  (dissoc :parameters))))

(defn query-hash
  "Return a 256-bit SHA3 hash of QUERY as a key for the cache. (This is returned as a byte array.)"
  [query]
  (hash/sha3-256 (json/generate-string (select-keys-for-hashing query))))


;;; --------------------------------------------- Query Source Card IDs ----------------------------------------------

(defn query->source-card-id
  "Return the ID of the Card used as the \"source\" query of this query, if applicable; otherwise return `nil`."
  ^Integer [outer-query]
  (let [source-table (get-in outer-query [:query :source-table])]
    (when (string? source-table)
      (when-let [[_ card-id-str] (re-matches #"^card__(\d+$)" source-table)]
        (Integer/parseInt card-id-str)))))

;;; ---------------------------------------- General Tree Manipulation Helpers ---------------------------------------

(defn postwalk-pred
  "Transform `form` by applying `f` to each node where `pred` returns true"
  [pred f form]
  (walk/postwalk (fn [node]
                   (if (pred node)
                     (f node)
                     node))
                 form))

(defn postwalk-collect
  "Invoke `collect-fn` on each node satisfying `pred`. If `collect-fn` returns a value, accumulate that and return the
  results.

  Note: This would be much better as a zipper. It could have the same API, would be faster and would avoid side
  affects."
  [pred collect-fn form]
  (let [results (atom [])]
    (postwalk-pred pred
                   (fn [node]
                     (when-let [result (collect-fn node)]
                       (swap! results conj result))
                     node)
                   form)
    @results))
