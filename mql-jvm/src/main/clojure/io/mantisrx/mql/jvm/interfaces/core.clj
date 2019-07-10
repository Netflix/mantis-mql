(ns io.mantisrx.mql.jvm.interfaces.core
  (:require
    [io.mantisrx.mql.core :as mql]
    [io.mantisrx.mql.parser :as parser]
    [io.mantisrx.mql.util :as util]
    [io.mantisrx.mql.jvm.compilers.core :as compiler]
    [io.mantisrx.mql.compilers.core.operands :as operands]
    [io.mantisrx.mql.compilers.core.select :as select]
    [instaparse.core :as insta])
  (:gen-class)
  )

(defn query->pred
  "Computes a predicate which can be used to determine if a specific datum
   satisfies the WHERE clause of this query.

   mql-query: A string representing the query.

   Returns: A predicate function of datum -> boolean."
  [mql-query]
  (let
    [parsed (mql/parser mql-query)
     where-false (->> parsed
                      (util/extract-clause :WHERE_FALSE)
                      first
                      second)
     extracted (->> mql-query
                    mql/parser
                    (util/extract-clause :WHERE)
                    first
                    second)]
    (cond
      (not (nil? where-false)) (fn [_] false)
      (not (nil? extracted)) (insta/transform compiler/mql-evaluators extracted)
      :else (fn [_] true))))

(defn- parse-tree->properties
  "Extracts all properties used within a parse tree. This is useful for SOURCE mode
   in which we want to ensure all necessary data is transmitted but do not actually
   want to process parts of the query.

   tree: A parse tree as returned by the parser.

   returns: A seq of the subtrees representing the properties."
  [tree]
  (concat (util/extract-clause :property tree)
          (util/extract-clause :sw_property tree)
          (util/extract-clause :star_property tree)))


(defn queries->superset-projection
  "Computes a projection function which accepts a datum and returns a map
   projecting the necessary properties for all of the queries.
   See: query->projection.

   queries: A seq of strings representing the queries.

   Returns: A function of datum -> map."
  [queries]
  (let
    [parsed (map mql/parser queries)
     stars? (< 0 (count (apply concat (map (partial util/extract-clause :asterisk) parsed))))
     extractors (map #(insta/transform (merge compiler/mql-evaluators {:star_property operands/star-property->projection}) %) (distinct (apply concat (map parse-tree->properties parsed))))]
    (if stars?
      identity
      (apply select/select-list->fn extractors))))

(defn query->projection
  "Computes a projection function which accepts a datum and returns a map
   projecting all properties necessary to run the query against this datum.
   This reduces the amount of bytes sent over the wire while providing the
   guarantee that all necessary data will remain.

   mql-query: A string representing the query.

   Returns: A function of datum -> map."
  [mql-query]
  (queries->superset-projection [mql-query]))

(defn agg-query->projection
  "Computes a function of Observable -> Observable implementing the rollup
   function for an aggregate query."
  [query]
  (->>
    (mql/parser query)
    (util/extract-clause :AGG_LIST)
    (insta/transform compiler/mql-evaluators)
    first))

(defn query->sampler
  "Computes a sampling function which accepts a datum and returns a boolean
   indicating wether or not the datum should be sampled.

   mql-query: A string representing the mql query.

   Returns: A function of datum -> boolean."
  [mql-query]
  (let
    [parsed (mql/parser mql-query)
     extracted (first (util/extract-clause :SAMPLE parsed))]
    (if (nil? extracted)
      (fn [_] true)
      (:sample (insta/transform compiler/mql-evaluators extracted)))))

(defn query->subjects
  ^java.util.List
  [mql-query]
  (let
    [parsed (mql/parser mql-query)
     extracted (map rest (util/extract-clause :table_list parsed))]
    (if (empty? extracted)
      (list "stream")
      (flatten (insta/transform compiler/mql-evaluators extracted)))))

(defn parses?
  [query]
  (parser/parses? query))

(defn get-parse-error
  [query]
  (parser/query->parse-error query))
