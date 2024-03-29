;
; Copyright 2022 Netflix, Inc.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;     http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;
(ns io.mantisrx.mql.jvm.interfaces.core
  (:require
    [io.mantisrx.mql.core :as mql]
    [io.mantisrx.mql.parser :as parser]
    [io.mantisrx.mql.util :as util]
    [io.mantisrx.mql.jvm.compilers.core :as compiler]
    [io.mantisrx.mql.compilers.core.operands :as operands]
    [io.mantisrx.mql.compilers.core.select :as select]
    [rx.lang.clojure.core :as rx]
    [rx.lang.clojure.interop :as rxi]
    [instaparse.core :as insta])
  (:import rx.Observable)
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

(defn is-aggregate
  [query]
  (let
    [parsed (parser/parser query)]
    (<= 1 (count (util/extract-clause :AGG_QUERY parsed)))))

(defn query->groupby
  "Extracts a function of datum -> key for the group by key."
  [query]
  (comp
    doall
    (->> (mql/parser query)
         (util/extract-clause :GROUP)
         (insta/transform compiler/mql-evaluators)
         first
         :groupby)))

(defn query->orderby
  "Extracts a function of Observable -> Observable in which the result
   is sorted."
  [query]
  (let [ordering (->>
                    (mql/parser query)
                    (util/extract-clause :ORDER)
                    (insta/transform compiler/mql-evaluators)
                    first)
         order-comparator (:order-comparator ordering)
         prop (:orderby ordering)
         fun (rxi/fn [x y] (int (order-comparator (prop x) (prop y))))]
    (fn [^Observable coll]
      (rx/mapcat rx/seq->o (.toSortedList coll fun)))))

(defn query->limit
  "Extracts the limit from a query."
  [query]
  (->>
    (mql/parser query)
    (util/extract-clause :LIMIT)
    (insta/transform compiler/mql-evaluators)
    first
    :limit))

(defn query->window
  "Extracts the window seconds from the query."
  [query]
  (->> (mql/parser query)
       (util/extract-clause :WINDOW)
       (insta/transform compiler/mql-evaluators)
       first
       :window))

(defn query->having-pred
  "Computes a predicate which can be used to determine if a specific datum
   satisfies the HAVING clause of this query.

   mql-query: A string representing the query.

   Returns: A predicate function of datum -> boolean."
  [mql-query]
  (let
    [parsed (mql/parser mql-query)
     extracted (->> mql-query
                    mql/parser
                 (util/extract-clause :HAVING)
                 first
                 second)]
    (if (nil? extracted) (fn [_] true)
      (insta/transform compiler/mql-evaluators extracted))))

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

(defn query->extrapolator
  [mql-query]
  (let
    [evaluators (merge compiler/mql-evaluators {:SAMPLE io.mantisrx.mql.compilers.core.sampling/sample-config->extrapolation-fn})
     clause
     ( ->> mql-query
         mql/parser
         (util/extract-clause :SAMPLE)
         first)]
    (if (nil? clause)
      identity
      (insta/transform evaluators clause))))
