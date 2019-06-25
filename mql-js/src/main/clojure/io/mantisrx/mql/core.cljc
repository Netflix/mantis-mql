(ns io.mantisrx.mql.core
  (:require [instaparse.core :as insta]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [rx.lang.clojure.core :as rx])
            #?(:clj [rx.lang.clojure.interop :as rxi])
            [clojure.set :as s]
            [io.mantisrx.mql.transformers :as t]
            [io.mantisrx.mql.util :as util]
            [io.mantisrx.mql.parser :as mqlp]
            [io.mantisrx.mql.optimization :as opt]
            [io.mantisrx.mql.compilers.anomaly :as anom]
            [instaparse.print :as iprint]
            #?(:cljs [cljs.nodejs :as nodejs])
            #?(:cljs [cljs.reader :as rdr]))
  #?(:clj
     (:import rx.Observable java.util.Map java.util.concurrent.TimeUnit))
  #?(:clj (:gen-class
            :name io.mantisrx.mql.Core
            :methods [
                      #^{:static true} [evalMql [String java.util.Map] rx.Observable]
                      #^{:static true} [evalMql [String java.util.Map String] rx.Observable]
                      #^{:static true} [evalMql [String java.util.Map String Boolean] rx.Observable]
                      #^{:static true} [isAggregate [String] Boolean]])))

#?(:cljs (defn read-string [x] (rdr/read-string x)))
#?(:clj (def ^:export parser (comp opt/run clojure.zip/vector-zip mqlp/parser clojure.string/trim)))
#?(:cljs (def ^:export parser (comp mqlp/parser clojure.string/trim)))

(def mql-evaluators t/mql-evaluators)

#?(:clj
   (definterface
     Query
     (^String getSubscriptionId [])
     (^String getRawQuery [])
     (^java.util.List getSubjects [])
     (^Boolean matches [^java.util.Map datum])
     (^Boolean sample [^java.util.Map datum])
     (^java.util.Map project [^java.util.Map this])))

(defn reason->str
  "Provides special case for printing negative lookahead reasons"
  [r]
  (cond
    (:NOT r)
    (do (print "NOT ")
        (print (:NOT r))),
    (:char-range r)
    (iprint/char-range->str r)
    (instance? #?(:clj java.util.regex.Pattern
                  :cljs js/RegExp)
               r)
    (iprint/regexp->str r)
    :else
    r))

(defn failure->reason-str
  [failure]
  (let
    [reasons (map reason->str (distinct (map :expecting (filter (complement :full) (:reason failure)))))]
    (str "Parse error at line: "
         (:line failure) " column: " (:column failure)
         ". Expected one of " (clojure.string/join ", " reasons))))

(def modes
  {"client" {:SAMPLE (fn [_] {:sample (fn [_] true)})
             :number (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as read-string)
             :integer (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as (comp int read-string))
             :string_literal (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as util/strip-surround)
             :boolean_literal (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as read-string)
             :property_with_as io.mantisrx.mql.compilers.core.operands/property-with-as->fn}
   "full" {}
   })

#?(:clj (defn eval-mql
          "Evaluates an MQL query against a specific context.

           mql-string: A valid MQL(SQL) string.
           context: A map of string -> Observable<String> map representing the observables
           available to query.
           mode: A string representing the operating mode. Either client or full
           threading: A boolean indicating wether or not to use threading features.

           Returns: An Observable<Map<String, Object>> of the transformation."
          ([mql-string context] (eval-mql mql-string context "full"))
          ([mql-string context mode] (eval-mql mql-string context mode false))
          ([mql-string context mode threading]
           (binding
             [t/*threading-enabled* threading]
             (let [context-map (assoc (into {} context) "key" "value")
                   transformers (assoc mql-evaluators :table_list (partial t/table-list->observable context-map))
                   transformers (assoc transformers :TABLE_NO_FROM (fn [& args] [(get context-map "stream" (rx/empty)) (apply merge args)]))
                   transformers (merge transformers (get modes mode {}))
                   parsed (parser mql-string)]
               (insta/transform transformers parsed))))))
(defn parses?
  [query]
  (->> query
       parser
       insta/failure?
       not))

(defn get-parse-error
  [query]
  (->> query
       parser
       insta/get-failure
       failure->reason-str))

#?(:clj (defn -evalMql
          "A Java-callable wrapper around the eval-mql function."
          ([query context]
           (eval-mql query context "full"))
          ([query context mode]
           (eval-mql query context mode))
          ([query context mode threading]
           (eval-mql query context mode threading))))

(defn is-aggregate
  [query]
  (let
    [parsed (parser query)]
    (<= 1 (count (util/extract-clause :AGG_QUERY parsed)))))

(defn -isAggregate
  [query]
  (is-aggregate query))

;;;;
;;;; Query Transformations
;;;;

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

(defn query->pred
  "Computes a predicate which can be used to determine if a specific datum
   satisfies the WHERE clause of this query.

   mql-query: A string representing the query.

   Returns: A predicate function of datum -> boolean."
  [mql-query]
  (let
    [parsed (parser mql-query)
     where-false (->> parsed
                      (util/extract-clause :WHERE_FALSE)
                      first
                      second)
     extracted (->> mql-query
                    parser
                    (util/extract-clause :WHERE)
                    first
                    second)]
    (cond
      (not (nil? where-false)) (fn [_] false)
      (not (nil? extracted)) (insta/transform mql-evaluators extracted)
      :else (fn [_] true))))

(defn query->having-pred
  "Computes a predicate which can be used to determine if a specific datum
   satisfies the HAVING clause of this query.

   mql-query: A string representing the query.

   Returns: A predicate function of datum -> boolean."
  [mql-query]
  (let
    [parsed (parser mql-query)
     extracted (->> mql-query
                    parser
                    (util/extract-clause :HAVING)
                    first
                    second)]
    (if (nil? extracted) (fn [_] true)
      (insta/transform mql-evaluators extracted))))

(defn queries->superset-projection
  "Computes a projection function which accepts a datum and returns a map
   projecting the necessary properties for all of the queries.
   See: query->projection.

   queries: A seq of strings representing the queries.

   Returns: A function of datum -> map."
  [queries]
  (let
    [parsed (map parser queries)
     stars? (< 0 (count (apply concat (map (partial util/extract-clause :asterisk) parsed))))
     extractors (map #(insta/transform (merge mql-evaluators {:star_property io.mantisrx.mql.compilers.core.operands/star-property->projection}) %) (distinct (apply concat (map parse-tree->properties parsed))))]
    (if stars?
      identity
      (apply t/select-list->fn extractors))))

(defn queries->superset-projection-lazy
  [queries]
  (let
    [parsed (map parser queries)
     stars? (< 0 (count (apply concat (map (partial util/extract-clause :asterisk) parsed))))
     extractors (map #(insta/transform (merge mql-evaluators {:star_property io.mantisrx.mql.compilers.core.operands/star-property->projection}) %) (distinct (apply concat (map parse-tree->properties parsed))))
     extraction-fn (apply t/select-list->fn extractors)]
    (if stars?
      (fn [datum] (doto ^Map datum (.putAll ^Map (extraction-fn datum))))
      extraction-fn)))

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
    (parser query)
    (util/extract-clause :AGG_LIST) 
    (insta/transform mql-evaluators)
    first))

(defn query->groupby
  "Extracts a function of datum -> key for the group by key."
  [query]
  (comp
    doall
    (->> (parser query)
         (util/extract-clause :GROUP)
         (insta/transform mql-evaluators)
         first
         :groupby)))

(defn query->orderby
  "Extracts a function of Observable -> Observable in which the result
   is sorted."
  [query]
  (let [ordering (->>
                   (parser query)
                   (util/extract-clause :ORDER)
                   (insta/transform mql-evaluators)
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
    (parser query)
    (util/extract-clause :LIMIT)
    (insta/transform mql-evaluators)
    first
    :limit))

(defn query->window
  "Extracts the window seconds from the query."
  [query]
  (->> (parser query)
       (util/extract-clause :WINDOW)
       first
       rest
       (map second)
       (map read-string)
       vec))

(defn query->sampler
  "Computes a sampling function which accepts a datum and returns a boolean
   indicating wether or not the datum should be sampled.

   mql-query: A string representing the mql query.

   Returns: A function of datum -> boolean."
  [mql-query]
  (let
    [parsed (parser mql-query)
     extracted (first (util/extract-clause :SAMPLE parsed))]
    (if (nil? extracted)
      (fn [_] true)
      (:sample (insta/transform mql-evaluators extracted)))))

(defn query->subjects
  ^java.util.List
  [mql-query]
  (let
    [parsed (parser mql-query)
     extracted (map rest (util/extract-clause :table_list parsed))]
    (if (empty? extracted)
      (list "stream")
      (flatten (insta/transform mql-evaluators extracted)))))

(defn query->extrapolator
  [mql-query]
  (let
    [evaluators (merge mql-evaluators {:SAMPLE t/sample-config->extrapolation-fn})
     clause 
     ( ->> mql-query
           parser
           (util/extract-clause :SAMPLE)
           first)]
    (if (nil? clause)
      identity
      (insta/transform evaluators clause))))
