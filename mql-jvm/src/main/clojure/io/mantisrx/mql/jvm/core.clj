(ns io.mantisrx.mql.jvm.core
  "The JVM core interface for MQL.
   This is a refactoring attempt to separate MQL into three projects;
   core: Fully compatible pure Clojure implementation.
   jvm: JVM specific functionality, optimizations, etc..."
  (:require [instaparse.core :as insta]
            [clojure.java.io :as io]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.interop :as rxi]
            [clojure.set :as s]
            [io.mantisrx.mql.jvm.compilers.core :as t]
            [io.mantisrx.mql.jvm.compilers.table :as table]
            [io.mantisrx.mql.util :as util]
            [io.mantisrx.mql.parser :as mqlp]
            [io.mantisrx.mql.jvm.optimization :as opt]
            [io.mantisrx.mql.core :as mql]
            [io.mantisrx.mql.jvm.compilers.core :as compiler]
            [instaparse.print :as iprint])
  (:import rx.Observable java.util.Map java.util.concurrent.TimeUnit)
  (:gen-class
    :name io.mantisrx.mql.jvm.Core
    :methods [#^{:static true} [evalMql [String java.util.Map] rx.Observable]
              #^{:static true} [evalMql [String java.util.Map String] rx.Observable]
              #^{:static true} [evalMql [String java.util.Map String Boolean] rx.Observable]
              #^{:static true} [isAggregate [String] Boolean]]))

(def ^:export parser (comp opt/run clojure.zip/vector-zip mqlp/parser clojure.string/trim))

(def evaluators {})

(def mql-evaluators (merge compiler/mql-evaluators evaluators))

(definterface
  Query
  (^String getSubscriptionId [])
  (^String getRawQuery [])
  (^java.util.List getSubjects [])
  (^Boolean matches [^java.util.Map datum])
  (^Boolean sample [^java.util.Map datum])
  (^java.util.Map project [^java.util.Map this])) 

(defn eval-mql
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
           transformers (assoc mql-evaluators :table_list (partial table/table-list->observable context-map))
           transformers (assoc transformers :TABLE_NO_FROM (fn [& args] [(get context-map "stream" (rx/empty)) (apply merge args)]))
           transformers (merge transformers (get mql/modes mode {}))
           parsed (parser mql-string)]
       (insta/transform transformers parsed)))))

(defn -evalMql
  "A Java-callable wrapper around the eval-mql function."
  ([query context]
   (eval-mql query context "full"))
  ([query context mode]
   (eval-mql query context mode))
  ([query context mode threading]
   (eval-mql query context mode threading)))
