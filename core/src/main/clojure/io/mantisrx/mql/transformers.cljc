(ns io.mantisrx.mql.transformers
  (:require #?@(:clj [[rx.lang.clojure.core :as rx]
                      [rx.lang.clojure.interop :as rxi]
                      [rx.lang.clojure.blocking :as rxb]
                       [clojure.data.json :as json]
                      [io.mantisrx.mql.properties :as mqlp]]
                :cljs [[cljs.reader :refer [read-string]]])
            [io.mantisrx.mql.util :as util]
            [io.mantisrx.mql.compilers.core.sampling :as samp] ;; Force loading of this namespace for compilation reasons
            [io.mantisrx.mql.compilers.core.join] ;; Force loading of this namespace for compilation reasons
            [io.mantisrx.mql.compilers.core.select] ;; Force loading of this namespace for compilation reasons
            [io.mantisrx.mql.compilers.core.operands] ;; Force loading of this namespace for compilation reasons
            [io.mantisrx.mql.compilers.core.where] ;; Force loading of this namespace for compilation reasons
            [clojure.string :as string]
            [clojure.set :as cljset])
  #?(:clj (:import rx.Observable
                   rx.schedulers.Schedulers
                   java.util.concurrent.TimeUnit
                   java.util.List
                   java.util.ArrayList
                   java.util.RandomAccess
                   java.util.Map
                   java.util.TreeMap
                   clojure.lang.RT
                   java.util.LinkedHashMap)))

(def ^:dynamic *threading-enabled* false)

;;;;
;;;; SELECT
;;;;

(def asterisk->fn io.mantisrx.mql.compilers.core.select/asterisk->fn)
(def select-list->fn io.mantisrx.mql.compilers.core.select/select-list->fn)

;;;;
;;;; AGGREGATE SELECT
;;;;

;;;; TODO: Put the fns here.

;;;;
;;;; WHERE
;;;;

; TODO: Imports from WHERE

;;;;
;;;; SAMPLE
;;;;

(def sample-config->sampler io.mantisrx.mql.compilers.core.sampling/sample-config->sampler)

;;;;
;;;; Operands
;;;;

(def sw-property->fn io.mantisrx.mql.compilers.core.operands/sw-property->fn)
(def literal->fn io.mantisrx.mql.compilers.core.operands/literal->fn)
(def not-operand->operand io.mantisrx.mql.compilers.core.operands/not-operand->operand)

;;;;
;;;; JOIN
;;;;

(def join->fn io.mantisrx.mql.compilers.core.join/join->fn)

(def mql-evaluators
  {:QUERY identity
   :asterisk asterisk->fn
   :SELECTLIST select-list->fn
   :AGG_LIST io.mantisrx.mql.compilers.core.select/agg-select-list->fn
   :WHERE io.mantisrx.mql.compilers.core.where/where-clause->fn
   :WHERE_TRUE (fn [& args] {:where (fn [datum] true)})
   :WHERE_FALSE (fn [& args] {:where (fn [datum] false)})
   :JOIN join->fn
   :HAVING io.mantisrx.mql.compilers.core.where/having-clause->fn
   :SAMPLE sample-config->sampler
   :AGG_OP io.mantisrx.mql.compilers.core.select/agg-op->selector+aggregator
   :as_clause (fn [n] (n {}))
   :StarBinaryExpr io.mantisrx.mql.compilers.core.where/star-binary-expr->pred
   :func_kw io.mantisrx.mql.compilers.core.select/agg-func->fn
   :number (partial literal->fn read-string)
   :property_number read-string
   :integer (partial literal->fn (comp int read-string))
   :string_literal (partial literal->fn util/strip-surround)
   :boolean_literal (partial literal->fn read-string)
   :not_search_condition not-operand->operand
   :property io.mantisrx.mql.compilers.core.operands/property->fn
   :property_with_as (fn [p as] p)
   :sw_property io.mantisrx.mql.compilers.core.operands/sw-property->fn
   :star_property io.mantisrx.mql.compilers.core.operands/star-property->fn
   :select_operand identity
   :not_operand not-operand->operand
   :distinct_operand io.mantisrx.mql.compilers.core.operands/distinct-operand->property
   :BINARY_OPERATOR identity
   :REGEX_OPERATOR identity
   :tick io.mantisrx.mql.compilers.core.operands/tick->operand
   :boolean_test io.mantisrx.mql.compilers.core.where/binary-expr->pred
   :boolean_term io.mantisrx.mql.compilers.core.where/boolean-term->pred
   :search_condition io.mantisrx.mql.compilers.core.where/search-condition->pred
   :re_expression #?(:clj #(java.util.regex.Pattern/compile % java.util.regex.Pattern/DOTALL)
                     :cljs re-pattern)
   :q_pword util/strip-surround
   :null (fn [_] nil)
   :word identity
   :pword identity
   :desc_kw identity
   })
 

