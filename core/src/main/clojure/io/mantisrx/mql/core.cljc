(ns io.mantisrx.mql.core
  (:require [instaparse.core :as insta]
            [io.mantisrx.mql.transformers :as t]
            [io.mantisrx.mql.util :as util]
            [io.mantisrx.mql.parser :as mqlp]
            [io.mantisrx.mql.optimization :as opt]
            [instaparse.print :as iprint])
  #?(:clj
     (:import java.util.Map))
  (:gen-class)
  )

;;;;
;;;; Parser
;;;;

#?(:cljs (defn read-string [x] (cljs.reader/read-string x)))
#?(:clj (def ^:export parser (comp opt/run clojure.zip/vector-zip mqlp/parser clojure.string/trim)))
#?(:cljs (def ^:export parser (comp mqlp/parser clojure.string/trim)))


#?(:clj
   (definterface
     Query
     (^String getSubscriptionId [])
     (^String getRawQuery [])
     (^java.util.List getSubjects [])
     (^Boolean matches [^java.util.Map datum])
     (^Boolean sample [^java.util.Map datum])
     (^java.util.Map project [^java.util.Map this])))


;;;;
;;;; Compiler
;;;;

(def mql-evaluators t/mql-evaluators)


;; Modes should be merged onto mql-evaluators to achieve specific behavior
(def modes
  {"client" {:SAMPLE (fn [_] {:sample (fn [_] true)})
             :number (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as read-string)
             :integer (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as (comp int read-string))
             :string_literal (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as util/strip-surround)
             :boolean_literal (partial io.mantisrx.mql.compilers.core.operands/literal->fn-with-as read-string)
             :property_with_as io.mantisrx.mql.compilers.core.operands/property-with-as->fn}
   "full" {}
   })
