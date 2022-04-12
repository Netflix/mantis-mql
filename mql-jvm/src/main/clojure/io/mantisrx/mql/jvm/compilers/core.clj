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
(ns io.mantisrx.mql.jvm.compilers.core
  "Primary compiler interface for MQL on the JVM."
  (:require [rx.lang.clojure.core :as rx]
            [io.mantisrx.mql.util :as util]
            [io.mantisrx.mql.jvm.compilers.query :as query]
            [io.mantisrx.mql.jvm.compilers.table :as table]
            [io.mantisrx.mql.jvm.compilers.groupby :as groupby]
            [io.mantisrx.mql.jvm.compilers.orderby :as orderby]
            [io.mantisrx.mql.jvm.compilers.window :as window]
            [io.mantisrx.mql.jvm.compilers.limit :as limit]
            [io.mantisrx.mql.jvm.compilers.anomaly :as anomaly]
            [io.mantisrx.mql.jvm.compilers.join :as join]
            [io.mantisrx.mql.transformers :as t]
            ))

(def ^:dynamic *threading-enabled* false)

(def jvm-evaluators
  {:QUERY identity
   :NORMAL_QUERY query/query->observable
   :AGG_QUERY query/agg-query->observable
   :FROM table/from-clause->observable
   :table_list (partial table/table-list->observable {})
   :JOIN join/join->fn
   :TABLE table/table->observable
   :TABLE_NO_FROM (partial table/table->observable (rx/empty))
   :ANOMALY anomaly/mad-anomaly->fn
   :GROUP groupby/group-by->fn
   :ORDER orderby/order->fn
   :LIMIT limit/limit->fn
   :WINDOW window/window->fn
   :WINDOW_N window/n-window->fn
   :as_clause (fn [n] (n {}))
   :property_with_as (fn [p as] p)
   :re_expression  #(java.util.regex.Pattern/compile % java.util.regex.Pattern/DOTALL)
   :q_pword util/strip-surround
   })

(def mql-evaluators (merge t/mql-evaluators jvm-evaluators))
