;
; Copyright 2019 Netflix, Inc.
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
(ns io.mantisrx.mql.jvm.compilers.groupby)

(defn group-by->fn
  "Transforms group-by to a function.

   Usage: (group-by->fn prop1 prop2... propn)

   props: One or more properties functions as returned by prop->fn.

   Returns a fn of datum -> [obj1, obj2... objn] which can be used as the fn
   parameter for rx's groupBy. rxClojure groupBy does not function as
   expected [0] and thus it is recommended to utilize this as such:
   (.groupBy ^rx.Observable obs (rxi/fn* fn)).

   [0] https://github.com/ReactiveX/RxClojure/blob/0.x/src/main/clojure/rx/lang/clojure/core.clj#L478"
  [& props]
  {:groupby
   (fn [datum] (map #(% datum) props))})
