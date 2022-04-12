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
(ns io.mantisrx.mql.jvm.compilers.join
  ""
  (:require
    [rx.lang.clojure.core :as rx]
    [rx.lang.clojure.interop :as rxi])
  (:import rx.Observable
           rx.observables.GroupedObservable
           java.util.concurrent.TimeUnit))

(defn- join-obs
  "Joins primary-obs and joining-obs together.
   This currently only supports left inner joins, but others may be an option in the future.

   NOTE: - The order in (merge datum-b datum-a) determines left/right join behavior.
   - The rx/first on the filter forces an inner join

   joining-observable: The observable to be joined to the primary observable.
   seconds           : The number of seconds for which to wait for matching data.
   key-fn            : A key to check for equality between observables.
   primary-obs       : The table being joined onto.

   Returns: An observable implementing the join.
   "
  [joining-observable seconds key-fn primary-obs]
  (rx/flatmap (fn [^GroupedObservable go]
                (let
                  [k (.getKey go)
                   seconds (seconds {})]
                  (.join go
                         (rx/first (rx/filter #(= (key-fn %) k) joining-observable))
                         (rxi/fn* (fn [_] (Observable/interval seconds TimeUnit/SECONDS)))
                         (rxi/fn* (fn [_] (Observable/interval seconds TimeUnit/SECONDS)))
                         (rxi/fn* (fn [datum-a datum-b] (merge datum-b datum-a))) ;; Prefer properties from the source stream.
                         )))
              (.groupBy ^Observable primary-obs (rxi/fn* key-fn))))

(defn join->fn
  [joining-observable seconds key-fn]
  (fn [obs] (join-obs joining-observable seconds key-fn obs)))
