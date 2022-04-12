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
(ns io.mantisrx.mql.jvm.compilers.table
  (:require
    [rx.lang.clojure.core :as rx]))

(defn table->observable
  "Converts the TABLE component of the query into a tuple.

   obs: An observable over which this table clause should operate.
   ops A seq of maps of clause -> fn or seq (in the case of window).

   Returns a tuple of [observable map]."
  [obs & ops] [obs (apply merge ops)])

(defn from-clause->observable
  "This function should receive a primary obs, and joining fns."
  [obs & join-fns]
  (reduce (fn [obs f] (f obs)) obs join-fns))

(defn table-list->observable
  [context & subjects]
  (apply rx/merge (map #(get context % (rx/empty)) subjects)))
