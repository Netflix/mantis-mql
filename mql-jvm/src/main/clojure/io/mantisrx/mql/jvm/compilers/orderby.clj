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
(ns io.mantisrx.mql.jvm.compilers.orderby)

(defn order->fn
  "Transforms a property to a function for use in sorting.

   Usage: (order->fn property)

   prop: A property function as returned by prop->fn.
   direction: ASC or DESC for ascending or descending sort.

   Returns a comparator function intended to be used with rx/sort to sort
   an observable. It can however be used anywhere a comparator would be."
  ([prop]
   (order->fn prop "asc"))
  ([prop direction]
   {:orderby prop
    :order-comparator (if (= "desc" (clojure.string/lower-case direction))
                        (comparator >)
                        compare)}))
