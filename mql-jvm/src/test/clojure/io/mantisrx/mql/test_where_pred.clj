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
(ns io.mantisrx.mql.test-where-pred
  "The objective of this namespace is to pin down the behaviour of the compiled
   where predicates, which is the hot path executed once per event per active
   query.

   binary-expr->pred hoists the operator lookup, the nil policy and the
   'is this operand a function of the datum' test out of the per event closure,
   and operands are tagged with metadata via a type which implements IFn
   directly rather than via clojure.lang.AFunction.withMeta (which returns a
   variadic RestFn trampoline). These tests exist to demonstrate that neither
   change is observable: predicate results, including MQL's nil semantics and
   the non-boolean truthy values returned by the containment operators, are
   identical to a reference implementation built on check-predicate, and
   operand metadata still round trips."
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.fnmeta :as fnmeta]
            [io.mantisrx.mql.properties :as mqlp]
            [io.mantisrx.mql.compilers.core.operands :as operands]
            [io.mantisrx.mql.compilers.core.where :as where]
            [io.mantisrx.mql.jvm.core :refer [eval-mql]]
            [rx.lang.clojure.blocking :as rxb])
  (:import rx.Observable))

(def ^:private ops (var-get #'where/ops))

(defn- legacy-binary-pred
  "binary-expr->pred as it was written before the compile time hoisting; the
   reference against which the current implementation is compared."
  [operand-a operator operand-b]
  (let [operator (ops operator)]
    (fn [datum]
      (let [a (if (fn? operand-a) (operand-a datum) operand-a)
            b (if (fn? operand-b) (operand-b datum) operand-b)]
        (where/check-predicate operator a b)))))

(def ^:private comparison-operators ["=" "==" "!=" "<>" "<" ">" "<=" ">="])

(def ^:private data
  [{"a" 1}
   {"a" 2}
   {"a" 3}
   {"a" nil}
   {}])

(deftest test-binary-pred-matches-legacy-semantics
  (testing "every operator agrees with the legacy predicate on every datum,
            including the nil and absent property cases."
    (doseq [operator comparison-operators
            rhs [1 2 nil]
            datum data]
      (let [lhs (operands/property->fn "a")
            actual ((where/binary-expr->pred lhs operator rhs) datum)
            expected ((legacy-binary-pred lhs operator rhs) datum)]
        (is (= expected actual)
            (str "operand a " (pr-str (clojure.core/get datum "a"))
                 " " operator " " (pr-str rhs)))))))

(deftest test-binary-pred-preserves-non-boolean-results
  (testing "the containment operators return their truthy value, not a coerced
            boolean, exactly as they did before."
    (doseq [operator ["==+" "==*"]
            rhs [["foo" "bar"] ["nope"]]
            datum [{"a" "foobar"} {"a" "bar"} {"a" nil} {}]]
      (let [lhs (operands/property->fn "a")]
        (is (= ((legacy-binary-pred lhs operator rhs) datum)
               ((where/binary-expr->pred lhs operator rhs) datum))
            (str operator " " (pr-str rhs) " " (pr-str datum)))))))

(deftest test-binary-pred-constant-operands
  (testing "constants on either side are not mistaken for functions of the datum."
    (is (true? ((where/binary-expr->pred 1 "=" 1) {})))
    (is (false? ((where/binary-expr->pred 1 "=" 2) {})))
    (is (true? ((where/binary-expr->pred 1 "<" 2) {})))))

(deftest test-star-binary-pred-matches-legacy-semantics
  (testing "the star form still returns a strict boolean and honours nil."
    (let [lhs (operands/sw-property->fn "e")]
      (doseq [operator comparison-operators
              rhs [1 2 nil]
              datum [{"e1" 1 "e2" 2} {"e1" nil} {}]]
        (let [actual ((where/star-binary-expr->pred lhs operator rhs) datum)
              expected (if (some (fn [v] (where/check-predicate (ops operator) v rhs))
                                 (map :value (lhs datum)))
                         true
                         false)]
          (is (= expected actual)
              (str "star " operator " " (pr-str rhs) " " (pr-str datum))))))))

(deftest test-operand-metadata-round-trips
  (testing "operands remain functions and retain their metadata."
    (let [prop (operands/property->fn "a")]
      (is (fn? prop))
      (is (ifn? prop))
      (is (= {:name '("a")} (update (meta prop) :name #(apply list %))))
      (is (= 1 (prop {"a" 1})))
      (is (nil? (prop {}))))
    (testing "and derived metadata via with-meta keeps both properties."
      (let [renamed (operands/property-with-as->fn (operands/property->fn "a") "b")]
        (is (fn? renamed))
        (is (= ["b"] (:name (meta renamed))))
        (is (= 1 (renamed {"a" 1}))))
      (let [distinct-prop (operands/distinct-operand->property
                            (operands/property->fn "a"))]
        (is (fn? distinct-prop))
        (is (true? (:distinct (meta distinct-prop))))
        (is (= 1 (distinct-prop {"a" 1})))))
    (testing "literals and ticks too."
      (let [lit (operands/literal->fn identity "x")]
        (is (fn? lit))
        (is (= ["x"] (:name (meta lit))))
        (is (= "x" (lit {}))))
      (let [tick (operands/tick->operand nil)]
        (is (fn? tick))
        (is (= ["tick"] (:name (meta tick))))
        (is (number? (tick {})))))))

(deftest test-fn-with-meta
  (testing "fn-with-meta is a drop in for with-meta on functions."
    (let [f (fnmeta/fn-with-meta (fn [x] (inc x)) {:name ["inc"]})]
      (is (fn? f))
      (is (ifn? f))
      (is (= {:name ["inc"]} (meta f)))
      (is (= 2 (f 1)))
      (is (= 2 (apply f [1])))
      (is (= [2 3] (map f [1 2])))
      (testing "and re-tagging replaces rather than nests the wrapper."
        (let [g (with-meta f {:name ["other"]})]
          (is (= {:name ["other"]} (meta g)))
          (is (= 2 (g 1))))))))

(deftest test-nested-property-lookup-still-works
  (testing "the single key fast path in property->fn does not break nesting."
    (let [nested (operands/property->fn "a" "b")]
      (is (= 1 (nested {"a" {"b" 1}})))
      (is (nil? (nested {"a" {}})))
      (is (nil? (nested {}))))
    (testing "and list indexing, including negative indices, is unchanged."
      (let [indexed (operands/property->fn "a" 1)
            from-end (operands/property->fn "a" -1)]
        (is (= 2 (indexed {"a" (java.util.ArrayList. [1 2 3])})))
        (is (= 3 (from-end {"a" (java.util.ArrayList. [1 2 3])})))))))

(deftest test-end-to-end-where-clauses
  (testing "queries compiled through the full stack filter as expected."
    (is (= [{"a" 1}]
           (rxb/into [] (eval-mql "select * from stream where a == 1"
                                  {"stream" (Observable/just {"a" 1} {"a" 2} {"b" 3})}))))
    (testing "a missing or nil property is not less than anything."
      (is (= [{"a" 1}]
             (rxb/into [] (eval-mql "select * from stream where a < 2"
                                    {"stream" (Observable/just {"a" 1} {"a" nil} {"b" 3})})))))
    (testing "but it does participate in equality checks."
      (is (= [{"a" 2} {"b" 3}]
             (rxb/into [] (eval-mql "select * from stream where a != 1"
                                    {"stream" (Observable/just {"a" 1} {"a" 2} {"b" 3})})))))))
