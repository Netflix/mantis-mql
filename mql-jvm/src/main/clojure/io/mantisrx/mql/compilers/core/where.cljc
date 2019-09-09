(ns io.mantisrx.mql.compilers.core.where
  (:require [clojure.string :as string]))



(def ^:private ops {
                    "=" =
                    "==" =
                    "!=" not=
                    "<>" not=
                    "<" <
                    ">" >
                    "<=" <=
                    ">=" >=
                    "==~" #?(:clj #(.matches (.matcher ^java.util.regex.Pattern %2 ^String (str %1)))
                             :cljs #(re-matches %2 (str %1)))
                    "==+" (fn [target terms]
                            (let
                              [terms (if (coll? terms) terms [terms])
                               target (str target)]
                              (some
                                #(string/includes? target %1)
                                terms)))
                    "==*" (fn [target terms]
                            (let
                              [terms (if (coll? terms) terms [terms])
                               target (str target)]
                              (some #(= %1 target) terms)))
                    "startsWith" (fn [target ^String term]
                                   (.startsWith ^String (str target) term))
                    })

(defn where-clause->fn
  [pred]
  {:where pred})

(defn having-clause->fn
  [pred]
  {:having pred})


(defn check-predicate
  "Helper function to check predicates, this exists primarily because of MQL's
   semantics surrounding nil (null) properties. If we're checking equality or
   inequality then null is acceptable, otherwise the predicate is false.

   operator: A function sering as the operator, ex: =, <, not=
   lhs: The left hand side expression.
   rhs: The right hand side expression.

   Returns the result of (operator lhs rhs) and false if either operand is
   nil, unless operator is an equality (or inequality) check.)"
  [operator lhs rhs]
  (cond
    (or (= = operator) (= not= operator)) (operator lhs rhs)
    (or (nil? lhs) (nil? rhs)) false
    :else (operator lhs rhs)))

(defn binary-expr->pred
  ([pred] pred)
  ([operand-a operator operand-b]
   (let
     [operator (ops operator)]
     (with-meta #(let [operand-a (if (fn? operand-a) (operand-a %) operand-a)
                       operand-b (if (fn? operand-b) (operand-b %) operand-b)]
                   (check-predicate operator operand-a operand-b))
                {:clause :where}))))

(defn star-binary-expr->pred
  [operand-a operator operand-b]
  (let
    [operator (ops operator)]
    (with-meta #(let [rhs (if (fn? operand-b) (operand-b %) operand-b)]
                  (if (some
                        (fn [lhs] (check-predicate operator lhs rhs))
                        (map :value (operand-a %)))
                    true
                    false))
               {:clause :where})))

(defn search-condition->pred
  "Search conditions are used in the MQL spec to implement OR operations. Due to
   the recursive nature of parsing this function may receive a predicate or
   an OR clause of the form pred-1 OR pred-2.

   pred*: A predicate function.
   op: Always the OR function.

   Returns a predicate function of datum -> boolean in which datum is a map of
   String -> Object."
  ([pred] pred)
  ([pred-1 op pred-2] #(or (pred-1 %) (pred-2 %))))

(defn boolean-term->pred
  "Identical to search-condition->pred with `and` swapped for `or`."
  ([pred] pred)
  ([pred-1 op pred-2] #(and (pred-1 %) (pred-2 %))))
