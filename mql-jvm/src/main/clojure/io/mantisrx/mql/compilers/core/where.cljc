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

(def ^:private nil-tolerant-operators
  "The operators for which MQL considers nil an acceptable operand, see
   check-predicate."
  #{"=" "==" "!=" "<>"})

(defn binary-expr->pred
  ([pred] pred)
  ([operand-a operator operand-b]
   ;; Everything which can be decided while compiling the query is decided
   ;; here, once: which operator function to call, whether that operator
   ;; tolerates nil operands, and whether each operand is a function of the
   ;; datum or a constant. The returned predicate runs per event, per query,
   ;; so it does no reflection, no lookup and no metadata work.
   (let [op      (ops operator)
         nil-ok? (contains? nil-tolerant-operators operator)
         a-fn?   (fn? operand-a)
         b-fn?   (fn? operand-b)]
     (fn [datum]
       (let [lhs (if a-fn? (operand-a datum) operand-a)
             rhs (if b-fn? (operand-b datum) operand-b)]
         (if (or nil-ok? (and (some? lhs) (some? rhs)))
           (op lhs rhs)
           false))))))

(defn star-binary-expr->pred
  [operand-a operator operand-b]
  (let [op      (ops operator)
        nil-ok? (contains? nil-tolerant-operators operator)
        b-fn?   (fn? operand-b)]
    (fn [datum]
      (let [rhs (if b-fn? (operand-b datum) operand-b)]
        (if (some
              (fn [lhs]
                (if (or nil-ok? (and (some? lhs) (some? rhs)))
                  (op lhs rhs)
                  false))
              (map :value (operand-a datum)))
          true
          false)))))

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
