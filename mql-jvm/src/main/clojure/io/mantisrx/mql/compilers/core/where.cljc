(ns io.mantisrx.mql.compilers.core.where
  (:require [clojure.string :as string]))



;;;; ---------------------------------------------------------------------------
;;;; ASCII case folding
;;;;
;;;; The optimizer rewrites `(?i)`-flagged regexes into the `*i` operators below, so those
;;;; operators must fold exactly the way `(?i)` does -- and no more. MQL compiles patterns with
;;;; DOTALL only (see transformers.cljc), so `(?i)` is Java's *ASCII-only* case insensitivity:
;;;; UNICODE_CASE is never set. `equalsIgnoreCase` and `regionMatches(true, ...)` fold over the
;;;; full Unicode case tables and therefore accept strings the regex rejects -- U+017F LATIN
;;;; SMALL LETTER LONG S vs "s", U+212A KELVIN SIGN vs "k", U+0130 vs "i", and every accented
;;;; pair. Folding A-Z and nothing else is the only exact equivalent.
;;;;
;;;; The comparisons scan characters in place rather than lower-casing their operands, because
;;;; these run once per registered query per event: folding would allocate a String per event
;;;; per term, which is most of what rewriting away the regex was meant to save.
;;;; ---------------------------------------------------------------------------

#?(:clj
   (defn- ascii-lower
     ^long [^long c]
     (if (and (>= c 65) (<= c 90)) (+ c 32) c)))

#?(:clj
   (defn- ci-region=
     "True when term occurs in s at offset off, comparing with ASCII-only case folding."
     [^String s ^long off ^String term]
     (let [n (.length term)]
       (and (<= (+ off n) (.length s))
            (loop [i 0]
              (cond
                (>= i n) true
                (== (ascii-lower (long (.charAt s (+ off i))))
                    (ascii-lower (long (.charAt term i)))) (recur (inc i))
                :else false))))))

#?(:cljs
   (defn- ascii-fold [s] (string/replace (str s) #"[A-Z]" (fn [c] (.toLowerCase c)))))

(defn- ci-equals?
  [target ^String term]
  #?(:clj (let [^String s (str target)]
            (and (== (.length s) (.length term)) (ci-region= s 0 term)))
     :cljs (= (ascii-fold (str target)) (ascii-fold term))))

(defn- ci-starts-with?
  [target ^String term]
  #?(:clj (ci-region= (str target) 0 term)
     :cljs (string/starts-with? (ascii-fold (str target)) (ascii-fold term))))

(defn- ci-ends-with?
  [target ^String term]
  #?(:clj (let [^String s (str target)
                off (- (.length s) (.length term))]
            (and (>= off 0) (ci-region= s off term)))
     :cljs (string/ends-with? (ascii-fold (str target)) (ascii-fold term))))

(defn- ci-includes?
  [target ^String term]
  #?(:clj (let [^String s (str target)
                lim (- (.length s) (.length term))]
            (loop [i 0]
              (cond
                (> i lim) false
                (ci-region= s i term) true
                :else (recur (inc i)))))
     :cljs (string/includes? (ascii-fold (str target)) (ascii-fold term))))

(defn- coll-some
  "The terms operand is a collection when the optimizer folded an alternation into one
   operator and a bare string otherwise; both spellings are accepted, as for ==+ and ==*."
  [pred terms]
  (let [terms (if (coll? terms) terms [terms])]
    (boolean (some pred terms))))

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
                    ;; Accepts a collection as well as a single term, as ==+ and ==* do: the
                    ;; optimizer emits a collection when it folds an alternation into one
                    ;; operator, e.g. /(abc|def).*/ -> startsWith ("abc" "def").
                    "startsWith" (fn [target terms]
                                   (coll-some
                                     #(.startsWith ^String (str target) ^String %1)
                                     terms))

                    ;; Emitted only by the optimizer, which rewrites an equivalent regex into
                    ;; one of these. See optimization.cljc for which shape produces which.
                    "endsWith" (fn [target terms]
                                 (coll-some
                                   #(.endsWith ^String (str target) ^String %1)
                                   terms))
                    "==i" (fn [target terms] (coll-some #(ci-equals? target %1) terms))
                    "==+i" (fn [target terms] (coll-some #(ci-includes? target %1) terms))
                    "startsWithi" (fn [target terms]
                                    (coll-some #(ci-starts-with? target %1) terms))
                    "endsWithi" (fn [target terms]
                                  (coll-some #(ci-ends-with? target %1) terms))
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
