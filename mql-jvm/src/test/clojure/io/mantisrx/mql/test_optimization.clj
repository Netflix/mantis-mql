(ns io.mantisrx.mql.test-optimization
  "Differential tests for the optimizer.

   Every rule in optimization.cljc replaces a regex with string operators, so the property
   worth testing is not the shape of the rewritten tree but that the rewrite decides exactly
   what the regex decided -- for every input, including the ones that make the two disagree.
   Each test below therefore compiles the pattern with java.util.regex under the same flags
   MQL uses (DOTALL only, see transformers.cljc), runs the optimized tree, and asserts the
   two agree; a rule that declines to fire still passes, because the unoptimized tree is the
   regex itself.

   The predicates are built with where/binary-expr->pred and where/search-condition->pred --
   the same functions transformers.cljc wires the parse tree through -- so the operators under
   test are the ones that ship, not a transcription of them."
  (:require [clojure.test :refer :all]
            [clojure.zip :as zip]
            [clojure.string :as string]
            [io.mantisrx.mql.optimization :as opt]
            [io.mantisrx.mql.compilers.core.where :as where])
  (:import [java.util.regex Pattern]))

(def ^:private prop (fn [datum] (get datum "a")))

(defn- regex-tree
  [pattern]
  [:boolean_test prop [:REGEX_OPERATOR "==~"] [:re_expression pattern]])

(defn- optimize
  [pattern]
  (opt/run (zip/vector-zip (regex-tree pattern))))

(defn- ->pred
  "Compiles an optimized tree into a predicate, mirroring the transformers.cljc wiring."
  [node]
  (case (first node)
    :boolean_test (if (= 2 (count node))
                    (->pred (second node))
                    (where/binary-expr->pred
                      (nth node 1)
                      (second (nth node 2))
                      (let [operand (last node)]
                        (if (and (vector? operand) (= :re_expression (first operand)))
                          (Pattern/compile (second operand) Pattern/DOTALL)
                          operand))))
    :boolean_term (if (= 2 (count node))
                    (->pred (second node))
                    (where/boolean-term->pred (->pred (nth node 1)) nil (->pred (nth node 3))))
    :search_condition (if (= 2 (count node))
                        (->pred (second node))
                        (where/search-condition->pred
                          (->pred (nth node 1)) nil (->pred (nth node 3))))))

(defn- differs
  "Inputs on which the optimized tree disagrees with the pattern it was derived from."
  [pattern inputs]
  (let [compiled (Pattern/compile pattern Pattern/DOTALL)
        pred (->pred (optimize pattern))]
    (for [in inputs
          :let [expected (.matches (.matcher compiled ^String in))
                actual (boolean (pred {"a" in}))]
          :when (not= expected actual)]
      {:input in :regex expected :optimized actual})))

(defn- rewritten?
  [pattern]
  (not= (optimize pattern) (regex-tree pattern)))

;;;; Inputs are shared across patterns on purpose: an input that is irrelevant to one pattern
;;;; is often the counterexample for another, and a rule that over-fires is far more likely to
;;;; be caught by an input drawn from a neighbouring case than by one tailored to its own.
(def ^:private inputs
  ["" "a" "abba" "abba1234" "1234abba" "xabbax" "dabba" "ABBA" "AbBa" "abb" "abbaabba"
   "abba\n" "\nabba" "abc" "def" "ghi" "xyz" "abcdef" "xabc" "abcx" "xabcx"
   "prod" "PROD" "Prod" "test-prod-1" "production" "nonprod" "17.32.0" "17.32.O"
   "a.b" "a|b" "aXb" "a\\b" "a$" "a$b" "$" "^a" "(focus)" "(focus)test" "x(focus)"
   "{\"prop\":\"123\"}" "prop\":\"123" "123" "k" "K" "K" "s" "S" "ſ"
   "straße" "STRASSE" "i" "I" "İ" "ı" "é" "É"])

(deftest test-terminal-shapes-preserve-semantics
  (doseq [pattern ["abba" "^abba" "abba$" "^abba$"
                   "abba.*" "^abba.*"
                   ".*abba" ".*abba$"
                   ".*abba.*" "^.*abba.*$"
                   "a\\.b" "\\(focus\\)" "^\\(focus\\).*" ".*prop\\\"\\:\\\"123.*"]]
    (testing pattern
      (is (rewritten? pattern) "expected the optimizer to rewrite this shape")
      (is (empty? (differs pattern inputs))))))

(deftest test-case-insensitive-folding-is-ascii-only
  (testing "(?i) in MQL is ASCII-only, because patterns compile without UNICODE_CASE.
            equalsIgnoreCase and regionMatches(true, ...) fold more than that, so the *i
            operators must not be implemented with them."
    (doseq [pattern ["(?i)abba" "(?i)abba.*" "(?i).*abba" "(?i).*abba.*"
                     "(?i)k" "(?i)s" "(?i)i" "(?i)straße" "(?i).*prod.*"]]
      (testing pattern
        (is (rewritten? pattern))
        (is (empty? (differs pattern inputs))))))
  (testing "a (?i) over a literal with no ASCII letter folds nothing, so it collapses to the
            cheaper case-sensitive operator."
    (is (= [:boolean_test prop [:BINARY_OPERATOR "=="] "17.32.0"]
           (optimize "(?i)17\\.32\\.0")))))

(deftest test-alternations-preserve-semantics
  (doseq [pattern ["abc|def|ghi" "^abc|def|ghi$" "(abc|def)" ".*(abc|def).*"
                   "pre(abc|def)" "(abc|def)post" "pre(abc|def)post"
                   ".*(abc|def)" "(abc|def).*"
                   "abba.*|.*abc" ".*abba.*|.*abc" "abc|.*def.*|ghi.*"
                   "a\\|b|c" "(?i)abc|def"]]
    (testing pattern
      (is (rewritten? pattern))
      (is (empty? (differs pattern inputs))))))

(deftest test-mixed-shape-alternation-is-not-collapsed
  (testing "A mixed alternation must become an OR of per-branch operators, never one list
            operator: as `contains-any(abc, def)` the pattern below would accept \"xdefy\",
            which the regex rejects because that branch is anchored at the end."
    (is (empty? (differs ".*abc.*|.*def" (conj inputs "xdefy" "xdef" "abcdef" "defx"))))))

(deftest test-unsupported-constructs-are-left-alone
  (testing "Rules must decline anything whose semantics they cannot reproduce exactly."
    (doseq [pattern ["\\d+" "\\w" "\\s" "\\Qa.b\\E" "\\p{Alpha}" "(a)\\1" "\\babba\\b"
                     "[abc]" "[^a]" "a{2}" "a+" "a?" "a.*b.*c" ".*a.*b.*"
                     "(a|b)(c|d)" "(a)(b)" "(?:a)" "(?<=a)b" "((?!abba).)*"
                     "(?s)a" "(?m)a" "a(?i)b" "" ".*" "^" "$"]]
      (testing pattern
        (is (not (rewritten? pattern)) "expected the optimizer to decline this pattern")
        (is (empty? (differs pattern inputs)))))))
