(ns io.mantisrx.mql.optimization
  (:require [clojure.zip :as zip]
            [clojure.string :as string]))

(defn- apply-rule
  [loc rule]
  (if (try ((:pred rule) (zip/node loc))
            (catch Exception e (identity false)))
       (zip/edit loc (:apply rule))
       loc))

;;;; ---------------------------------------------------------------------------------
;;;; The literal class.
;;;;
;;;; Every rule below shares this one definition. A regex "word" is any run of
;;;; characters that carry no regex meaning: either a non-metacharacter, or a backslash
;;;; escape. Java's Pattern accepts \X for every non-alphanumeric X and treats it as a
;;;; literal X, so the escape branch admits exactly that set -- no narrower, or the
;;;; rules decline patterns they could have rewritten, and no wider, or \d / \w / \1
;;;; would be mistaken for literals.
;;;;
;;;; `unescape` below must strip exactly the escapes this class admits. If the two
;;;; disagree, a rule fires and emits a term that still contains a backslash, and the
;;;; resulting string comparison silently matches nothing.
;;;; ---------------------------------------------------------------------------------

(def ^:private word-src
  "(?:[^\\\\.*+?()\\[\\]{}|^$]|\\\\[^A-Za-z0-9])+")

(def word-regex (re-pattern word-src))

(defn unescape
  "Strips the backslash from every escape word-regex admits. Single-pass and
   left-to-right, so an escaped backslash consumes its own escapee rather than being
   re-examined as the escape of the character after it."
  [x]
  (string/replace x #"\\([^A-Za-z0-9])" "$1"))

;;;; ---------------------------------------------------------------------------------
;;;; Terminal shapes. Each captures the literal in group 1, so the term is read off the
;;;; match rather than recovered by index arithmetic on the pattern text.
;;;;
;;;; `^` and `$` are accepted and discarded. MQL evaluates ==~ with Matcher.matches,
;;;; which already requires the whole input to be consumed, so a leading `^` and a
;;;; trailing `$` are both no-ops -- including the `$`-before-final-newline case, since
;;;; a leftover trailing newline fails the full-region match either way.
;;;;
;;;; The four shapes are mutually exclusive: the literal class excludes `.` and `*`, so
;;;; no pattern can satisfy two of them.
;;;; ---------------------------------------------------------------------------------

(def ^:private equals-re   (re-pattern (str "\\^?(" word-src ")\\$?")))
(def ^:private starts-re   (re-pattern (str "\\^?(" word-src ")\\.\\*")))
(def ^:private ends-re     (re-pattern (str "\\.\\*(" word-src ")\\$?")))
(def ^:private contains-re (re-pattern (str "\\^?\\.\\*(" word-src ")\\.\\*\\$?")))

(defn- split-ci
  "Returns [case-insensitive? body] for a pattern, or nil if the pattern uses any
   inline construct other than a single leading (?i). (?i) without UNICODE_CASE folds
   the ASCII range only, which the ==i / ==+i operators reproduce exactly; every other
   (?...) group -- lookahead, non-capturing, other flags -- is left as a regex."
  [p]
  (let [ci?  (string/starts-with? p "(?i)")
        body (if ci? (subs p 4) p)]
    (when-not (string/includes? body "(?")
      [ci? body])))

(defn- regex-node?
  [node]
  (and (vector? node)
       (= :boolean_test (first node))
       (= 4 (count node))
       (vector? (nth node 2))
       (= :REGEX_OPERATOR (first (nth node 2)))
       (vector? (last node))
       (string? (second (last node)))))

(defn- terminal-rule
  "Builds a rule that rewrites one terminal shape into a string operator.
   `ci-op` is used only when the literal actually contains an ASCII letter: (?i) over a
   literal with no letters folds nothing, so those collapse to the cheaper exact op."
  [shape-re plain-op ci-op]
  {:pred (fn [node]
           (and (regex-node? node)
                (let [[_ body] (split-ci (second (last node)))]
                  (boolean (and body (re-matches shape-re body))))))
   :apply (fn [node]
            (let [[ci? body] (split-ci (second (last node)))
                  term (unescape (second (re-matches shape-re body)))
                  fold? (and ci? (boolean (re-find #"[A-Za-z]" term)))]
              [:boolean_test
               (second node)
               [:BINARY_OPERATOR (if fold? ci-op plain-op)]
               term]))})

;;;; ---------------------------------------------------------------------------------
;;;; Alternation.
;;;;
;;;; Because ==~ is a full-region match, matches(A|B|C) is exactly
;;;; matches(A) or matches(B) or matches(C) -- the alternation distributes over the
;;;; whole predicate. So an alternation is split into an OR of independent tests and
;;;; each branch picks its own operator.
;;;;
;;;; This is the only correct treatment. Collapsing an alternation into a single list
;;;; operator is wrong whenever the branches do not all have the same shape:
;;;; `.*Error.*|.*Fatal` is contains-or-endsWith, and evaluating it as
;;;; contains-any(Error, Fatal) reports a match for "xFatalY", which the regex rejects.
;;;;
;;;; The split only fires when *every* branch is itself a terminal shape. A branch that
;;;; stayed a regex would leave two Matcher.matches calls where there was one.
;;;; ---------------------------------------------------------------------------------

(defn- scan-alternatives
  "Splits on the `|` at group depth 0, honouring backslash escapes -- an escaped `\\|` is a
   literal and not a separator. Returns nil if the pattern uses a character class or a
   backreference, or if its parens are unbalanced; otherwise a vector of branches, which
   is a single element when there is no top-level `|`.

   A plain string/split on `|` cannot be used here: it would cut `a\\|b|c` into three
   branches instead of two, and the resulting terms would carry a stray backslash."
  [body]
  (let [n (count body)]
    (loop [i 0 depth 0 start 0 acc []]
      (if (>= i n)
        (when (zero? depth) (conj acc (subs body start)))
        (let [c (.charAt ^String body i)]
          (cond
            (= c \\) (when-not (and (< (inc i) n)
                                    (Character/isDigit (.charAt ^String body (inc i))))
                       (recur (+ i 2) depth start acc))
            (= c \[) nil
            (= c \() (recur (inc i) (inc depth) start acc)
            (= c \)) (if (zero? depth) nil (recur (inc i) (dec depth) start acc))
            (and (= c \|) (zero? depth))
            (recur (inc i) depth (inc i) (conj acc (subs body start i)))
            :else (recur (inc i) depth start acc)))))))

(defn- top-level-alternatives
  "scan-alternatives, but only when there really is a top-level alternation to split."
  [body]
  (when-let [branches (scan-alternatives body)]
    (when (> (count branches) 1) branches)))

(def ^:private terminal-shapes [equals-re starts-re ends-re contains-re])

(defn- terminal-branch?
  [branch]
  (and (not (string/includes? branch "(?"))
       (boolean (some #(re-matches % branch) terminal-shapes))))

(defn- or-tree
  "Left-nested OR, matching the grammar's
   search_condition = boolean_term | search_condition or_kw boolean_term."
  [tests]
  (reduce (fn [acc t] [:search_condition acc [:or_kw "or"] t])
          (first tests)
          (rest tests)))

(def ^:private alternation-split-rule
  {:pred (fn [node]
           (and (regex-node? node)
                (let [[_ body] (split-ci (second (last node)))]
                  (boolean
                    (when body
                      (when-let [branches (top-level-alternatives body)]
                        (and (> (count branches) 1)
                             (every? terminal-branch? branches))))))))
   :apply (fn [node]
            (let [[ci? body] (split-ci (second (last node)))
                  prefix (if ci? "(?i)" "")
                  branches (top-level-alternatives body)]
              ;; The wrapping :boolean_test is what keeps this rewrite composable.
              ;; binary-expr->pred has a 1-arity pass-through, so a :boolean_test with a
              ;; single child transforms to just that child's predicate -- the node tag
              ;; survives for any enclosing rule, and zip/next descends into the new
              ;; children, so the terminal rules above rewrite each branch in this pass.
              [:boolean_test
               (or-tree (for [b branches]
                          [:boolean_test
                           (second node)
                           [:REGEX_OPERATOR "==~"]
                           [:re_expression (str prefix b)]]))]))})

;;;; A group wrapping the entire pattern carries no meaning once there are no
;;;; backreferences, and removing it lets the alternation split see the `|`s as
;;;; top-level: `(master|release)` becomes `master|release`.

(defn- self-contained-group?
  "True when body is a single group spanning the whole pattern -- `(a|b)` and not
   `(a)(b)`, which would lose the concatenation if the outer parens were dropped."
  [body]
  (and (string/starts-with? body "(")
       (string/ends-with? body ")")
       (not (re-find #"\\[1-9]" body))
       (let [inner (subs body 1 (dec (count body)))
             n (count inner)]
         (and (pos? n)
              (loop [i 0 depth 0]
                (cond
                  (>= i n) (zero? depth)
                  (= \\ (.charAt ^String inner i)) (recur (+ i 2) depth)
                  (= \( (.charAt ^String inner i)) (recur (inc i) (inc depth))
                  (= \) (.charAt ^String inner i)) (if (zero? depth)
                                                     false
                                                     (recur (inc i) (dec depth)))
                  :else (recur (inc i) depth)))))))

(def ^:private strip-outer-group-rule
  {:pred (fn [node]
           (and (regex-node? node)
                (let [[_ body] (split-ci (second (last node)))]
                  (boolean (and body (self-contained-group? body))))))
   :apply (fn [node]
            (let [[ci? body] (split-ci (second (last node)))]
              [:boolean_test
               (second node)
               [:REGEX_OPERATOR "==~"]
               [:re_expression (str (if ci? "(?i)" "")
                                    (subs body 1 (dec (count body))))]]))})

;;;; Uniform alternations. When every branch of an alternation is a bare literal, every
;;;; branch really does reduce to the same operator, so the whole pattern collapses to one
;;;; list operator instead of the chain of ORs the general split would produce. Literals
;;;; adjacent to the group distribute into each term: `pre(a|b)post` is
;;;; equals-any(preapost, prebpost).
;;;;
;;;; Anything these decline -- in particular any alternation whose branches have differing
;;;; shapes -- falls through to alternation-split-rule, which is the only correct treatment
;;;; there. Collapsing a mixed alternation into one list operator is a real bug:
;;;; `.*Error.*|.*Fatal` as contains-any(Error, Fatal) accepts "xFatalY", which the regex
;;;; rejects.

(defn- alt-rule
  "Builds a rule for `<lead><prefix>(a|b|...)<suffix><trail>`, where lead/trail are the
   fixed `.*` decorations that select the operator and prefix/suffix are optional
   literals that distribute into every term."
  [lead trail plain-op ci-op]
  (let [shape (re-pattern (str "\\^?" lead "(" word-src ")?\\((" word-src
                               "(?:\\|" word-src ")*)\\)(" word-src ")?" trail "\\$?"))]
    {:pred (fn [node]
             (and (regex-node? node)
                  (let [[_ body] (split-ci (second (last node)))]
                    (boolean (and body (re-matches shape body))))))
     :apply (fn [node]
              (let [[ci? body] (split-ci (second (last node)))
                    [_ prefix alts suffix] (re-matches shape body)
                    prefix (unescape (or prefix ""))
                    suffix (unescape (or suffix ""))
                    words (map #(str prefix (unescape %) suffix)
                               (scan-alternatives alts))
                    fold? (and ci? (boolean (some #(re-find #"[A-Za-z]" %) words)))]
                [:boolean_test
                 (second node)
                 [:BINARY_OPERATOR (if fold? ci-op plain-op)]
                 words]))}))

;;;; The paren-free form, `a|b|c`. strip-outer-group-rule has already unwrapped
;;;; `(a|b|c)` by the time this runs, so this covers both.
(def ^:private bare-multi-equals-rule
  ;; The whole alternation is a capture group rather than something recovered by trimming
  ;; `^`/`$` off the ends: a pattern may legitimately *end* in an escaped `\$`, and a
  ;; trailing-`$` string replace would eat that dollar and leave a dangling backslash in
  ;; the term. Reading the group off the match cannot make that mistake, because the
  ;; literal class already decided which dollar is a metacharacter.
  (let [shape (re-pattern (str "\\^?((?:" word-src ")(?:\\|(?:" word-src "))+)\\$?"))]
    {:pred (fn [node]
             (and (regex-node? node)
                  (let [[_ body] (split-ci (second (last node)))]
                    (boolean (and body (re-matches shape body))))))
     :apply (fn [node]
              (let [[ci? body] (split-ci (second (last node)))
                    words (map unescape (scan-alternatives (second (re-matches shape body))))
                    fold? (and ci? (boolean (some #(re-find #"[A-Za-z]" %) words)))]
                [:boolean_test
                 (second node)
                 [:BINARY_OPERATOR (if fold? "==i" "==*")]
                 words]))}))

;;;; ---------------------------------------------------------------------------------
;;;; Rule order is significant. `reduce` threads one location through every rule in
;;;; turn, so a rule sees whatever its predecessors produced:
;;;;
;;;;   strip-outer-group  runs first so the multi-term rules and the split see the
;;;;                      unwrapped pattern.
;;;;   terminals          run before the multi-term rules, so a single-literal pattern
;;;;                      becomes `==` rather than a one-element `==*`.
;;;;   multi-term rules   take the uniform alternations, which they express as one list
;;;;                      operator.
;;;;   alternation-split  is the fallback for every other alternation, and cannot loop:
;;;;                      its output branches contain no top-level `|`.
;;;;
;;;; A rule that has already fired leaves a :BINARY_OPERATOR behind, so every later
;;;; predicate declines it via regex-node?.
;;;; ---------------------------------------------------------------------------------

(def optimization-rules
  [strip-outer-group-rule

   (terminal-rule equals-re   "==" "==i")
   (terminal-rule starts-re   "startsWith" "startsWithi")
   (terminal-rule ends-re     "endsWith" "endsWithi")
   (terminal-rule contains-re "==+" "==+i")

   bare-multi-equals-rule
   (alt-rule ""       ""       "==*" "==i")
   (alt-rule "\\.\\*" "\\.\\*" "==+" "==+i")
   (alt-rule ""       "\\.\\*" "startsWith" "startsWithi")
   (alt-rule "\\.\\*" ""       "endsWith" "endsWithi")

   alternation-split-rule])

(defn- apply-rules
  [loc rules]
  (reduce apply-rule loc rules))

(defn run
  [loc]
  (if (zip/end? loc)
    (zip/root loc)
    (recur (zip/next (apply-rules loc optimization-rules)))))
