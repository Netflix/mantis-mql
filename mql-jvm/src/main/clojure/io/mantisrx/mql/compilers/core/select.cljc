(ns io.mantisrx.mql.compilers.core.select
  ""
  (:require
    [rx.lang.clojure.core :as rx]
    [io.mantisrx.mql.util :as util])
  )

#?(:clj (defn- map-assoc-in!
  "Associates a value in a nested TreeMap using mutable behavior.
   This function mimics the behavior of assoc-in and will create
   new nested TreeMaps as necessary if nested levels do not exist."
  {:static true}
  [m [k & ks] v]
  (if (not (nil? v))
    (if ks
      (.put ^java.util.Map m k (map-assoc-in! (get m k (java.util.TreeMap.)) ks v))
      (.put ^java.util.Map m k v))
    m)
  m))

(defn asterisk->fn
  [clause]
  (with-meta identity {:name "(*)"}))

(defn- select-element->value
  [select-element element]
  (cond
    (fn? select-element) (select-element element)  
    (number? select-element) select-element
    (string? select-element) select-element))

(defn- select-element->name
  [select-element]
  (cond
    (fn? select-element)
    (get (meta select-element) :name)
    :else
    [(str select-element)]))

(defn- operand->name-value-pairs
  [datum operand]
  (if (= (get (meta operand) :type) :starts-with)
    (operand datum)
    [{:name (select-element->name operand)
      :value (select-element->value operand datum)}]))

(defn select-list->fn
  "Now that projections are a list of {:name :value} pairs we can make it such that many are returned.
   The elements in select-list is a list of operand: property, string_literal, number, boolean_literal, math_expr, not_operand, null 

   This removes any select elements which have been subsumed by others."
  [& select-list]
  (let
    [names (->> select-list
                (map select-element->name)
                (map vec)
                util/simplify-select-list
                set)
     select-list (filter (fn [e] (some
                                   #{(vec (select-element->name e))}
                                   names))
                         select-list)]
    (fn [element] 
      (let [projections (flatten (map (partial operand->name-value-pairs element) select-list))]
        (reduce
          #?(:clj #(map-assoc-in! %1 (:name %2) (:value %2))
             :cljs #(assoc-in %1 (:name %2) (:value %2)))
          #?(:clj (java.util.TreeMap.)
             :cljs {})
          (filter #((complement nil?) (:value %)) projections))))))

;;;;
;;;; Aggregate Select
;;;;

(defn agg-op->selector+aggregator
  ([aggregator selector]
   (seq [selector aggregator]))
  ([aggregator selector as]
   (seq [selector (with-meta aggregator {:as as})])))

(defn percentile
  [rank xs]
  (let
    [idx (->> xs
              count
              (* rank)
              Math/ceil
              int
              (max 0)
              (min (count xs))
              )]
    (nth (sort xs) idx)))

;;; Functions which can be used like (apply fn [args...])
(def agg-funcs
  {
   "count" (fn [& xs] (count xs))
   "sum" +
   "min" (fn [& xs]
           (if (empty? xs)
             nil
             (apply min xs)))
   "max" (fn [& xs]
           (if (empty? xs)
             nil
             (apply max xs)))
   "average" (fn [& xs]
               (if (empty? xs) nil
                 (/ (apply + xs) (count xs))))
   "list" (fn [& xs]
            (identity xs))
   "p99" (fn [& xs]
           (percentile 0.99 xs))
   "p995" (fn [& xs]
            (percentile 0.995 xs))
   "p90" (fn [& xs]
           (percentile 0.90 xs))
   "p75" (fn [& xs]
           (percentile 0.75 xs))
   "p50" (fn [& xs]
           (percentile 0.5 xs))
   "p25" (fn [& xs]
           (percentile 0.25 xs))
   "p10" (fn [& xs]
           (percentile 0.1 xs))
   "p5" (fn [& xs]
          (percentile 0.05 xs))
   "p1" (fn [& xs]
          (percentile 0.01 xs))
   "first" (fn [& xs] (first xs)) ;; Used to fetch non-agg properties.
   })

(defn agg-func->fn
  [agg-func]
  (with-meta (get agg-funcs (clojure.string/lower-case agg-func))
             {:name (clojure.string/upper-case agg-func)}))

(defn selector-aggregator->name
  [selector aggregator]
  (let
    [agg-name (get (meta aggregator) :name)
     as-name (get (meta aggregator) :as)
     prop-name (clojure.string/join "." (select-element->name selector))]
    (cond
      (not (nil? as-name))
      as-name
      (nil? agg-name)
      prop-name
      :else
      (str agg-name "(" prop-name ")"))))

(defn selector-aggregator-obs->val
  [selector aggregator obs]
  (let
    [distinct-op
     (if (:distinct (meta selector))
       #?(:clj rx/distinct)
       identity)
     prop-name (selector-aggregator->name selector aggregator)
     data
     (->> obs
          (rx/map selector)
          (rx/filter some?)
          (rx/into [])
          (distinct-op)
          (rx/map (fn [data] {prop-name (apply aggregator data)}))
          )]
    data))

(defn selector-aggregator-list->val
  [selector aggregator xs]
  (let
    [distinct-op
     (if (:distinct (meta selector))
       distinct
       identity)
     prop-name (selector-aggregator->name selector aggregator)]
    {prop-name
     (->> xs
          (map selector)
          (filter some?)
          (distinct-op)
          (apply aggregator)
          )}))

(defn- literal->selector
  "Converts a literal into a 'selector' for aggregate selection.
   When mapped over the target collection literals will just return
   themselves."
  [x]
  (if (or (string? x) (number? x))
    (with-meta (fn [_] x) {:name [x]})
    x))

(defn agg-select-list->fn
  ""
  [& agg-list]
  (let
    [agg-list (map #(if (seq? %) % [(literal->selector %) (agg-funcs "first")]) agg-list)]
    (fn [xs]
      (java.util.TreeMap. ^java.util.Map
                          (reduce merge {} 
                                  (map
                                    (fn [[selector aggregator]] (selector-aggregator-list->val selector aggregator xs))
                                    agg-list))))))
