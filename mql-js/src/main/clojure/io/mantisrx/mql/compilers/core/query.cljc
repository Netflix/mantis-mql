(ns io.mantisrx.mql.compilers.core.query
  (:require
    [rx.lang.clojure.core :as rx]
    [rx.lang.clojure.blocking :as rxb])
  (:import
    rx.Observable)
  )

(defn query->obs
  "The final transformation step for queries.

   select-fn: A function to be mapped over the observable performing the select
   clause.
   obs: The observable on which this query will operate.
   ops: A map of :keyword -> fn (usually) for implementing the various operations.
   aggregate?: A boolean indicating whether or not the query is an aggregate.

   NOTE: We're operating on one of three levels of nesting.
         1 - Observable of datum.
         2 - Obsevable of Observable of datum.
         3 - Observable of Observable of GroupedObservable of datum.

   TODO: This function is rather long, and contains patterns.

   Returns an observable representing the final result of the query."
  [select-fn obs ops aggregate?]
  (let
    [nesting-level (cond
                     (and (:window ops) (:groupby ops)) 2
                     (or (:window ops) (:groupby ops)) 1
                     :default 0)
     where-fn (if (:where ops)
                (fn [obs] (rx/filter (:where ops) obs))
                identity)
     window-fn (if (:window ops)
                 (:window ops)
                 identity)
     group-fn (if (:groupby ops)
                (cond
                  (= 2 nesting-level)
                  (fn [obs] (rx/map (fn [xs] (vals (group-by (:groupby ops) xs))) obs))
                  (= 1 nesting-level)
                  (fn [obs]
                    (->> obs
                         (rxb/into [])
                         (group-by (:groupby ops))
                         vals
                         rx/seq->o
                         ))
                  :default identity)
               identity)
     anomaly-fn (if (:anomaly ops)
                  (cond
                    (= 2 nesting-level)
                    identity ;;;; TODO: Nested anomalies
                    :default
                    (:anomaly ops))
                  identity)
     limit-fn (if (:limit ops)
                (cond
                  (= 2 nesting-level)
                  (fn [obs] (rx/map (fn [window] (rx/take (:limit ops) window)) obs))
                  :default
                  (fn [obs] (rx/take (:limit ops) obs)))
                identity)
     order-comparator (:order-comparator ops)
     order-fn (if (:orderby ops) (cond
                                   (= 2 nesting-level)
                                   (fn [obs] (rx/map (fn [window] (rx/sort-by (:orderby ops) order-comparator window)) obs))
                                   :default
                                   (fn [obs] (rx/sort-by (:orderby ops) order-comparator obs)))
                identity)
     having-fn (if (:having ops)
                 (cond (= 2 nesting-level)
                       (fn [obs] (rx/map (fn [window] (rx/filter (:having ops) window)) obs))
                       :default
                       (fn [obs] (rx/filter (:having ops) obs)))
                 identity)
     rollup-fn (cond (= 2 nesting-level)
                     (fn [obs] (rx/map (fn [window] (rx/seq->o (map select-fn window))) obs))
                     (= 1 nesting-level)
                     (if aggregate?
                       (fn [obs] (rx/map select-fn obs))
                       (fn [obs] (rx/map select-fn obs)))
                     :default
                     (if aggregate?
                       (fn [obs] (rx/return (select-fn (rxb/into [] obs))))
                       (fn [obs] (rx/map select-fn obs))))
     sample-fn (if (:sample ops)
                 (fn [obs] (rx/filter (:sample ops) obs))
                 identity)
     extrapolation-fn (if (:extrapolation ops)
                        (cond (= 2 nesting-level)
                              (fn [obs] (rx/map (fn [window] (rx/map (:extrapolation ops) window)) obs))
                              :default
                              (fn [obs] (rx/map (:extrapolation ops) obs))) 
                        identity)
     flattener (if (and aggregate? (= 2 nesting-level))
                 (partial rx/flatmap #(.toList ^Observable %1))
                 identity)]
    (->> obs
         sample-fn
         where-fn  ; Potential nesting levels: 0
         window-fn ; Potential nesting levels: 0, 1
         group-fn  ; Potential nesting levels: 0, 1, 2 ; **
         rollup-fn ; Potential nesting levels: 0, 1 ; **
         anomaly-fn; Potential nesting levels: 0, 1 ; **
         having-fn ; Potential nesting levels: 0, 1
         order-fn  ; Potential nesting levels: 0, 1
         limit-fn  ; Potential nesting levels: 0, 1
         extrapolation-fn ; Potential nesting levels: 0, 1
         flattener ; Potential nesting levels: 0
         )))


(defn query->observable
  [select-fn obs+ops & more-ops]
  (query->obs select-fn
              (first obs+ops)
              (merge (second obs+ops)
                     (apply merge more-ops))
              false))

(defn agg-query->observable
  [select-fn obs+ops & more-ops]
  (query->obs select-fn
              (first obs+ops)
              (merge (second obs+ops)
                     (apply merge more-ops))
              true))
