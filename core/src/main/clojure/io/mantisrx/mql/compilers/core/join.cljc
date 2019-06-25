(ns io.mantisrx.mql.compilers.core.join
  ""
  (:require
    [rx.lang.clojure.core :as rx]
    [rx.lang.clojure.interop :as rxi])
  (:import rx.Observable
           rx.observables.GroupedObservable
           java.util.concurrent.TimeUnit)
  (:gen-class))

(defn- join-obs
  "Joins primary-obs and joining-obs together.
   This currently only supports left inner joins, but others may be an option in the future.

   NOTE: - The order in (merge datum-b datum-a) determines left/right join behavior.
   - The rx/first on the filter forces an inner join

   joining-observable: The observable to be joined to the primary observable.
   seconds           : The number of seconds for which to wait for matching data.
   key-fn            : A key to check for equality between observables.
   primary-obs       : The table being joined onto.

   Returns: An observable implementing the join.
   "
  [joining-observable seconds key-fn primary-obs]
  (rx/flatmap (fn [^GroupedObservable go]
                (let
                  [k (.getKey go)
                   seconds (seconds {})]
                  (.join go
                         (rx/first (rx/filter #(= (key-fn %) k) joining-observable))
                         (rxi/fn* (fn [_] (Observable/interval seconds TimeUnit/SECONDS)))
                         (rxi/fn* (fn [_] (Observable/interval seconds TimeUnit/SECONDS)))
                         (rxi/fn* (fn [datum-a datum-b] (merge datum-b datum-a))) ;; Prefer properties from the source stream.
                         )))
              (.groupBy ^Observable primary-obs (rxi/fn* key-fn))))

(defn join->fn
  [joining-observable seconds key-fn]
  (fn [obs] (join-obs joining-observable seconds key-fn obs)))
