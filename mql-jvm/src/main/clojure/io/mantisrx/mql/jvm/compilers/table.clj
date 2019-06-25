(ns io.mantisrx.mql.jvm.compilers.table
  (:require
    [rx.lang.clojure.core :as rx]))

(defn table->observable
  "Converts the TABLE component of the query into a tuple.

   obs: An observable over which this table clause should operate.
   ops A seq of maps of clause -> fn or seq (in the case of window).

   Returns a tuple of [observable map]."
  [obs & ops] [obs (apply merge ops)])

(defn from-clause->observable
  "This function should receive a primary obs, and joining fns."
  [obs & join-fns]
  (reduce (fn [obs f] (f obs)) obs join-fns))

(defn table-list->observable
  [context & subjects]
  (apply rx/merge (map #(get context % (rx/empty)) subjects)))
