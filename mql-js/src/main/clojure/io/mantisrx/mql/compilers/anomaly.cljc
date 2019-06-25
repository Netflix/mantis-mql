(ns io.mantisrx.mql.compilers.anomaly
  "This namespace contains the mql built-in anomaly detectors."
#?(:clj  (:require [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.interop :as rxi])))


#?(:clj (defn mad-anomaly->fn
  "method: A string representing the method, right now only MAD is available.
   prop: A property function for fetching the number out of datum.
   alpha: The learning rate parameter. Controls how quickly our median estimation adapts.
   beta:  The threshold parameter. If the value is above median by (beta * median) we have an anomaly.

   Returns: A function which takes an observables and returns the observable with anomalies bolted on."
  [method prop alpha beta]
  {:anomaly
   (fn [obs]
     (let [alpha (alpha {})
           beta (beta {})]
     (->> obs
          (rx/reductions (fn [acc datum]
                           (if (nil? (:median acc))
                             {:median (prop datum) :datum datum}
                             (if (< (:median acc) (prop datum))
                               {:median (+ (:median acc) alpha) :datum datum}
                               {:median (- (:median acc) alpha) :datum datum})))
                         {:median nil :datum nil})
          (rx/filter #(not (nil? (:datum %))))
          (rx/map (fn [acc]
                    (let
                      [anomalous (< (* beta (:median acc 0))
                                    (prop (:datum acc)))]
                      (assoc (:datum acc) "anomalous" anomalous)))))))}))

#?(:cljs (defn mad-anomaly->fn [method prop alpha beta] {:anomaly (fn [obs] obs)}))
