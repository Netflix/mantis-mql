(ns io.mantisrx.mql.compilers.core.groupby)

(defn group-by->fn
  "Transforms group-by to a function.

   Usage: (group-by->fn prop1 prop2... propn)

   props: One or more properties functions as returned by prop->fn.

   Returns a fn of datum -> [obj1, obj2... objn] which can be used as the fn
   parameter for rx's groupBy. rxClojure groupBy does not function as
   expected [0] and thus it is recommended to utilize this as such:
   (.groupBy ^rx.Observable obs (rxi/fn* fn)).

   [0] https://github.com/ReactiveX/RxClojure/blob/0.x/src/main/clojure/rx/lang/clojure/core.clj#L478"
  [& props]
  {:groupby
   (fn [datum] (map #(% datum) props))})
