(ns io.mantisrx.mql.jvm.compilers.orderby)

(defn order->fn
  "Transforms a property to a function for use in sorting.

   Usage: (order->fn property)

   prop: A property function as returned by prop->fn.
   direction: ASC or DESC for ascending or descending sort.

   Returns a comparator function intended to be used with rx/sort to sort
   an observable. It can however be used anywhere a comparator would be."
  ([prop]
   (order->fn prop "asc"))
  ([prop direction]
   {:orderby prop
    :order-comparator (if (= "desc" (clojure.string/lower-case direction))
                        (comparator >)
                        compare)}))
