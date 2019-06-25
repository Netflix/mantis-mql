(ns io.mantisrx.mql.compilers.core.window
  #?(:clj (:import  rx.Observable
           java.util.concurrent.TimeUnit)))

(defn rx-window
  "An implementation for the missing rxClojure window function.
   
   obs: An observable over which we want to window.
   span: The length of the window in seconds.
   shift: An amount to shift by for sliding windows. (optional)
   
   Returns an Observable of Observables containing the windows."
  ([obs span]
   (.buffer ^rx.Observable obs ^Long span TimeUnit/SECONDS))
  ([obs span shift]
   (.buffer ^rx.Observable obs ^Long span ^Long shift TimeUnit/SECONDS)))

(defn window->fn
  "MQL supports two modes for windowing; tumbling and sliding. Tumbling windows
   are made using a single integer parameter `select * from stream window 10`
   Sliding windows are provided a span (length of the window) and a shift
   which indicates how far to slide `select * from stream window 5 1`

   span: The length of the window in seconds.
   shift: The amount a sliding window should shift by at each step.

   Returns: I'm not sure yet."
  ([span]
   (let [span (span {})]
     {:window (fn [obs] (.buffer ^rx.Observable obs ^Long span TimeUnit/SECONDS))}))
  ([span shift]
   (let [span (span {})
         shift (shift {})]
     {:window (fn [obs] (.buffer ^rx.Observable obs ^Long span ^Long shift TimeUnit/SECONDS))})))

(defn n-window->fn
  "See window->fn."
  [n]
  (let [n (n {})]
    {:window (fn [obs] (.buffer ^rx.Observable obs ^Long n))}))
