(ns io.mantisrx.mql.compilers.core.sampling
  (:require
    [clojure.string :as string]
    #?(:clj [clojure.data.json :as json])))

(defn- abs
  ^long [x]
  (if (neg? x) (- x) x))

(defn- hasher
  ^long [^Object x ^long salt]
  (abs (hash (str x salt))))

(defn default-sampler
  "A tautology function of datum -> true."
  [datum]
  true)

(defn random-sampler
  "Implementation for the random sampler, this specific implementation using
   the hard coded 2^32 is to maintain ClojureScript compatibility. The design
   has been ported from mantis-query (the old query language) to maintain
   identical behavior. The random sampler is not deterministic, and consequently
   not referentially transparent.

   factor: A uniform random number will be generated on the interval [0 factor).
   threshold: datum is sampled if said uniform random number is below threshold.
   datum: A map of String -> Object representing the data to be sampled.

   Sampling criteria:
   x ~ uniform [0 factor)
   x <= threshold

   1% random sampling:  (random-sampler 100 10000)
   2% random sampling:  (random-sampler 200 10000)
   2% random sampling:  (random-sampler 100 5000)
   10% random sampling: (random-sampler 10 100)
   25% random sampling: (random-sampler 2500 10000)

   Returns true if the datum should be sampled, false otherwise."
  [factor threshold datum]
  (< (mod (rand-int 2147483647) factor) threshold))

(defn sticky-sampler
  "A sticky sampler will 'stick' to certain values for the specified keys.
   As an example if use esn as a key then esn=NFANDROID-FOO is sampled then
   the stream will always sample data for which esn=NFANDROID-FOO. This is
   determined using the hash of all the values concatenated as a string. Unlike
   the random sampler the sticky sampler is is deterministic and referentially
   transparent.

   This is useful in use cases such as sessionization in which we want to
   process only a sample of the stream, but unlike random sampling we want
   to ensure we see all data sampled matches the criteria.

   As with the random sampler the factor and threshold parameters can be used
   to control the volume of data. See (doc random-sampler) for more details.

   ks: A seq of keys at the top level of datum used for stickiness.
   salt: A salt value to be added to the hash of the key values.
   factor: A uniform random number will be generated on the interval [0 factor)
   threshold: datum is sampled if said uniform random number is below threshold.
   datum: A map of String -> Object representing the data to be sampled.

   Returns true if the datum should be sampled, false otherwise."
  [ks salt factor threshold datum]
  (let
    [str-to-hash (apply str (map #(get datum %) ks))]
    (cond
      (= ^String str-to-hash "") false
      (< (mod (hasher str-to-hash salt) factor) threshold) true
      :else false)))

(defn ->timer [] (java.util.Timer.))

(defn fixed-rate
  ([f per] (fixed-rate f (->timer) 0 per))
  ([f timer per] (fixed-rate f timer 0 per))
  ([f timer dlay per]
    (let [tt (proxy [java.util.TimerTask] [] (run [] (f)))]
      (.scheduleAtFixedRate timer tt dlay per)
      #(.cancel tt))))

(defn rps-sampler
  "A sampler which allows a set number of elements through each second."
  [rps]
  (let
    [cnt (atom 0)
     t (->timer)
     task (fixed-rate #(reset! cnt 0) t 1000)]
    (fn [datum] (if
                  (< @cnt rps)
                  (do (swap! cnt inc) true)
                  false))))

(defn- parse-json
  "A cross platform json parsing function which defers to host specific
   parsers.

   string: A string representing valid JSON.

   Returns nested maps and vectors representing the parsed data."
  [string]
  #?(:clj (json/read-str string)
     :cljs (js->clj (.parse js/JSON string))))

(def default-factor 10000)

(defn sample-config->extrapolation-factor
  [config]
  (let
    [threshold (get config "threshold" default-factor)
     factor (get config "factor" default-factor)]
    (double (/ factor threshold))))

(defn- extrapolatable?
  [x]
  (or (string/starts-with? x "SUM(")
      (string/starts-with? x "COUNT(")))

(defn sample-config->extrapolation-fn
  [raw-config]
  (let [config (parse-json (clojure.string/replace raw-config "'" "\""))
        factor (sample-config->extrapolation-factor config)]
    (if (get config "extrapolate" false)
      (fn [datum]
        (let
          [names (filter extrapolatable? (keys datum))]
          (java.util.TreeMap. 
            ^java.util.Map (apply (partial merge (into {} datum))
                                  (map (fn [name] {name (* factor (get datum name))}) names)))))
      identity)))

(defn percent-sample-config->sampler
  "Converts a percentile sampler ie. 2% 0.5% 0.1 % to a function
   which can then be used to determine sampling eligibility by applying to
   data. Intended for use with filter functions and behaves like a predicate.

   n: The percent to sample. 2% for two percent and 99.5% for ninety-nine point five."
  [n]
  {:sample (partial random-sampler (* 100 n) n)
   :extrapolation identity})


(defn sample-config->sampler
  "Converts a json sampler configuration into a function which can then
   be used to determine sampling eligibility by applying to data. Intended
   for use with the filter function and behaves like a predicate.

   raw-config: A JSON string representing the configuration for the sampler.
   This function will replace single quotes with double quotes first.

   Returns a function of datum -> boolean indicating eligibility for sampling
   in which datum is a map of String -> Object."
  [raw-config]
  (let
    [config (parse-json (clojure.string/replace raw-config "'" "\""))
     strategy (get config "strategy" :default)
     threshold (get config "threshold" default-factor)
     factor (get config "factor" default-factor)
     ks (get config "keys" [])
     salt (get config "salt" 0)
     extrapolate? (get config "extrapolate" false)
     ]
    {:sample (cond
               (>= threshold factor)
               default-sampler
               (= strategy "RANDOM")
               (partial random-sampler factor threshold)
               (= strategy "STICKY")
               (partial sticky-sampler ks salt factor threshold)
               (= strategy "RPS")
               (rps-sampler threshold)
               :else default-sampler)
     :extrapolation (if extrapolate?
                      (sample-config->extrapolation-fn config)
                      identity)
     }))
