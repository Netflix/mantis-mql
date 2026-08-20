;
; Copyright 2022 Netflix, Inc.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;     http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;
(ns io.mantisrx.mql.fnmeta
  "Attaching metadata to a function without paying for it on every call.

   MQL hangs compile time metadata (`:name`, `:as`, `:distinct`, ...) off the
   operand functions it compiles. On the JVM `clojure.core/with-meta` cannot
   attach metadata to a function in place: `clojure.lang.AFunction.withMeta`
   returns an anonymous `clojure.lang.AFunction$1 extends RestFn` whose only
   implementation is

     protected Object doInvoke(Object args) {
       return AFunction.this.applyTo((ISeq) args);
     }

   Every subsequent call to that operand therefore goes
   `invoke(datum)` -> `RestFn.invoke` -> allocate an `ArraySeq` ->
   `doInvoke` -> `AFn.applyTo` -> `AFn.applyToHelper` -> the real function.
   That is five frames and one allocation per operand, per node, per event,
   for metadata which is only ever read while compiling the query.

   `fn-with-meta` attaches the same metadata to a type which implements
   `IFn` directly, so the call is a single virtual dispatch and allocates
   nothing. `meta`, `with-meta` and `fn?` all behave as they did before.")

#?(:clj
   (deftype MetaFn [f m]
     clojure.lang.Fn

     clojure.lang.IObj
     (meta [_] m)
     (withMeta [_ m'] (MetaFn. f m'))

     clojure.lang.IFn
     (invoke [_] (.invoke ^clojure.lang.IFn f))
     (invoke [_ a] (.invoke ^clojure.lang.IFn f a))
     (invoke [_ a b] (.invoke ^clojure.lang.IFn f a b))
     (invoke [_ a b c] (.invoke ^clojure.lang.IFn f a b c))
     (invoke [_ a b c d] (.invoke ^clojure.lang.IFn f a b c d))
     (invoke [_ a b c d e] (.invoke ^clojure.lang.IFn f a b c d e))
     (applyTo [_ args] (clojure.lang.AFn/applyToHelper ^clojure.lang.IFn f args))))

(defn fn-with-meta
  "Returns f carrying the metadata m.

   Equivalent to (with-meta f m) for every observable purpose -- `meta`,
   `with-meta` and `fn?` all agree -- but on the JVM the result is invoked
   through a plain IFn call rather than through the variadic RestFn trampoline
   that clojure.lang.AFunction.withMeta produces. See the namespace docstring.

   f: The function to tag.
   m: The metadata map.

   Returns a function which behaves as f and whose metadata is m."
  [f m]
  #?(:clj  (if (instance? MetaFn f)
             (MetaFn. (.-f ^MetaFn f) m)
             (MetaFn. f m))
     :cljs (with-meta f m)))
