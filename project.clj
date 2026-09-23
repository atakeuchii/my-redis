(defproject my-redis "0.1.0-SNAPSHOT"
  :description "A minimal Redis implementation in Clojure"
  :url "https://github.com/atakeuchii/my-redis"
  :dependencies [[org.clojure/clojure "1.12.5"]]
  :main my-redis.core
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}
             :uberjar {:aot :all}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default (complement :skip)
                   :skip :skip}
  :repl-options {:init-ns my-redis.core})
