(defproject my-redis "0.1.0-SNAPSHOT"
  :description "A minimal Redis implementation in Clojure"
  :url "https://github.com/atakeuchii/my-redis"
  :dependencies [[org.clojure/clojure "1.12.5"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}}
  :repl-options {:init-ns my-redis.core})
