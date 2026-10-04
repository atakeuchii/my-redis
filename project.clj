(defproject atakeuchii/my-redis (or (System/getenv "RELEASE_VERSION") "0.1.0-SNAPSHOT")
  :description "A minimal Redis implementation in Clojure"
  :url "https://github.com/atakeuchii/my-redis"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]]
  :main my-redis.core
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}
             :uberjar {:aot :all}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default (complement :skip)
                   :skip :skip}
  :repl-options {:init-ns my-redis.core} 
  :deploy-repositories [["github"
                         {:url "https://maven.pkg.github.com/atakeuchii/my-redis"
                          :username :env/github_actor
                          :password :env/github_token
                          :sign-releases false}]])
