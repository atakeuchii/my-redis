(ns my-redis.core
  (:require [my-redis.server :as server])
  (:gen-class))

(defn -main
  "Main entry point for lein run commands"
  [& args]
  (case (first args)
    "server" (let [port (Integer/parseInt (or (System/getenv "PORT") "6380"))]
               (println "Starting my-redis server...")
               (server/start! port))
    (println "Usage: PORT=xxxx lein run server")))
