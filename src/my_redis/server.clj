(ns my-redis.server
  (:require [clojure.java.io :as io]
            [my-redis.aof :as aof]
            [my-redis.db :as db]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.executor :as executor]
            [my-redis.expiry :as expiry]
            [my-redis.resp :as resp])
  (:import [java.net ServerSocket Socket SocketException]
           [java.io BufferedInputStream BufferedOutputStream EOFException]))

(defn- serve-connection!
  [server-state ^Socket sock]
  (try
    (.setTcpNoDelay sock true)
    (swap! server-state update :connections conj sock)
    (let [in (BufferedInputStream. (.getInputStream sock))
          out (BufferedOutputStream. (.getOutputStream sock))
          ex (:executor @server-state)]
      (loop []
        (let [cmd (resp/read-reply in)
              reply (executor/submit! ex cmd)]
          (cond
            (= reply :no-reply)
            (recur)

            (= reply :quit)
            (do (resp/write-reply! out (resp/simple "OK"))
                (.flush out))

            :else
            (do (resp/write-reply! out reply)
                (.flush out)
                (recur))))))
    (catch EOFException _ nil) ; クライアントが切断。正常終了
    (catch SocketException _ nil) ; 接続が切れた。正常終了
    (catch IllegalStateException _ nil) ; executor 停止後の submit!
    (catch Exception e
      (println "[server] connection error:" (.getMessage e)))
    (finally
      (swap! server-state update :connections disj sock)
      (try
        (.close sock)
        (catch Exception _ nil)))))

(defn start!
  ([port] (start! port {}))
  ([port {:keys [expire-interval-ms dir appendonly appendfsync verbose?]
          :or {expire-interval-ms 100 appendonly false appendfsync :everysec verbose? true}}]
   (let [socket (ServerSocket. port)
         actual-port (.getLocalPort socket)
         keyspace (db/create)
         cfg (config/create)
         aof-handle (when appendonly
                      (aof/open! (io/file (or dir ".") "appendonly.aof") appendfsync))
         ctx {:db keyspace
              :config cfg
              :aof aof-handle
              :stats (atom {:commands 0})}
         ex (executor/start! (fn [cmd] (command/dispatch ctx cmd)))
         cycler (expiry/start! (fn [] (executor/submit! ex [:expire-cycle]))
                               {:interval-ms expire-interval-ms})
         state (atom {:socket socket
                      :port actual-port
                      :running? true
                      :connections #{}
                      :db keyspace
                      :executor ex
                      :expiry cycler
                      :verbose? verbose?
                      :config cfg
                      :aof aof-handle})
         log (fn [& args] (when verbose? (apply println args)))]
     (future
       (try
         (log (format "[server] listening on %d" actual-port))
         (loop []
           (let [sock (.accept socket)]
             (future (serve-connection! state sock))
             (recur)))
         (catch SocketException e
           (if (:running? @state)
             (log "[server] accept error:" (.getMessage e))
             (log "[server] stopped")))
         (catch Exception e
           (log "[server] fatal:" (.getMessage e)))))
     state)))

(defn stop!
  [state]
  (swap! state assoc :running? false)
  (let [{:keys [^ServerSocket socket connections executor expiry aof verbose?]} @state
        log (fn [& args] (when verbose? (apply println args)))]
    (expiry/stop! expiry)
    (doseq [^Socket c connections]
      (try
        (.close c)
        (catch Exception _ nil)))
    (try
      (.close socket)
      (catch Exception _ nil))
    (executor/stop! executor)
    (when aof (aof/close! aof))
    (log "[server] stop requested"))
  nil)
