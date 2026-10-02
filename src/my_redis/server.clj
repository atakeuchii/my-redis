(ns my-redis.server
  (:require [clojure.java.io :as io]
            [my-redis.aof :as aof]
            [my-redis.db :as db]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.executor :as executor]
            [my-redis.expiry :as expiry]
            [my-redis.rewrite :as rewrite]
            [my-redis.resp :as resp])
  (:import [java.net ServerSocket Socket SocketException]
           [java.io BufferedInputStream BufferedOutputStream EOFException File]))

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

(defn- load-aof!
  [^File file ctx log]
  (when (.exists file)
    (let [replay-ctx (assoc ctx :replaying? true :aof-ref nil)
          start (System/nanoTime)
          [applied good] (aof/replay! file
                                      (fn [cmd]
                                        (let [r (command/dispatch replay-ctx cmd)]
                                          (when (resp/error? r)
                                            (println "[aof] replay error:" (:message r)
                                                     "cmd:" (pr-str cmd))))))
          ms (/ (- (System/nanoTime) start) 1e6)]
      (log (format "[aof] replayed %d commands in %.1f ms" applied ms))
      (aof/truncate! file good))))

(defn start!
  ([port] (start! port {}))
  ([port {:keys [expire-interval-ms dir appendonly appendfsync verbose?]
          :or {expire-interval-ms 100 appendonly false appendfsync :everysec verbose? true}}]
   (let [log (fn [& args] (when verbose? (apply println args)))
         socket (ServerSocket. port)
         actual-port (.getLocalPort socket)
         keyspace (db/create)
         cfg (config/create)
         ^File aof-file (io/file (or dir ".") "appendonly.aof")
         aof-base-size (atom (if appendonly (.length aof-file) 0))
         base-ctx {:db keyspace :config cfg :stats (atom {:commands 0})}
         _ (when appendonly (load-aof! aof-file base-ctx log))
         aof-ref (atom (when appendonly (aof/open! aof-file appendfsync)))
         rewriting? (atom false)
         ;; executor を先に宣言できないので、実行関数を後から差し込む
         ex-ref (atom nil)
         start-rw (fn [prepared]
                    (future
                      (try
                        (let [built (rewrite/build! prepared aof-file)
                              r     (executor/submit! @ex-ref [:commit-rewrite built])]
                          (log (format "[aof] rewrite done: %d commands, %d -> %d bytes (copied %d)"
                                       (:commands built) (:old-size r)
                                       (:new-size r) (:copied-bytes built))))
                        (catch Exception e
                          (println "[aof] rewrite failed:" (.getMessage e)))
                        (finally (reset! rewriting? false)))))
         ctx (assoc base-ctx
                    :aof-ref aof-ref
                    :aof-file aof-file
                    :appendfsync appendfsync
                    :aof-base-size aof-base-size
                    :rewrite-running? rewriting?
                    :start-rewrite start-rw)
         ex (executor/start! (fn [cmd] (command/dispatch ctx cmd)))
         _ (reset! ex-ref ex)
         cycler (expiry/start! (fn [] (executor/submit! ex [:expire-cycle]))
                               {:interval-ms expire-interval-ms})
         state (atom {:socket socket
                      :port actual-port
                      :running? true
                      :connections #{}
                      :db keyspace
                      :config cfg
                      :ctx ctx
                      :aof-ref aof-ref
                      :aof-base-size aof-base-size
                      :executor ex
                      :expiry cycler})]
     (future
       (try
         (log (format "[server] listening on %d" actual-port))
         (loop []
           (let [sock (.accept socket)]
             (future (serve-connection! state sock))
             (recur)))
         (catch SocketException e
           (if (:running? @state)
             (println "[server] accept error:" (.getMessage e))
             (log "[server] stopped")))
         (catch Exception e
           (println "[server] fatal:" (.getMessage e)))))
     state)))

(defn stop! [state]
  (swap! state assoc :running? false)
  (let [{:keys [^ServerSocket socket connections executor expiry aof-ref]} @state]
    (expiry/stop! expiry)
    (doseq [^Socket c connections]
      (try
        (.close c)
        (catch Exception _ nil)))
    (try
      (.close socket)
      (catch Exception _ nil))
    (executor/stop! executor)
    (when-let [a @aof-ref] (aof/close! a)))
  (println "[server] stop requested")
  nil)
