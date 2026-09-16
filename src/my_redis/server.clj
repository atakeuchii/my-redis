(ns my-redis.server
  (:require [my-redis.command :as command]
            [my-redis.db :as db]
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
          ctx {:db (:db @server-state)}]
      (loop []
        (let [cmd (resp/read-reply in)
              reply (command/dispatch ctx cmd)]
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
    (catch Exception e
      (println "[server] connection error:" (.getMessage e)))
    (finally
      (swap! server-state update :connections disj sock)
      (try
        (.close sock)
        (catch Exception _ nil)))))

(defn start!
  [port]
  (let [socket (ServerSocket. port)
        actual-port (.getLocalPort socket)
        state (atom {:socket socket
                     :port actual-port
                     :running? true
                     :connections #{}
                     :db (db/create)})]
    (future
      (try
        (println (format "[server] listening on %d" actual-port))
        (loop []
          (let [sock (.accept socket)]
            (future (serve-connection! state sock))
            (recur)))
        (catch SocketException e
          (if (:running? @state)
            (println "[server] accept error:" (.getMessage e))
            (println "[server] stopped")))
        (catch Exception e
          (println "[server] fatal:" (.getMessage e)))))
    state))

(defn stop!
  [state]
  (swap! state assoc :running? false)
  (let [{:keys [^ServerSocket socket connections]} @state]
    (doseq [^Socket c connections]
      (try
        (.close c)
        (catch Exception _ nil)))
    (try
      (.close socket)
      (catch Exception _ nil)))
  (println "[server] stop requested")
  nil)
