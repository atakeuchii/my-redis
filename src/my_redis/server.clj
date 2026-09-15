(ns my-redis.server
  (:require [clojure.string :as str]
            [my-redis.resp :as resp])
  (:import [java.net ServerSocket Socket SocketException]
           [java.io BufferedInputStream BufferedOutputStream EOFException]))

(defn- handle-command
  [cmd]
  (when (seq cmd)
    (let [name (str/upper-case (first cmd))]
      (case name
        "PING" (resp/simple "PONG")
        (resp/error (str "ERR unknown command '" (first cmd) "'"))))))

(defn- serve-connection!
  [^Socket sock]
  (try
    (.setTcpNoDelay sock true)
    (let [in (BufferedInputStream. (.getInputStream sock))
          out (BufferedOutputStream. (.getOutputStream sock))]
      (loop []
        (let [cmd (resp/read-reply in)]
          (when-let [reply (handle-command cmd)]
            (resp/write-reply! out reply)
            (.flush out))
          (recur))))
    (catch EOFException _  ; クライアントが切断。正常終了
      nil)
    (catch SocketException _ ; 接続が切れた。正常終了
      nil)
    (catch Exception e
      (println "[server] connection error:" (.getMessage e)))
    (finally
      (try
        (.close sock)
        (catch Exception _ nil)))))

(defn start!
  [port]
  (let [server (ServerSocket. port)]
    (println (format "[server] listening on %d" port))
    (loop []
      (let [sock (.accept server)]
        (future (serve-connection! sock))
        (recur)))))
