(ns my-redis.server-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.resp :as resp]
            [my-redis.server :as server])
  (:import [java.net Socket SocketException ConnectException]
           [java.io BufferedInputStream BufferedOutputStream]))

;; ---------- テスト用クライアント ----------

(defn- connect
  "サーバに接続し、{:socket :in :out} を返す。"
  [port]
  (let [sock (Socket. "localhost" (int port))]
    {:socket sock
     :in  (BufferedInputStream. (.getInputStream sock))
     :out (BufferedOutputStream. (.getOutputStream sock))}))

(defn- send-cmd!
  "コマンドを送って応答を読む。接続は開いたまま。"
  [{:keys [in out]} cmd]
  (resp/write-reply! out cmd)
  (.flush out)
  (resp/read-reply in))

(defn- disconnect!
  [{:keys [^Socket socket]}]
  (.close socket))

(defn- one-shot
  "接続してコマンドを1つ送り、応答を読んで切断する。"
  [port cmd]
  (let [conn (connect port)]
    (try
      (send-cmd! conn cmd)
      (finally (disconnect! conn)))))

(defmacro with-server
  "テスト用サーバを起動し、body を実行して必ず停止する。
   port には OS が割り当てた実際のポート番号が束縛される。"
  [[port] & body]
  `(let [srv# (server/start! 0)
         ~port (:port @srv#)]
     (try
       ~@body
       (finally (server/stop! srv#)))))

;; ---------- テスト ----------

(deftest ping-returns-pong
  (with-server [port]
    (is (= "PONG" (one-shot port ["PING"])))))

(deftest ping-with-argument-echoes
  (with-server [port]
    (is (= "hello" (one-shot port ["PING" "hello"])))))

(deftest echo-returns-argument
  (with-server [port]
    (is (= "world" (one-shot port ["ECHO" "world"])))))

(deftest echo-with-wrong-arity-returns-error
  (with-server [port]
    (let [r (one-shot port ["ECHO"])]
      (is (resp/error? r))
      (is (= "ERR wrong number of arguments for 'echo' command" (:message r))))))

(deftest unknown-command-returns-error
  (with-server [port]
    (let [r (one-shot port ["NOSUCHCMD"])]
      (is (resp/error? r))
      (is (= "ERR unknown command 'NOSUCHCMD'" (:message r))))))

(deftest command-is-case-insensitive
  (with-server [port]
    (is (= "PONG" (one-shot port ["ping"])))
    (is (= "PONG" (one-shot port ["PiNg"])))))

(deftest connection-persists-across-commands
  (testing "1つの接続で複数コマンドを続けて打てる"
    (with-server [port]
      (let [conn (connect port)]
        (try
          (is (= "PONG" (send-cmd! conn ["PING"])))
          (is (= "a" (send-cmd! conn ["ECHO" "a"])))
          (is (= "b" (send-cmd! conn ["ECHO" "b"])))
          (is (= "PONG" (send-cmd! conn ["PING"])))
          (finally (disconnect! conn)))))))

(deftest multiple-connections-work-simultaneously
  (testing "複数接続が同時に動く（accept がブロックしていない）"
    (with-server [port]
      (let [conns (repeatedly 5 #(connect port))]
        (try
          ;; 全接続を開いたまま、交互にコマンドを送る
          (doseq [c conns]
            (is (= "PONG" (send-cmd! c ["PING"]))))
          (doseq [c conns]
            (is (= "x" (send-cmd! c ["ECHO" "x"]))))
          (finally (doseq [c conns] (disconnect! c))))))))

(deftest inline-command-works
  (testing "型記号なしの素のテキストも受け付ける"
    (with-server [port]
      (let [{:keys [^Socket socket in out]} (connect port)]
        (try
          (.write out (.getBytes "PING\r\n" "UTF-8"))
          (.flush out)
          (is (= "PONG" (resp/read-reply in)))
          (finally (.close socket)))))))

(deftest blank-line-is-ignored
  (testing "空行を送っても接続が切れず、次のコマンドが通る"
    (with-server [port]
      (let [{:keys [^Socket socket in out]} (connect port)]
        (try
          (.write out (.getBytes "\r\nPING\r\n" "UTF-8"))
          (.flush out)
          (is (= "PONG" (resp/read-reply in)))
          (finally (.close socket)))))))

(deftest quit-closes-connection
  (with-server [port]
    (let [conn (connect port)]
      (try
        (is (= "OK" (send-cmd! conn ["QUIT"])))
        (testing "QUIT の後はストリームが終端に達している"
          (is (neg? (.read ^BufferedInputStream (:in conn)))))
        (finally (disconnect! conn))))))

(deftest server-survives-abrupt-disconnect
  (testing "クライアントが突然切断してもサーバは生き続ける"
    (with-server [port]
      ;; 応答を読まずに切断する
      (let [conn (connect port)]
        (resp/write-reply! (:out conn) ["PING"])
        (.flush ^BufferedOutputStream (:out conn))
        (disconnect! conn))
      (Thread/sleep 50)
      ;; 別の接続が普通に動く
      (is (= "PONG" (one-shot port ["PING"]))))))

(deftest connections-are-released
  (testing "切断された接続がレジストリから除かれる"
    (let [srv  (server/start! 0)
          port (:port @srv)]
      (try
        (let [conns (repeatedly 3 #(connect port))]
          (doseq [c conns] (send-cmd! c ["PING"]))
          (is (= 3 (count (:connections @srv))))
          (doseq [c conns] (disconnect! c)))
        (Thread/sleep 100)
        (is (zero? (count (:connections @srv))))
        (finally (server/stop! srv))))))

(deftest stop-closes-existing-connections
  (testing "stop! は既存接続も切る"
    (let [srv  (server/start! 0)
          port (:port @srv)
          conn (connect port)]
      (is (= "PONG" (send-cmd! conn ["PING"])))
      (server/stop! srv)
      (Thread/sleep 100)
      (testing "接続が切られている"
        (is (thrown? Exception (send-cmd! conn ["PING"]))))
      (disconnect! conn))))

(deftest stop-refuses-new-connections
  (testing "stop! の後は新規接続を受け付けない"
    (let [srv  (server/start! 0)
          port (:port @srv)]
      (is (= "PONG" (one-shot port ["PING"])))
      (server/stop! srv)
      (Thread/sleep 100)
      (is (thrown? ConnectException (connect port))))))
