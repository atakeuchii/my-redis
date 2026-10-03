(ns my-redis.pubsub-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.db :as db]
            [my-redis.pubsub :as pubsub]
            [my-redis.resp :as resp]
            [my-redis.server :as server])
  (:import [java.net Socket]
           [java.io BufferedInputStream BufferedOutputStream]))

;; ---------- テスト用クライアント ----------

(defrecord TestClient [id received]
  pubsub/IClient
  (send-push! [_ payload] (swap! received conj payload))
  (client-id [_] id))

(defn- test-client [id] (->TestClient id (atom [])))
(defn- received [c] @(:received c))

(defn- ctx []
  {:db (db/create) :config (config/create)
   :pubsub (pubsub/create) :stats (atom {:commands 0})})

(defn- run
  ([c & args] (command/dispatch c (vec args))))

(defn- run-as [c client & args]
  (command/dispatch c (vec args) client))

(defn- replies
  "[:multi [...]] から応答のベクタを取り出す。"
  [reply]
  (if (and (vector? reply) (= :multi (first reply))) (second reply) [reply]))

;; ---------- レジストリ単体 ----------

(deftest registry-maintains-both-indexes
  (let [reg (pubsub/create)
        c1  (test-client "c1")
        c2  (test-client "c2")]
    (is (= 1 (pubsub/subscribe! reg c1 "news")))
    (is (= 2 (pubsub/subscribe! reg c1 "sports")))
    (is (= 1 (pubsub/subscribe! reg c2 "news")))

    (is (= #{"c1" "c2"} (set (map pubsub/client-id (pubsub/subscribers-of reg "news")))))
    (is (= #{"news" "sports"} (pubsub/channels-of reg c1)))
    (is (= 2 (pubsub/channel-count reg)))
    (is (pubsub/subscribed? reg c1))))

(deftest unsubscribe-removes-from-both-indexes
  (let [reg (pubsub/create)
        c1  (test-client "c1")]
    (pubsub/subscribe! reg c1 "news")
    (pubsub/subscribe! reg c1 "sports")
    (is (= 1 (pubsub/unsubscribe! reg c1 "news")))
    (is (empty? (pubsub/subscribers-of reg "news")) "配信側の索引から消えた")
    (is (= #{"sports"} (pubsub/channels-of reg c1)) "逆引きからも消えた")
    (testing "購読者がいなくなったチャンネルはレジストリから消える"
      (is (= 1 (pubsub/channel-count reg))))))

(deftest unsubscribe-all-cleans-everything
  (let [reg (pubsub/create)
        c1  (test-client "c1")
        c2  (test-client "c2")]
    (pubsub/subscribe! reg c1 "a")
    (pubsub/subscribe! reg c1 "b")
    (pubsub/subscribe! reg c2 "a")
    (is (= #{"a" "b"} (set (pubsub/unsubscribe-all! reg c1))))
    (is (false? (pubsub/subscribed? reg c1)))
    (is (= 1 (pubsub/channel-count reg)) "c2 が残る a だけ")
    (is (nil? (get (:subscribed @reg) "c1")) "c1 の逆引きが消えた")))

(deftest unsubscribe-by-id-works-without-the-client-object
  (testing "切断時は id だけで掃除できる（クライアントの等価性に依存しない）"
    (let [reg (pubsub/create)
          c1  (test-client "c1")]
      (pubsub/subscribe! reg c1 "news")
      (is (= ["news"] (pubsub/unsubscribe-all-by-id! reg "c1")))
      (is (zero? (pubsub/channel-count reg))))))

(deftest subscribe-is-idempotent
  (let [reg (pubsub/create)
        c1  (test-client "c1")]
    (pubsub/subscribe! reg c1 "news")
    (is (= 1 (pubsub/subscribe! reg c1 "news")) "同じチャンネルを二重購読しない")
    (is (= 1 (count (pubsub/subscribers-of reg "news"))))))

;; ---------- コマンド ----------

(deftest subscribe-returns-three-element-reply
  (let [c  (ctx)
        c1 (test-client "c1")]
    (is (= [["subscribe" "news" 1]] (replies (run-as c c1 "SUBSCRIBE" "news"))))
    (is (= [["subscribe" "sports" 2]] (replies (run-as c c1 "SUBSCRIBE" "sports"))))))

(deftest subscribe-multiple-channels-returns-one-reply-each
  (let [c  (ctx)
        c1 (test-client "c1")]
    (is (= [["subscribe" "a" 1] ["subscribe" "b" 2] ["subscribe" "c" 3]]
           (replies (run-as c c1 "SUBSCRIBE" "a" "b" "c"))))))

(deftest unsubscribe-specific-channel
  (let [c  (ctx)
        c1 (test-client "c1")]
    (run-as c c1 "SUBSCRIBE" "a" "b")
    (is (= [["unsubscribe" "a" 1]] (replies (run-as c c1 "UNSUBSCRIBE" "a"))))))

(deftest unsubscribe-without-args-removes-all
  (let [c  (ctx)
        c1 (test-client "c1")]
    (run-as c c1 "SUBSCRIBE" "a" "b")
    (let [rs (replies (run-as c c1 "UNSUBSCRIBE"))]
      (is (= 2 (count rs)))
      (is (= #{"a" "b"} (set (map second rs))))
      (is (zero? (last (last rs))) "最後の応答で残り 0"))))

(deftest publish-delivers-to-subscribers
  (let [c  (ctx)
        c1 (test-client "c1")
        c2 (test-client "c2")
        c3 (test-client "c3")]
    (run-as c c1 "SUBSCRIBE" "news")
    (run-as c c2 "SUBSCRIBE" "news")
    (run-as c c3 "SUBSCRIBE" "sports")

    (is (= 2 (run c "PUBLISH" "news" "hello")))
    (is (= [["message" "news" "hello"]] (received c1)))
    (is (= [["message" "news" "hello"]] (received c2)))
    (is (= [] (received c3)) "別チャンネルには届かない")))

(deftest publish-to-nobody-returns-zero
  (let [c (ctx)]
    (is (= 0 (run c "PUBLISH" "news" "lost")))))

(deftest published-message-is-not-stored
  (testing "購読者がいないときのメッセージは消える（配信保証がない）"
    (let [c  (ctx)
          c1 (test-client "c1")]
      (run c "PUBLISH" "news" "lost")
      (run-as c c1 "SUBSCRIBE" "news")
      (is (= [] (received c1)) "後から購読しても届かない"))))

(deftest unsubscribed-client-stops-receiving
  (let [c  (ctx)
        c1 (test-client "c1")]
    (run-as c c1 "SUBSCRIBE" "news")
    (run c "PUBLISH" "news" "first")
    (run-as c c1 "UNSUBSCRIBE" "news")
    (run c "PUBLISH" "news" "second")
    (is (= [["message" "news" "first"]] (received c1)))))

(deftest publish-continues-when-one-delivery-fails
  (testing "配信に失敗する購読者がいても、他への配信は続く"
    (let [c   (ctx)
          bad (reify pubsub/IClient
                (send-push! [_ _] (throw (RuntimeException. "broken pipe")))
                (client-id [_] "bad"))
          ok  (test-client "ok")]
      (pubsub/subscribe! (:pubsub c) bad "news")
      (pubsub/subscribe! (:pubsub c) ok "news")
      (is (= 2 (run c "PUBLISH" "news" "x")) "配信を試みた数を返す")
      (is (= [["message" "news" "x"]] (received ok))))))

;; ---------- 購読モード ----------

(deftest subscribe-mode-restricts-commands
  (let [c  (ctx)
        c1 (test-client "c1")]
    (run-as c c1 "SUBSCRIBE" "news")
    (testing "通常のコマンドは拒否される"
      (let [r (run-as c c1 "GET" "k")]
        (is (resp/error? r))
        (is (re-find #"only" (:message r)))))
    (testing "許可されたコマンドは通る"
      (is (= "PONG" (:value (run-as c c1 "PING"))))
      (is (= [["subscribe" "sports" 2]] (replies (run-as c c1 "SUBSCRIBE" "sports"))))
      (is (= :quit (run-as c c1 "QUIT"))))))

(deftest subscribe-mode-ends-after-unsubscribing-all
  (let [c  (ctx)
        c1 (test-client "c1")]
    (run-as c c1 "SUBSCRIBE" "news")
    (is (resp/error? (run-as c c1 "GET" "k")))
    (run-as c c1 "UNSUBSCRIBE")
    (is (nil? (run-as c c1 "GET" "k")) "通常モードに戻る")))

(deftest other-clients-are-not-restricted
  (testing "購読モードは接続ごと。他の接続は影響を受けない"
    (let [c  (ctx)
          c1 (test-client "c1")
          c2 (test-client "c2")]
      (run-as c c1 "SUBSCRIBE" "news")
      (is (= "OK" (:value (run-as c c2 "SET" "k" "v"))))
      (is (= "v" (run-as c c2 "GET" "k"))))))

(deftest publishing-client-is-not-in-subscribe-mode
  (let [c  (ctx)
        c1 (test-client "c1")
        c2 (test-client "c2")]
    (run-as c c1 "SUBSCRIBE" "news")
    (is (= 1 (run-as c c2 "PUBLISH" "news" "x")) "購読していない接続は PUBLISH できる")))

;; ---------- PUBSUB 内観 ----------

(deftest pubsub-channels-and-numsub
  (let [c  (ctx)
        c1 (test-client "c1")
        c2 (test-client "c2")]
    (run-as c c1 "SUBSCRIBE" "news" "sports")
    (run-as c c2 "SUBSCRIBE" "news")
    (is (= #{"news" "sports"} (set (run c "PUBSUB" "CHANNELS"))))
    (is (= #{"news"} (set (run c "PUBSUB" "CHANNELS" "new*"))))
    (is (= ["news" 2 "sports" 1] (run c "PUBSUB" "NUMSUB" "news" "sports")))
    (is (= ["nochan" 0] (run c "PUBSUB" "NUMSUB" "nochan")))))

(deftest pubsub-channels-empty-when-nobody-subscribes
  (is (= [] (run (ctx) "PUBSUB" "CHANNELS"))))

;; ---------- 実際のソケット経由 ----------

(defn- connect [port]
  (let [sock (doto (Socket. "localhost" (int port)) (.setSoTimeout 3000))]
    {:socket sock
     :in  (BufferedInputStream. (.getInputStream sock))
     :out (BufferedOutputStream. (.getOutputStream sock))}))

(defn- send! [{:keys [in ^BufferedOutputStream out]} cmd]
  (resp/write-reply! out (vec cmd))
  (.flush out)
  (resp/read-reply in))

(defn- read-next [{:keys [in]}] (resp/read-reply in))
(defn- close! [{:keys [^Socket socket]}] (.close socket))

(deftest end-to-end-pubsub-over-sockets
  (let [srv  (server/start! 0 {:appendonly false :verbose? false})
        port (:port @srv)
        sub  (connect port)
        pub  (connect port)]
    (try
      (is (= ["subscribe" "news" 1] (send! sub ["SUBSCRIBE" "news"])))
      (is (= 1 (send! pub ["PUBLISH" "news" "hello"])))
      (is (= ["message" "news" "hello"] (read-next sub))
          "購読者のソケットにメッセージが届く")

      (testing "購読中でも PING は通る"
        (resp/write-reply! (:out sub) ["PING"])
        (.flush ^BufferedOutputStream (:out sub))
        (is (= "PONG" (read-next sub))))

      (testing "購読中は通常のコマンドが拒否される"
        (resp/write-reply! (:out sub) ["GET" "k"])
        (.flush ^BufferedOutputStream (:out sub))
        (let [r (read-next sub)]
          (is (resp/error? r))))
      (finally
        (close! sub) (close! pub) (server/stop! srv)))))

(deftest disconnect-cleans-up-registry
  (let [srv  (server/start! 0 {:appendonly false :verbose? false})
        port (:port @srv)]
    (try
      (let [sub (connect port)]
        (send! sub ["SUBSCRIBE" "news"])
        (is (= 1 (pubsub/channel-count (:pubsub @srv))))
        (close! sub))
      ;; 接続スレッドが finally を実行するのを待つ
      (loop [n 0]
        (when (and (pos? (pubsub/channel-count (:pubsub @srv))) (< n 100))
          (Thread/sleep 20)
          (recur (inc n))))
      (is (zero? (pubsub/channel-count (:pubsub @srv)))
          "切断でレジストリが掃除される")
      (is (= {} (:subscribed @(:pubsub @srv))))
      (finally (server/stop! srv)))))

(deftest multiple-subscribers-over-sockets
  (let [srv  (server/start! 0 {:appendonly false :verbose? false})
        port (:port @srv)
        s1   (connect port)
        s2   (connect port)
        pub  (connect port)]
    (try
      (send! s1 ["SUBSCRIBE" "news"])
      (send! s2 ["SUBSCRIBE" "news"])
      (is (= 2 (send! pub ["PUBLISH" "news" "x"])))
      (is (= ["message" "news" "x"] (read-next s1)))
      (is (= ["message" "news" "x"] (read-next s2)))
      (finally (close! s1) (close! s2) (close! pub) (server/stop! srv)))))
