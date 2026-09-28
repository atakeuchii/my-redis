(ns my-redis.aof-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.aof :as aof]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.db :as db]
            [my-redis.executor :as executor]
            [my-redis.resp :as resp]
            [my-redis.server :as server])
  (:import [java.io File FileOutputStream BufferedInputStream FileInputStream]))

(defn- temp-file ^File []
  (let [f (File/createTempFile "myredis" ".aof")]
    (.delete f)
    f))

(defn- read-all
  "AOF に記録されたコマンドを全部読む。"
  [^File f]
  (if-not (.exists f)
    []
    (with-open [in (BufferedInputStream. (FileInputStream. f))]
      (loop [acc []]
        (let [cmd (try (resp/read-reply in) (catch java.io.EOFException _ nil))]
          (if cmd (recur (conj acc cmd)) acc))))))

(defn- ctx-with-aof
  "AOF 付きの ctx と、AOF ハンドル・ファイルを返す。"
  ([] (ctx-with-aof (temp-file)))
  ([^File f]
   (let [a (aof/open! f :always)]
     [{:db (db/create) :config (config/create) :aof a
       :stats (atom {:commands 0})}
      a f])))

(defn- run [c & args] (command/dispatch c (vec args)))

;; ---------- 書き込みの基本 ----------

(deftest writes-are-recorded-in-resp
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v")
    (run c "INCR" "counter")
    (aof/close! a)
    (is (= [["SET" "k" "v"] ["INCR" "counter"]] (read-all f)))))

(deftest reads-are-not-recorded
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v")
    (run c "GET" "k")
    (run c "EXISTS" "k")
    (run c "TTL" "k")
    (run c "KEYS" "*")
    (run c "DBSIZE")
    (aof/close! a)
    (is (= [["SET" "k" "v"]] (read-all f)))))

(deftest no-op-writes-are-not-recorded
  (testing "状態が変わらなかった書き込みは記録しない"
    (let [[c a f] (ctx-with-aof)]
      (run c "SET" "k" "v" "NX")        ; 成功
      (run c "SET" "k" "other" "NX")    ; nil を返す
      (run c "SET" "nokey" "v" "XX")    ; nil を返す
      (run c "RPOP" "nolist")           ; nil を返す
      (aof/close! a)
      (is (= [["SET" "k" "v"]] (read-all f))))))

(deftest errors-are-not-recorded
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "s" "abc")
    (run c "INCR" "s")                  ; エラー
    (run c "LPUSH" "s" "x")             ; WRONGTYPE
    (aof/close! a)
    (is (= [["SET" "s" "abc"]] (read-all f)))))

;; ---------- コマンドの書き換え ----------

(deftest spop-is-recorded-as-srem
  (testing "ランダム性を除くため、実際に消えた要素を SREM として記録する"
    (let [[c a f] (ctx-with-aof)]
      (run c "SADD" "s" "x" "y" "z")
      (let [popped (run c "SPOP" "s")]
        (aof/close! a)
        (let [cmds (read-all f)]
          (is (= 2 (count cmds)))
          (is (= ["SREM" "s" popped] (second cmds))))))))

(deftest expire-is-recorded-as-absolute-time
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v")
    (run c "EXPIRE" "k" "100")
    (aof/close! a)
    (let [[_ exp-cmd] (read-all f)]
      (is (= "PEXPIREAT" (first exp-cmd)))
      (is (= "k" (second exp-cmd)))
      (let [t (Long/parseLong (nth exp-cmd 2))]
        (is (< (System/currentTimeMillis) t (+ (System/currentTimeMillis) 101000)))))))

(deftest set-with-ex-is-split
  (testing "SET k v EX n は SET + PEXPIREAT に分かれる"
    (let [[c a f] (ctx-with-aof)]
      (run c "SET" "k" "v" "EX" "100")
      (aof/close! a)
      (let [cmds (read-all f)]
        (is (= 2 (count cmds)))
        (is (= ["SET" "k" "v"] (first cmds)))
        (is (= "PEXPIREAT" (ffirst (rest cmds))))))))

(deftest set-without-ttl-is-single-command
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v")
    (aof/close! a)
    (is (= [["SET" "k" "v"]] (read-all f)))))

(deftest set-nx-options-are-stripped
  (testing "NX/XX は実行時に判定済みなので記録しない"
    (let [[c a f] (ctx-with-aof)]
      (run c "SET" "k" "v" "NX")
      (aof/close! a)
      (is (= [["SET" "k" "v"]] (read-all f))))))

;; ---------- サーバ自身の削除 ----------

(deftest passive-expiration-is-recorded
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v" "PX" "50")
    (Thread/sleep 100)
    (run c "GET" "k")                   ; 受動的期限切れで削除される
    (aof/close! a)
    (let [cmds (read-all f)]
      (is (some #(= ["DEL" "k"] %) cmds) "削除が記録されている"))))

(deftest active-expiration-is-recorded
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v" "PX" "50")
    (Thread/sleep 100)
    (command/dispatch c [:expire-cycle])
    (aof/close! a)
    (is (some #(= ["DEL" "k"] %) (read-all f)))))

(deftest eviction-is-recorded
  (let [[c a f] (ctx-with-aof)]
    (run c "CONFIG" "SET" "maxkeys" "3")
    (run c "CONFIG" "SET" "maxmemory-policy" "allkeys-random")
    (dotimes [i 5] (run c "SET" (str "k" i) "v"))
    (aof/close! a)
    (let [cmds (read-all f)
          dels (filter #(= "DEL" (first %)) cmds)]
      (is (= 2 (count dels)) "2 件が捨てられ、それが記録されている"))))

;; ---------- リプレイ ----------

(defn- replay-into
  "AOF をリプレイして新しい ctx を作る。"
  [^File f]
  (let [c {:db (db/create) :config (config/create)
           :replaying? true :aof nil :stats (atom {:commands 0})}
        [applied good] (aof/replay! f (fn [cmd] (command/dispatch c cmd)))]
    [c applied good]))

(deftest replay-restores-all-types
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "s" "v")
    (run c "RPUSH" "l" "a" "b" "c")
    (run c "HSET" "h" "f1" "v1" "f2" "v2")
    (run c "SADD" "st" "x" "y")
    (run c "ZADD" "z" "1" "a" "2" "b")
    (aof/close! a)

    (let [[c2] (replay-into f)]
      (is (= "v" (run c2 "GET" "s")))
      (is (= ["a" "b" "c"] (run c2 "LRANGE" "l" "0" "-1")))
      (is (= #{["f1" "v1"] ["f2" "v2"]} (set (partition 2 (run c2 "HGETALL" "h")))))
      (is (= #{"x" "y"} (set (run c2 "SMEMBERS" "st"))))
      (is (= ["a" "b"] (run c2 "ZRANGE" "z" "0" "-1")))
      (is (= 5 (run c2 "DBSIZE"))))))

(deftest replay-preserves-ttl-without-extending
  (testing "PEXPIREAT で記録しているので、リプレイで期限が延びない"
    (let [[c a f] (ctx-with-aof)]
      (run c "SET" "k" "v" "EX" "100")
      (aof/close! a)
      (Thread/sleep 1100)                    ; 1 秒経過させる
      (let [[c2] (replay-into f)
            ttl (run c2 "TTL" "k")]
        (is (<= 98 ttl 99) (str "TTL が " ttl " になった（延びていないこと）"))))))

(deftest replay-does-not-resurrect-expired-keys
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "k" "v" "PX" "50")
    (Thread/sleep 100)
    (run c "GET" "k")                        ; 削除され、DEL が記録される
    (aof/close! a)
    (let [[c2] (replay-into f)]
      (is (nil? (run c2 "GET" "k")))
      (is (= 0 (run c2 "EXISTS" "k"))))))

(deftest replay-does-not-rewrite-aof
  (testing "リプレイ中は AOF に追記しない"
    (let [[c a f] (ctx-with-aof)]
      (run c "SET" "k" "v")
      (aof/close! a)
      (let [before (.length f)]
        (replay-into f)
        (is (= before (.length f)))))))

(deftest replay-of-missing-file
  (let [f (temp-file)
        [_ applied] (replay-into f)]
    (is (zero? applied))))

;; ---------- 末尾破損 ----------

(deftest replay-survives-truncated-tail
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "a" "1")
    (run c "SET" "b" "2")
    (aof/close! a)

    ;; 書き込み途中で落ちた状況を再現
    (with-open [o (FileOutputStream. f true)]
      (.write o (.getBytes "*3\r\n$3\r\nSET\r\n$5\r\nbroke" "UTF-8")))

    (let [[c2 applied good] (replay-into f)]
      (is (= 2 applied) "健全な 2 件が適用された")
      (is (= "1" (run c2 "GET" "a")))
      (is (= "2" (run c2 "GET" "b")))
      (testing "切り捨てると正常なファイルに戻る"
        (aof/truncate! f good)
        (is (= [["SET" "a" "1"] ["SET" "b" "2"]] (read-all f)))))))

(deftest append-after-truncate-works
  (let [[c a f] (ctx-with-aof)]
    (run c "SET" "a" "1")
    (aof/close! a)
    (with-open [o (FileOutputStream. f true)]
      (.write o (.getBytes "*2\r\n$3\r\nGE" "UTF-8")))

    (let [[_ _ good] (replay-into f)]
      (aof/truncate! f good))

    ;; 切り捨て後に追記できる
    (let [a2 (aof/open! f :always)
          c2 {:db (db/create) :config (config/create) :aof a2
              :stats (atom {:commands 0})}]
      (run c2 "SET" "b" "2")
      (aof/close! a2)
      (is (= [["SET" "a" "1"] ["SET" "b" "2"]] (read-all f))))))

;; ---------- fsync モード ----------

(deftest all-fsync-modes-write-data
  (doseq [mode [:always :everysec :no]]
    (testing (str "fsync-mode " mode)
      (let [f (temp-file)
            a (aof/open! f mode)]
        (aof/append! a ["SET" "k" "v"])
        (is (pos? (aof/file-size a)) "flush されて OS に渡っている")
        (aof/close! a)
        (is (= [["SET" "k" "v"]] (read-all f)))))))

;; ---------- サーバ経由の往復 ----------

(deftest server-restart-preserves-data
  (let [dir (doto (File/createTempFile "myredis-srv" "") (.delete) (.mkdir))]
    (try
      (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                  :appendfsync :always :verbose? false})
            ex (:executor @srv)]
        (executor/submit! ex ["SET" "k" "v"])
        (executor/submit! ex ["RPUSH" "l" "a" "b"])
        (executor/submit! ex ["SET" "t" "v" "EX" "1000"])
        (server/stop! srv))

      (let [srv2 (server/start! 0 {:dir (str dir) :appendonly true
                                   :appendfsync :always :verbose? false})
            ex2 (:executor @srv2)]
        (try
          (is (= "v" (executor/submit! ex2 ["GET" "k"])))
          (is (= ["a" "b"] (executor/submit! ex2 ["LRANGE" "l" "0" "-1"])))
          (is (<= 999 (executor/submit! ex2 ["TTL" "t"]) 1000))
          (is (= 3 (executor/submit! ex2 ["DBSIZE"])))
          (finally (server/stop! srv2))))
      (finally
        (doseq [f (reverse (file-seq dir))] (.delete f))))))
