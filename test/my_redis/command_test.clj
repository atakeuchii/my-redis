(ns my-redis.command-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.command :as command]
            [my-redis.db :as db]
            [my-redis.resp :as resp]))

(defn- ctx [] {:db (db/create)})

(defn- run
  "コマンドを実行して応答を返す。"
  [c & args]
  (command/dispatch c (vec args)))

(defn- err-msg [reply]
  (when (resp/error? reply) (:message reply)))

;; ---------- ディスパッチ ----------

(deftest unknown-command
  (is (= "ERR unknown command 'NOSUCH'"
         (err-msg (run (ctx) "NOSUCH")))))

(deftest unknown-command-preserves-user-case
  (testing "打たれたままの表記でエラーを返す"
    (is (= "ERR unknown command 'NoSuch'"
           (err-msg (run (ctx) "NoSuch"))))))

(deftest command-name-is-case-insensitive
  (let [c (ctx)]
    (is (= "OK" (:value (run c "set" "k" "v"))))
    (is (= "v" (run c "GET" "k")))
    (is (= "v" (run c "GeT" "k")))))

(deftest keys-are-case-sensitive
  (let [c (ctx)]
    (run c "SET" "k" "lower")
    (run c "SET" "K" "upper")
    (is (= "lower" (run c "GET" "k")))
    (is (= "upper" (run c "GET" "K")))))

(deftest arity-exact
  (testing "GET は arity 2（ちょうど）"
    (is (= "ERR wrong number of arguments for 'get' command"
           (err-msg (run (ctx) "GET"))))
    (is (= "ERR wrong number of arguments for 'get' command"
           (err-msg (run (ctx) "GET" "a" "b"))))))

(deftest arity-minimum
  (testing "DEL は arity -2（最低2個）"
    (is (= "ERR wrong number of arguments for 'del' command"
           (err-msg (run (ctx) "DEL"))))
    (is (= 0 (run (ctx) "DEL" "a")))
    (is (= 0 (run (ctx) "DEL" "a" "b" "c")))))

(deftest empty-command-is-no-reply
  (is (= :no-reply (command/dispatch (ctx) nil)))
  (is (= :no-reply (command/dispatch (ctx) []))))

;; ---------- 文字列 ----------

(deftest set-returns-simple-ok
  (let [r (run (ctx) "SET" "k" "v")]
    (is (instance? my_redis.resp.SimpleString r))
    (is (= "OK" (:value r)))))

(deftest get-missing-returns-nil
  (testing "存在しないキーは Null Bulk String（nil）"
    (is (nil? (run (ctx) "GET" "nope")))))

(deftest set-overwrites
  (let [c (ctx)]
    (run c "SET" "k" "v1")
    (run c "SET" "k" "v2")
    (is (= "v2" (run c "GET" "k")))))

(deftest set-empty-string
  (testing "空文字列は nil と区別される"
    (let [c (ctx)]
      (run c "SET" "k" "")
      (is (= "" (run c "GET" "k")))
      (is (= 1 (run c "EXISTS" "k"))))))

;; ---------- キー全般 ----------

(deftest del-counts-deleted
  (let [c (ctx)]
    (run c "SET" "a" "1")
    (run c "SET" "b" "2")
    (is (= 2 (run c "DEL" "a" "b" "nope")))
    (is (= 0 (run c "DBSIZE")))))

(deftest del-with-duplicate-keys
  (testing "同じキーを複数回指定しても1回だけ数える"
    (let [c (ctx)]
      (run c "SET" "k" "v")
      (is (= 1 (run c "DEL" "k" "k" "k"))))))

(deftest exists-counts-arguments
  (testing "EXISTS は引数の個数を数える（重複も数える）"
    (let [c (ctx)]
      (run c "SET" "k" "v")
      (is (= 1 (run c "EXISTS" "k")))
      (is (= 3 (run c "EXISTS" "k" "k" "k")))
      (is (= 1 (run c "EXISTS" "k" "nope"))))))

(deftest type-returns-simple-string
  (let [c (ctx)]
    (run c "SET" "k" "v")
    (let [r (run c "TYPE" "k")]
      (is (instance? my_redis.resp.SimpleString r))
      (is (= "string" (:value r))))
    (is (= "none" (:value (run c "TYPE" "nope"))))))

;; ---------- KEYS パターン ----------

(deftest keys-star-returns-all
  (let [c (ctx)]
    (run c "SET" "a" "1")
    (run c "SET" "b" "2")
    (is (= #{"a" "b"} (set (run c "KEYS" "*"))))))

(deftest keys-prefix-pattern
  (let [c (ctx)]
    (run c "SET" "user:1" "x")
    (run c "SET" "user:2" "y")
    (run c "SET" "other" "z")
    (is (= #{"user:1" "user:2"} (set (run c "KEYS" "user:*"))))))

(deftest keys-question-mark
  (let [c (ctx)]
    (run c "SET" "ab" "1")
    (run c "SET" "acb" "2")
    (is (= #{"ab"} (set (run c "KEYS" "a?"))))))

(deftest keys-char-class
  (let [c (ctx)]
    (run c "SET" "ka" "1")
    (run c "SET" "kb" "2")
    (run c "SET" "kc" "3")
    (is (= #{"ka" "kb"} (set (run c "KEYS" "k[ab]"))))))

(deftest keys-literal-special-chars
  (testing "パターン外の記号はリテラルとして扱う"
    (let [c (ctx)]
      (run c "SET" "a.b" "1")
      (run c "SET" "axb" "2")
      (is (= #{"a.b"} (set (run c "KEYS" "a.b")))))))

(deftest keys-empty-db
  (is (= [] (run (ctx) "KEYS" "*"))))

;; ---------- 管理 ----------

(deftest flushdb-clears-all
  (let [c (ctx)]
    (run c "SET" "a" "1")
    (run c "SET" "b" "2")
    (is (= "OK" (:value (run c "FLUSHDB"))))
    (is (= 0 (run c "DBSIZE")))))

(deftest ping-and-echo-still-work
  (let [c (ctx)]
    (is (= "PONG" (:value (run c "PING"))))
    (is (= "hi" (run c "PING" "hi")))
    (is (= "hi" (run c "ECHO" "hi")))))

(deftest quit-returns-keyword
  (is (= :quit (run (ctx) "QUIT"))))

;; ---------- INCR 系 ----------

(deftest incr-on-missing-key
  (testing "存在しないキーは 0 として扱う"
    (is (= 1 (run (ctx) "INCR" "fresh")))))

(deftest incr-increments
  (let [c (ctx)]
    (run c "SET" "n" "10")
    (is (= 11 (run c "INCR" "n")))
    (is (= "11" (run c "GET" "n")))))

(deftest decr-decrements
  (let [c (ctx)]
    (run c "SET" "n" "10")
    (is (= 9 (run c "DECR" "n")))))

(deftest incrby-and-decrby
  (let [c (ctx)]
    (run c "SET" "n" "10")
    (is (= 15 (run c "INCRBY" "n" "5")))
    (is (= 5 (run c "DECRBY" "n" "10")))
    (is (= -5 (run c "DECRBY" "n" "10")))))

(deftest incr-on-non-integer
  (let [c (ctx)]
    (run c "SET" "s" "abc")
    (is (= "ERR value is not an integer or out of range"
           (err-msg (run c "INCR" "s"))))
    (testing "値が変わっていない"
      (is (= "abc" (run c "GET" "s"))))))

(deftest incrby-with-non-integer-delta
  (is (= "ERR value is not an integer or out of range"
         (err-msg (run (ctx) "INCRBY" "k" "abc")))))

(deftest incr-overflow
  (testing "オーバーフローは例外ではなくエラー値を返す"
    (let [c (ctx)]
      (run c "SET" "big" (str Long/MAX_VALUE))
      (is (= "ERR increment or decrement would overflow"
             (err-msg (run c "INCR" "big"))))
      (testing "値が変わっていない"
        (is (= (str Long/MAX_VALUE) (run c "GET" "big")))))))

(deftest decr-underflow
  (let [c (ctx)]
    (run c "SET" "small" (str Long/MIN_VALUE))
    (is (= "ERR increment or decrement would overflow"
           (err-msg (run c "DECR" "small"))))))

(deftest incr-is-atomic
  (testing "並行 INCR で1つも失われない"
    (let [c (ctx)
          threads 50
          per-thread 200]
      (run c "SET" "counter" "0")
      (->> (range threads)
           (map (fn [_] (future (dotimes [_ per-thread]
                                  (command/dispatch c ["INCR" "counter"])))))
           doall
           (run! deref))
      (is (= (str (* threads per-thread)) (run c "GET" "counter"))))))

;; ---------- APPEND / STRLEN / GETSET / SETNX ----------

(deftest append-to-missing-key
  (is (= 5 (run (ctx) "APPEND" "k" "hello"))))

(deftest append-accumulates
  (let [c (ctx)]
    (run c "APPEND" "k" "hello")
    (is (= 11 (run c "APPEND" "k" " world")))
    (is (= "hello world" (run c "GET" "k")))))

(deftest strlen-counts-bytes-not-chars
  (testing "マルチバイト文字はバイト数で数える"
    (let [c (ctx)]
      (run c "SET" "j" "あ")
      (is (= 3 (run c "STRLEN" "j"))))))

(deftest append-returns-byte-length
  (let [c (ctx)]
    (is (= 3 (run c "APPEND" "j" "あ")))
    (is (= 3 (run c "STRLEN" "j")))))

(deftest strlen-on-missing-key
  (testing "Integer を返すコマンドなので nil ではなく 0"
    (is (= 0 (run (ctx) "STRLEN" "nope")))))

(deftest getset-returns-old-value
  (let [c (ctx)]
    (run c "SET" "k" "old")
    (is (= "old" (run c "GETSET" "k" "new")))
    (is (= "new" (run c "GET" "k")))))

(deftest getset-on-missing-key
  (let [c (ctx)]
    (is (nil? (run c "GETSET" "k" "v")))
    (is (= "v" (run c "GET" "k")))))

(deftest setnx-only-when-absent
  (let [c (ctx)]
    (is (= 1 (run c "SETNX" "k" "first")))
    (is (= 0 (run c "SETNX" "k" "second")))
    (is (= "first" (run c "GET" "k")))))

;; ---------- MSET / MGET ----------

(deftest mset-and-mget
  (let [c (ctx)]
    (run c "MSET" "a" "1" "b" "2" "c" "3")
    (is (= ["1" "2" "3"] (run c "MGET" "a" "b" "c")))))

(deftest mget-missing-keys-are-nil
  (let [c (ctx)]
    (run c "SET" "a" "1")
    (is (= ["1" nil] (run c "MGET" "a" "nope")))))

(deftest mset-odd-arguments
  (is (= "ERR wrong number of arguments for 'mset' command"
         (err-msg (run (ctx) "MSET" "a" "1" "b")))))

(deftest mset-duplicate-keys-last-wins
  (let [c (ctx)]
    (run c "MSET" "k" "v1" "k" "v2")
    (is (= "v2" (run c "GET" "k")))))

(deftest mset-is-atomic
  (testing "MSET の途中経過が観測されない"
    (let [d (db/create)
          c {:db d}
          observed (atom #{})
          watching (atom true)]
      (let [watcher (future (while @watching (swap! observed conj (db/size d))))]
        (command/dispatch c ["MSET" "a" "1" "b" "2" "c" "3" "d" "4" "e" "5"])
        (Thread/sleep 30)
        (reset! watching false)
        @watcher)
      (is (empty? (disj @observed 0 5))
          (str "中間状態が観測された: " @observed)))))

(deftest mget-reads-consistent-snapshot
  (testing "MGET は一貫したスナップショットを読む"
    (let [c (ctx)
          writing (atom true)
          mismatches (atom 0)]
      (command/dispatch c ["MSET" "a" "0" "b" "0"])
      (let [writer (future
                     (loop [n 1]
                       (when @writing
                         (command/dispatch c ["MSET" "a" (str n) "b" (str n)])
                         (recur (inc n)))))]
        (dotimes [_ 5000]
          (let [[a b] (command/dispatch c ["MGET" "a" "b"])]
            (when (not= a b) (swap! mismatches inc))))
        (reset! writing false)
        @writer)
      (is (zero? @mismatches)))))

;; ---------- SET のオプション ----------

(deftest set-nx-when-absent
  (let [c (ctx)]
    (is (= "OK" (:value (run c "SET" "k" "v" "NX"))))
    (is (= "v" (run c "GET" "k")))))

(deftest set-nx-when-present-returns-nil
  (testing "セットされなかった場合は Null Bulk String"
    (let [c (ctx)]
      (run c "SET" "k" "first")
      (is (nil? (run c "SET" "k" "second" "NX")))
      (is (= "first" (run c "GET" "k"))))))

(deftest set-xx-when-present
  (let [c (ctx)]
    (run c "SET" "k" "first")
    (is (= "OK" (:value (run c "SET" "k" "second" "XX"))))
    (is (= "second" (run c "GET" "k")))))

(deftest set-xx-when-absent-returns-nil
  (let [c (ctx)]
    (is (nil? (run c "SET" "k" "v" "XX")))
    (is (= 0 (run c "EXISTS" "k")))))

(deftest set-stores-expire-at
  (testing "EX は絶対時刻に変換して保存される（まだ期限切れはしない）"
    (let [d (db/create)
          c {:db d}
          before (db/now)]
      (run c "SET" "k" "v" "EX" "10")
      (let [e (db/get-entry d "k")
            exp (:expire-at e)]
        (is (some? exp))
        (is (<= (+ before 10000) exp (+ before 10000 1000)))))))

(deftest set-px-uses-milliseconds
  (let [d (db/create)
        c {:db d}
        before (db/now)]
    (run c "SET" "k" "v" "PX" "5000")
    (is (<= (+ before 5000) (:expire-at (db/get-entry d "k")) (+ before 5000 1000)))))

(deftest set-option-syntax-errors
  (let [c (ctx)]
    (is (= "ERR syntax error" (err-msg (run c "SET" "k" "v" "NX" "XX"))))
    (is (= "ERR syntax error" (err-msg (run c "SET" "k" "v" "EX" "10" "PX" "5000"))))
    (is (= "ERR syntax error" (err-msg (run c "SET" "k" "v" "FOO"))))
    (is (= "ERR syntax error" (err-msg (run c "SET" "k" "v" "EX"))))))

(deftest set-invalid-expire
  (let [c (ctx)]
    (is (= "ERR value is not an integer or out of range"
           (err-msg (run c "SET" "k" "v" "EX" "abc"))))
    (is (= "ERR invalid expire time in 'set' command"
           (err-msg (run c "SET" "k" "v" "EX" "0"))))))

(deftest set-options-are-case-insensitive
  (let [c (ctx)]
    (is (= "OK" (:value (run c "SET" "k" "v" "nx"))))
    (is (nil? (run c "SET" "k" "v2" "nx")))))

(deftest set-nx-with-ex
  (let [d (db/create)
        c {:db d}]
    (run c "SET" "k" "v" "NX" "EX" "10")
    (is (some? (:expire-at (db/get-entry d "k"))))))

;; ---------- ハンドラの例外が接続を落とさない ----------

(deftest handler-exception-becomes-error
  (testing "ハンドラが例外を投げてもエラー値に変換される"
    (with-redefs [command/command-table
                  (assoc command/command-table
                         "BOOM" {:arity 1 :write? false
                                 :handler (fn [_ _] (throw (RuntimeException. "boom")))})]
      (is (= "ERR internal error" (err-msg (run (ctx) "BOOM")))))))
