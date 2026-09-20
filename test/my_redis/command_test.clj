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

;; ---------- List: 基本 ----------

(deftest rpush-appends-in-order
  (let [c (ctx)]
    (is (= 3 (run c "RPUSH" "l" "a" "b" "c")))
    (is (= ["a" "b" "c"] (run c "LRANGE" "l" "0" "-1")))))

(deftest lpush-reverses-argument-order
  (testing "LPUSH は引数を順に先頭へ押し込むので逆順になる"
    (let [c (ctx)]
      (is (= 3 (run c "LPUSH" "l" "a" "b" "c")))
      (is (= ["c" "b" "a"] (run c "LRANGE" "l" "0" "-1"))))))

(deftest push-returns-length-after
  (let [c (ctx)]
    (is (= 1 (run c "RPUSH" "l" "a")))
    (is (= 3 (run c "RPUSH" "l" "b" "c")))
    (is (= 4 (run c "LPUSH" "l" "z")))))

(deftest llen-counts
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (= 3 (run c "LLEN" "l")))))

;; ---------- List: 不在キー ----------

(deftest list-commands-on-missing-key
  (testing "返り値の型が不在時の挙動を決める"
    (let [c (ctx)]
      (is (= 0 (run c "LLEN" "nokey")))
      (is (= [] (run c "LRANGE" "nokey" "0" "-1")))
      (is (nil? (run c "LPOP" "nokey")))
      (is (nil? (run c "RPOP" "nokey")))
      (is (nil? (run c "LINDEX" "nokey" "0"))))))

(deftest pop-on-missing-key-does-not-create-it
  (let [c (ctx)]
    (run c "LPOP" "nokey")
    (is (= 0 (run c "EXISTS" "nokey")))))

;; ---------- List: 両端操作 ----------

(deftest pop-from-both-ends
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (= "a" (run c "LPOP" "l")))
    (is (= "c" (run c "RPOP" "l")))
    (is (= ["b"] (run c "LRANGE" "l" "0" "-1")))))

(deftest alternating-pops-across-rebalance
  (testing "front/back のリバランスを跨いでも順序が保たれる"
    (let [c (ctx)]
      (run c "RPUSH" "q" "1" "2" "3" "4" "5" "6")
      (is (= "1" (run c "LPOP" "q")))
      (is (= "6" (run c "RPOP" "q")))
      (is (= "2" (run c "LPOP" "q")))
      (is (= "5" (run c "RPOP" "q")))
      (is (= ["3" "4"] (run c "LRANGE" "q" "0" "-1"))))))

(deftest drain-from-left
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c" "d" "e")
    (is (= ["a" "b" "c" "d" "e"]
           (mapv (fn [_] (run c "LPOP" "l")) (range 5))))
    (is (= 0 (run c "EXISTS" "l")))))

(deftest drain-from-right
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c" "d" "e")
    (is (= ["e" "d" "c" "b" "a"]
           (mapv (fn [_] (run c "RPOP" "l")) (range 5))))
    (is (= 0 (run c "EXISTS" "l")))))

(deftest push-after-drain
  (testing "空になって消えたキーに再度 push できる"
    (let [c (ctx)]
      (run c "RPUSH" "l" "a")
      (run c "LPOP" "l")
      (is (= 1 (run c "RPUSH" "l" "new")))
      (is (= ["new"] (run c "LRANGE" "l" "0" "-1"))))))

(deftest empty-list-deletes-key
  (let [c (ctx)]
    (run c "RPUSH" "l" "only")
    (run c "LPOP" "l")
    (is (= 0 (run c "EXISTS" "l")))
    (is (= "none" (:value (run c "TYPE" "l"))))))

;; ---------- List: LRANGE の境界 ----------

(deftest lrange-negative-indices
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c" "d" "e")
    (is (= ["d" "e"] (run c "LRANGE" "l" "-2" "-1")))
    (is (= ["a" "b" "c" "d" "e"] (run c "LRANGE" "l" "0" "-1")))
    (is (= ["c"] (run c "LRANGE" "l" "-3" "-3")))))

(deftest lrange-clamps-out-of-range
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (testing "上限を超えてもエラーにならずクランプされる"
      (is (= ["a" "b" "c"] (run c "LRANGE" "l" "0" "999"))))
    (testing "下限を下回ってもクランプされる"
      (is (= ["a" "b" "c"] (run c "LRANGE" "l" "-999" "999"))))
    (testing "範囲が全く重ならなければ空"
      (is (= [] (run c "LRANGE" "l" "10" "20"))))
    (testing "start > stop なら空"
      (is (= [] (run c "LRANGE" "l" "2" "1"))))))

(deftest lrange-single-element
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (= ["b"] (run c "LRANGE" "l" "1" "1")))))

(deftest lrange-non-integer-index
  (is (= "ERR value is not an integer or out of range"
         (err-msg (run (ctx) "LRANGE" "l" "a" "b")))))

;; ---------- List: LINDEX / LSET ----------

(deftest lindex-positive-and-negative
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (= "a" (run c "LINDEX" "l" "0")))
    (is (= "c" (run c "LINDEX" "l" "2")))
    (is (= "c" (run c "LINDEX" "l" "-1")))
    (is (= "a" (run c "LINDEX" "l" "-3")))))

(deftest lindex-out-of-range-is-nil
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (nil? (run c "LINDEX" "l" "3")))
    (is (nil? (run c "LINDEX" "l" "-4")))))

(deftest lindex-after-rebalance
  (testing "リバランス後もインデックスが正しい"
    (let [c (ctx)]
      (run c "RPUSH" "l" "a" "b" "c" "d" "e" "f")
      (run c "LPOP" "l")
      (is (= "b" (run c "LINDEX" "l" "0")))
      (is (= "f" (run c "LINDEX" "l" "-1")))
      (is (= "d" (run c "LINDEX" "l" "2"))))))

(deftest lset-replaces-element
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (is (= "OK" (:value (run c "LSET" "l" "1" "X"))))
    (is (= ["a" "X" "c"] (run c "LRANGE" "l" "0" "-1")))))

(deftest lset-negative-index
  (let [c (ctx)]
    (run c "RPUSH" "l" "a" "b" "c")
    (run c "LSET" "l" "-1" "Z")
    (is (= ["a" "b" "Z"] (run c "LRANGE" "l" "0" "-1")))))

(deftest lset-out-of-range
  (let [c (ctx)]
    (run c "RPUSH" "l" "a")
    (is (= "ERR index out of range" (err-msg (run c "LSET" "l" "5" "x"))))))

(deftest lset-on-missing-key
  (is (= "ERR no such key" (err-msg (run (ctx) "LSET" "nokey" "0" "x")))))

;; ---------- 型チェック ----------

(deftest wrong-type-string-command-on-list
  (testing "List に String コマンドを打つと WRONGTYPE"
    (let [c (ctx)
          expected "WRONGTYPE Operation against a key holding the wrong kind of value"]
      (run c "RPUSH" "l" "a")
      (is (= expected (err-msg (run c "GET" "l"))))
      (is (= expected (err-msg (run c "APPEND" "l" "x"))))
      (is (= expected (err-msg (run c "STRLEN" "l"))))
      (is (= expected (err-msg (run c "INCR" "l"))))
      (is (= expected (err-msg (run c "GETSET" "l" "x")))))))

(deftest wrong-type-list-command-on-string
  (testing "String に List コマンドを打つと WRONGTYPE"
    (let [c (ctx)
          expected "WRONGTYPE Operation against a key holding the wrong kind of value"]
      (run c "SET" "s" "v")
      (is (= expected (err-msg (run c "RPUSH" "s" "x"))))
      (is (= expected (err-msg (run c "LPUSH" "s" "x"))))
      (is (= expected (err-msg (run c "LPOP" "s"))))
      (is (= expected (err-msg (run c "RPOP" "s"))))
      (is (= expected (err-msg (run c "LLEN" "s"))))
      (is (= expected (err-msg (run c "LRANGE" "s" "0" "-1"))))
      (is (= expected (err-msg (run c "LINDEX" "s" "0"))))
      (is (= expected (err-msg (run c "LSET" "s" "0" "x")))))))

(deftest type-agnostic-commands-work-on-list
  (testing "中身を見ないコマンドは型を問わない"
    (let [c (ctx)]
      (run c "RPUSH" "l" "a")
      (is (= 1 (run c "EXISTS" "l")))
      (is (= "list" (:value (run c "TYPE" "l"))))
      (is (= 0 (run c "SETNX" "l" "x")))
      (is (= 1 (run c "DEL" "l"))))))

(deftest mget-returns-nil-for-wrong-type
  (testing "MGET は型違いでもエラーにせず nil を返す"
    (let [c (ctx)]
      (run c "SET" "s" "v")
      (run c "RPUSH" "l" "a")
      (is (= ["v" nil] (run c "MGET" "s" "l"))))))

(deftest set-overwrites-list
  (testing "SET は型を問わず上書きする"
    (let [c (ctx)]
      (run c "RPUSH" "l" "a")
      (is (= "OK" (:value (run c "SET" "l" "now-a-string"))))
      (is (= "string" (:value (run c "TYPE" "l")))))))

;; ---------- 並行性 ----------

(deftest concurrent-push-loses-nothing
  (testing "並行 RPUSH で要素が失われない"
    (let [c (ctx)
          threads 20
          per-thread 100]
      (->> (range threads)
           (map (fn [t]
                  (future (dotimes [i per-thread]
                            (command/dispatch c ["RPUSH" "l" (str t "-" i)])))))
           doall
           (run! deref))
      (is (= (* threads per-thread) (run c "LLEN" "l"))))))

(deftest concurrent-pop-does-not-duplicate
  (testing "並行 LPOP で同じ要素が2回取れない"
    (let [c (ctx)
          n 2000]
      (dotimes [i n] (command/dispatch c ["RPUSH" "l" (str i)]))
      (let [results (->> (range 20)
                         (map (fn [_]
                                (future
                                  (loop [acc []]
                                    (if-let [v (command/dispatch c ["LPOP" "l"])]
                                      (recur (conj acc v))
                                      acc)))))
                         doall
                         (mapcat deref))]
        (is (= n (count results)))
        (is (= n (count (set results))) "重複した要素がある")))))

;; ---------- Hash ----------

(deftest hset-counts-new-fields-only
  (let [c (ctx)]
    (is (= 2 (run c "HSET" "h" "a" "1" "b" "2")))
    (testing "上書きは新規ではないので 0"
      (is (= 0 (run c "HSET" "h" "a" "9"))))
    (testing "新規と上書きが混在"
      (is (= 1 (run c "HSET" "h" "a" "8" "c" "3"))))))

(deftest hget-and-hgetall
  (let [c (ctx)]
    (run c "HSET" "h" "name" "Aki" "age" "30")
    (is (= "Aki" (run c "HGET" "h" "name")))
    (is (nil? (run c "HGET" "h" "nofield")))
    (testing "HGETALL はフラットな配列（順序は保証しない）"
      (is (= #{["name" "Aki"] ["age" "30"]}
             (set (partition 2 (run c "HGETALL" "h"))))))))

(deftest hkeys-hvals-hlen-hexists
  (let [c (ctx)]
    (run c "HSET" "h" "a" "1" "b" "2")
    (is (= #{"a" "b"} (set (run c "HKEYS" "h"))))
    (is (= #{"1" "2"} (set (run c "HVALS" "h"))))
    (is (= 2 (run c "HLEN" "h")))
    (is (= 1 (run c "HEXISTS" "h" "a")))
    (is (= 0 (run c "HEXISTS" "h" "nofield")))))

(deftest hdel-counts-removed
  (let [c (ctx)]
    (run c "HSET" "h" "a" "1" "b" "2")
    (is (= 2 (run c "HDEL" "h" "a" "b" "nofield")))))

(deftest hdel-duplicate-fields
  (testing "同じフィールドを複数回指定しても1回だけ数える"
    (let [c (ctx)]
      (run c "HSET" "h" "f" "v")
      (is (= 1 (run c "HDEL" "h" "f" "f" "f"))))))

(deftest hash-commands-on-missing-key
  (let [c (ctx)]
    (is (= [] (run c "HGETALL" "nokey")))
    (is (= [] (run c "HKEYS" "nokey")))
    (is (= 0 (run c "HLEN" "nokey")))
    (is (nil? (run c "HGET" "nokey" "f")))
    (is (= 0 (run c "HEXISTS" "nokey" "f")))))

(deftest empty-hash-deletes-key
  (let [c (ctx)]
    (run c "HSET" "h" "f" "v")
    (run c "HDEL" "h" "f")
    (is (= 0 (run c "EXISTS" "h")))))

(deftest hincrby-increments
  (let [c (ctx)]
    (run c "HSET" "h" "n" "10")
    (is (= 15 (run c "HINCRBY" "h" "n" "5")))
    (is (= 5 (run c "HINCRBY" "h" "n" "-10")))))

(deftest hincrby-on-missing-field
  (is (= 10 (run (ctx) "HINCRBY" "h" "fresh" "10"))))

(deftest hincrby-on-non-integer
  (let [c (ctx)]
    (run c "HSET" "h" "s" "abc")
    (is (= "ERR hash value is not an integer" (err-msg (run c "HINCRBY" "h" "s" "1"))))
    (is (= "abc" (run c "HGET" "h" "s")))))

(deftest hincrby-overflow
  (let [c (ctx)]
    (run c "HSET" "h" "big" (str Long/MAX_VALUE))
    (is (= "ERR increment or decrement would overflow"
           (err-msg (run c "HINCRBY" "h" "big" "1"))))))

(deftest hset-odd-arguments
  (is (= "ERR wrong number of arguments for 'hset' command"
         (err-msg (run (ctx) "HSET" "h" "a" "1" "b")))))

;; ---------- Set ----------

(deftest sadd-counts-new-members-only
  (let [c (ctx)]
    (is (= 3 (run c "SADD" "s" "a" "b" "c")))
    (is (= 1 (run c "SADD" "s" "a" "d")))
    (is (= 0 (run c "SADD" "s" "a")))))

(deftest sadd-duplicate-arguments
  (testing "同じメンバーを複数回指定しても1回だけ数える"
    (is (= 1 (run (ctx) "SADD" "s" "x" "x" "x")))))

(deftest smembers-and-scard
  (let [c (ctx)]
    (run c "SADD" "s" "a" "b" "c")
    (is (= #{"a" "b" "c"} (set (run c "SMEMBERS" "s"))))
    (is (= 3 (run c "SCARD" "s")))))

(deftest sismember
  (let [c (ctx)]
    (run c "SADD" "s" "a")
    (is (= 1 (run c "SISMEMBER" "s" "a")))
    (is (= 0 (run c "SISMEMBER" "s" "nope")))
    (is (= 0 (run c "SISMEMBER" "nokey" "a")))))

(deftest srem-counts-removed
  (let [c (ctx)]
    (run c "SADD" "s" "a" "b" "c")
    (is (= 2 (run c "SREM" "s" "a" "b" "nope")))
    (is (= #{"c"} (set (run c "SMEMBERS" "s"))))))

(deftest set-commands-on-missing-key
  (let [c (ctx)]
    (is (= [] (run c "SMEMBERS" "nokey")))
    (is (= 0 (run c "SCARD" "nokey")))
    (is (nil? (run c "SPOP" "nokey")))))

(deftest empty-set-deletes-key
  (let [c (ctx)]
    (run c "SADD" "s" "only")
    (run c "SREM" "s" "only")
    (is (= 0 (run c "EXISTS" "s")))))

(deftest spop-removes-and-returns
  (let [c (ctx)]
    (run c "SADD" "s" "a" "b" "c")
    (let [popped (run c "SPOP" "s")]
      (is (contains? #{"a" "b" "c"} popped))
      (is (= 2 (run c "SCARD" "s")))
      (is (not (contains? (set (run c "SMEMBERS" "s")) popped))))))

(deftest spop-drains-to-empty
  (let [c (ctx)]
    (run c "SADD" "s" "a" "b")
    (run c "SPOP" "s")
    (run c "SPOP" "s")
    (is (= 0 (run c "EXISTS" "s")))))

;; ---------- 集合演算 ----------

(deftest sinter-basic
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b" "c")
    (run c "SADD" "s2" "b" "c" "d")
    (is (= #{"b" "c"} (set (run c "SINTER" "s1" "s2"))))))

(deftest sinter-argument-order-does-not-matter
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b" "c")
    (run c "SADD" "s2" "b" "c" "d")
    (is (= (set (run c "SINTER" "s1" "s2"))
           (set (run c "SINTER" "s2" "s1"))))))

(deftest sinter-three-sets
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b" "c")
    (run c "SADD" "s2" "b" "c" "d")
    (run c "SADD" "s3" "c" "d" "e")
    (is (= #{"c"} (set (run c "SINTER" "s1" "s2" "s3"))))))

(deftest sinter-with-missing-key-is-empty
  (testing "不在キーは空集合なので積は空"
    (let [c (ctx)]
      (run c "SADD" "s1" "a" "b")
      (is (= [] (run c "SINTER" "s1" "nokey"))))))

(deftest sinter-disjoint-sets
  (let [c (ctx)]
    (run c "SADD" "s1" "a")
    (run c "SADD" "s2" "b")
    (is (= [] (run c "SINTER" "s1" "s2")))))

(deftest sunion-basic
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b")
    (run c "SADD" "s2" "b" "c")
    (is (= #{"a" "b" "c"} (set (run c "SUNION" "s1" "s2"))))))

(deftest sunion-with-missing-key
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b")
    (is (= #{"a" "b"} (set (run c "SUNION" "s1" "nokey"))))))

(deftest sdiff-is-order-sensitive
  (testing "SDIFF は引数の順序で結果が変わる"
    (let [c (ctx)]
      (run c "SADD" "s1" "a" "b" "c")
      (run c "SADD" "s2" "b" "c" "d")
      (is (= #{"a"} (set (run c "SDIFF" "s1" "s2"))))
      (is (= #{"d"} (set (run c "SDIFF" "s2" "s1")))))))

(deftest sdiff-with-missing-key
  (let [c (ctx)]
    (run c "SADD" "s1" "a" "b")
    (is (= #{"a" "b"} (set (run c "SDIFF" "s1" "nokey"))))
    (is (= [] (run c "SDIFF" "nokey" "s1")))))

(deftest sinter-picks-smallest-set-first
  (testing "引数の順序によらず性能が変わらない（大きい集合で計測）"
    (let [c (ctx)]
      (command/dispatch c (into ["SADD" "big"] (map str (range 50000))))
      (run c "SADD" "small" "1" "2" "3")
      (let [t1 (let [s (System/nanoTime)]
                 (dotimes [_ 50] (run c "SINTER" "small" "big"))
                 (- (System/nanoTime) s))
            t2 (let [s (System/nanoTime)]
                 (dotimes [_ 50] (run c "SINTER" "big" "small"))
                 (- (System/nanoTime) s))]
        (is (< (/ (max t1 t2) (min t1 t2)) 10)
            (str "引数順で性能が大きく変わる: " (float (/ (max t1 t2) (min t1 t2))) "倍"))))))

;; ---------- 型チェック（Hash/Set） ----------

(deftest wrong-type-across-all-types
  (let [c (ctx)
        expected "WRONGTYPE Operation against a key holding the wrong kind of value"]
    (run c "SET" "s" "v")
    (run c "RPUSH" "l" "a")
    (run c "HSET" "h" "f" "v")
    (run c "SADD" "st" "m")

    (testing "Hash コマンドを他の型に"
      (is (= expected (err-msg (run c "HGET" "s" "f"))))
      (is (= expected (err-msg (run c "HSET" "l" "f" "v"))))
      (is (= expected (err-msg (run c "HGETALL" "st"))))
      (is (= expected (err-msg (run c "HINCRBY" "s" "f" "1")))))

    (testing "Set コマンドを他の型に"
      (is (= expected (err-msg (run c "SADD" "s" "m"))))
      (is (= expected (err-msg (run c "SMEMBERS" "l"))))
      (is (= expected (err-msg (run c "SCARD" "h"))))
      (is (= expected (err-msg (run c "SPOP" "s")))))

    (testing "他の型のコマンドを Hash/Set に"
      (is (= expected (err-msg (run c "GET" "h"))))
      (is (= expected (err-msg (run c "LPUSH" "st" "x"))))
      (is (= expected (err-msg (run c "LRANGE" "h" "0" "-1")))))

    (testing "集合演算で型違いが混ざったら"
      (is (= expected (err-msg (run c "SINTER" "st" "l"))))
      (is (= expected (err-msg (run c "SUNION" "s" "st")))))))

(deftest type-command-reports-all-types
  (let [c (ctx)]
    (run c "SET" "s" "v")
    (run c "RPUSH" "l" "a")
    (run c "HSET" "h" "f" "v")
    (run c "SADD" "st" "m")
    (is (= "string" (:value (run c "TYPE" "s"))))
    (is (= "list"   (:value (run c "TYPE" "l"))))
    (is (= "hash"   (:value (run c "TYPE" "h"))))
    (is (= "set"    (:value (run c "TYPE" "st"))))))

;; ---------- OBJECT ----------

(deftest object-encoding-reports-implementation
  (testing "エンコーディング最適化は未実装なので常に非圧縮側を返す"
    (let [c (ctx)]
      (run c "SET" "s" "v")
      (run c "RPUSH" "l" "a")
      (run c "HSET" "h" "f" "v")
      (run c "SADD" "st" "m")
      (is (= "embstr"    (:value (run c "OBJECT" "ENCODING" "s"))))
      (is (= "quicklist" (:value (run c "OBJECT" "ENCODING" "l"))))
      (is (= "hashtable" (:value (run c "OBJECT" "ENCODING" "h"))))
      (is (= "hashtable" (:value (run c "OBJECT" "ENCODING" "st")))))))

(deftest object-encoding-on-missing-key
  (is (= "ERR no such key" (err-msg (run (ctx) "OBJECT" "ENCODING" "nokey")))))

;; ---------- 並行性 ----------

(deftest concurrent-sadd-loses-nothing
  (let [c (ctx)
        threads 20
        per-thread 100]
    (->> (range threads)
         (map (fn [t] (future (dotimes [i per-thread]
                                (command/dispatch c ["SADD" "s" (str t "-" i)])))))
         doall
         (run! deref))
    (is (= (* threads per-thread) (run c "SCARD" "s")))))

(deftest concurrent-hincrby-is-atomic
  (let [c (ctx)
        threads 20
        per-thread 100]
    (->> (range threads)
         (map (fn [_] (future (dotimes [_ per-thread]
                                (command/dispatch c ["HINCRBY" "h" "n" "1"])))))
         doall
         (run! deref))
    (is (= (str (* threads per-thread)) (run c "HGET" "h" "n")))))

(deftest concurrent-spop-does-not-duplicate
  (let [c (ctx)
        n 1000]
    (command/dispatch c (into ["SADD" "s"] (map str (range n))))
    (let [results (->> (range 10)
                       (map (fn [_]
                              (future
                                (loop [acc []]
                                  (if-let [v (command/dispatch c ["SPOP" "s"])]
                                    (recur (conj acc v))
                                    acc)))))
                       doall
                       (mapcat deref))]
      (is (= n (count results)))
      (is (= n (count (set results))) "重複した要素がある"))))
