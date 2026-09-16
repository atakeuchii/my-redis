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

(deftest set-rejects-options-for-now
  (testing "EX などのオプションは Day 4 まで未対応。黙って無視せずエラーにする"
    (is (= "ERR syntax error"
           (err-msg (run (ctx) "SET" "k" "v" "EX" "10"))))))

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
