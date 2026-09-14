(ns my-redis.resp-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [my-redis.resp :as resp])
  (:import [java.io ByteArrayInputStream]))

(defn- in-stream
  "文字列を InputStream にする。"
  ^ByteArrayInputStream [^String s]
  (ByteArrayInputStream. (.getBytes s "UTF-8")))

(defn- decode
  "RESP 文字列をデコードする。"
  [^String s]
  (resp/read-reply (in-stream s)))

;; ---------- デコード ----------

(deftest decode-simple-string
  (is (= "OK" (decode "+OK\r\n")))
  (is (= "PONG" (decode "+PONG\r\n"))))

(deftest decode-error
  (let [e (decode "-ERR unknown command 'FOO'\r\n")]
    (is (resp/error? e))
    (is (= "ERR unknown command 'FOO'" (:message e)))))

(deftest decode-integer
  (is (= 42 (decode ":42\r\n")))
  (is (= -1 (decode ":-1\r\n")))
  (is (= 0 (decode ":0\r\n"))))

(deftest decode-bulk-string
  (is (= "hello" (decode "$5\r\nhello\r\n")))
  (testing "Null と空文字列は別物"
    (is (nil? (decode "$-1\r\n")))
    (is (= "" (decode "$0\r\n\r\n"))))
  (testing "長さプレフィックスなので中身に CRLF を含められる"
    (is (= "a\r\nb" (decode "$4\r\na\r\nb\r\n"))))
  (testing "マルチバイトは文字数ではなくバイト数"
    (is (= "あ" (decode "$3\r\nあ\r\n")))))

(deftest decode-array
  (is (= ["GET" "k"] (decode "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")))
  (testing "Null Array と空配列は別物"
    (is (nil? (decode "*-1\r\n")))
    (is (= [] (decode "*0\r\n"))))
  (testing "型の混在と入れ子"
    (is (= [1 "foo" [2 3]]
           (decode "*3\r\n:1\r\n$3\r\nfoo\r\n*2\r\n:2\r\n:3\r\n"))))
  (testing "配列内の Null（MGET で不在キーがあった場合）"
    (is (= ["a" nil] (decode "*2\r\n$1\r\na\r\n$-1\r\n")))))

(deftest decode-inline-command
  (is (= ["PING"] (decode "PING\r\n")))
  (is (= ["SET" "k" "v"] (decode "SET k v\r\n")))
  (testing "連続する空白は潰れる"
    (is (= ["SET" "k" "v"] (decode "SET   k    v\r\n"))))
  (testing "空行は nil"
    (is (nil? (decode "\r\n")))
    (is (nil? (decode "   \r\n")))))

(deftest decode-consecutive
  (testing "1つのストリームから続けて読める"
    (let [in (in-stream "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")]
      (is (= ["PING"] (resp/read-reply in)))
      (is (= ["GET" "k"] (resp/read-reply in)))))
  (testing "空行を挟んでも続きが読める"
    (let [in (in-stream "\r\nPING\r\n")]
      (is (nil? (resp/read-reply in)))
      (is (= ["PING"] (resp/read-reply in))))))

(deftest decode-errors
  (testing "途中で切れた入力は EOFException"
    (is (thrown? java.io.EOFException (decode "*2\r\n$3\r\nGET\r\n")))
    (is (thrown? java.io.EOFException (decode "$10\r\nshort\r\n")))))

;; ---------- エンコード ----------

(deftest encode-distinguishes-simple-and-bulk
  (testing "Simple String は + で、素の文字列は Bulk で出る"
    (is (= "+OK\r\n" (resp/encode-str (resp/simple "OK"))))
    (is (= "$2\r\nOK\r\n" (resp/encode-str "OK")))))

(deftest encode-null-and-empty
  (is (= "$-1\r\n" (resp/encode-str nil)))
  (is (= "$0\r\n\r\n" (resp/encode-str ""))))

(deftest encode-integer
  (is (= ":42\r\n" (resp/encode-str 42)))
  (is (= ":-1\r\n" (resp/encode-str -1))))

(deftest encode-error
  (is (= "-ERR x\r\n" (resp/encode-str (resp/error "ERR x")))))

(deftest encode-array
  (is (= "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" (resp/encode-str ["GET" "k"])))
  (is (= "*0\r\n" (resp/encode-str [])))
  (is (= "*2\r\n$1\r\na\r\n$-1\r\n" (resp/encode-str ["a" nil]))))

(deftest encode-length-is-bytes-not-chars
  (testing "マルチバイト文字の長さはバイト数"
    (is (= "$3\r\nあ\r\n" (resp/encode-str "あ")))
    (is (= "$15\r\nこんにちは\r\n" (resp/encode-str "こんにちは")))))

;; ---------- プロパティテスト ----------

(defn- roundtrip
  "エンコードしてデコードする。"
  [x]
  (resp/read-reply (ByteArrayInputStream. (resp/encode x))))

(def gen-resp-scalar
  "往復で保存されるスカラー値。
   SimpleString はデコードで素の文字列になるため除外する。"
  (gen/one-of
   [gen/string                      ; Bulk String（"" を含む）
    gen/large-integer               ; Integer
    (gen/return nil)]))             ; Null Bulk String

(def gen-resp-value
  "配列の入れ子を含む RESP 値。
   ベクタのみを生成する（リストはデコードでベクタになるため）。"
  (gen/recursive-gen
   (fn [inner] (gen/vector inner 0 5))
   gen-resp-scalar))

(deftest roundtrip-property
  (let [result (tc/quick-check
                500
                (prop/for-all [v gen-resp-value]
                              (= v (roundtrip v))))]
    (is (:pass? result) (pr-str result))))

(deftest roundtrip-unicode
  (testing "マルチバイト文字列が往復する"
    (let [result (tc/quick-check
                  200
                  (prop/for-all [s (gen/fmap #(apply str %)
                                             (gen/vector gen/char 0 20))]
                                (= s (roundtrip s))))]
      (is (:pass? result) (pr-str result)))))

(deftest roundtrip-error
  (testing "エラーが往復する"
    (let [result (tc/quick-check
                  200
                  (prop/for-all [msg (gen/such-that
                                      #(not (re-find #"[\r\n]" %))
                                      gen/string
                                      100)]
                                (let [e (roundtrip (resp/error msg))]
                                  (and (resp/error? e)
                                       (= msg (:message e))))))]
      (is (:pass? result) (pr-str result)))))
