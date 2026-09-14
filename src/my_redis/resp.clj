(ns my-redis.resp
  (:require [clojure.string :as str])
  (:import [java.io InputStream OutputStream ByteArrayOutputStream EOFException]))

(def ^:private CR (int \return))
(def ^:private LF (int \newline))
;; (def ^:private ^bytes CRLF (.getBytes "\r\n" "UTF-8"))
(def ^:private ^"[B" CRLF (.getBytes "\r\n" "UTF-8"))

(defrecord RespError [^String message])
(defrecord SimpleString [^String value])

(declare read-reply)

(defn- read-byte!
  ^long [^InputStream in]
  (let [b (.read in)]
    (if (neg? b)
      (throw (EOFException. "unexpected end of stream"))
      b)))

(defn- read-line-bytes!
  ^bytes [^InputStream in]
  (let [baos (ByteArrayOutputStream.)]
    (loop []
      (let [b (read-byte! in)]
        (if (== b CR)
          (let [nxt (read-byte! in)]
            (when-not (== nxt LF)
              (throw (ex-info "protocol error: expected LF after CR" {:got nxt})))
            (.toByteArray baos))
          (do (.write baos (int b))
              (recur)))))))

(defn- read-line-str!
  ^String [^InputStream in]
  (String. (read-line-bytes! in) "UTF-8"))

(defn- read-count!
  ^long [^InputStream in]
  (let [s (read-line-str! in)]
    (try
      (Long/parseLong s)
      (catch NumberFormatException _
        (throw (ex-info "protoco error: invalid integer" {:got s}))))))

(defn error
  [^String message]
  (->RespError message))

(defn error?
  [x]
  (instance? RespError x))

(defn- read-bulk-string!
  [^InputStream in]
  (let [len (read-count! in)]
    (if (neg? len)
      nil
      (let [buf (byte-array len)]
        (loop [off 0]
          (when (< off len)
            (let [n (.read in buf off (- len off))]
              (when (neg? n)
                (throw (EOFException. "unexpected end of stream in bulk string")))
              (recur (+ off n)))))
        (let [cr (read-byte! in)
              lf (read-byte! in)]
          (when-not (and (== cr CR) (== lf LF))
            (throw (ex-info "protocol error: expected CRLF after bulk string" {}))))
        (String. buf "UTF-8")))))

(defn- read-array!
  [^InputStream in]
  (let [n (read-count! in)]
    (if (neg? n)
      nil
      (into [] (repeatedly n #(read-reply in))))))

(defn- read-inline-command!
  [^InputStream in ^long first-byte]
  (let [rest-bytes (read-line-bytes! in)
        baos (ByteArrayOutputStream.)]
    (.write baos (int first-byte))
    (.write baos rest-bytes)
    (let [line (String. (.toByteArray baos) "UTF-8")
          args (->> (str/split line #"\s+")
                    (remove str/blank?)
                    (into []))]
      (when (seq args)
        args))))

(defn read-reply
  [^InputStream in]
  (let [b (read-byte! in)]
    (case (char b)
      \+ (read-line-str! in)
      \- (->RespError (read-line-str! in))
      \: (read-count! in)
      \$ (read-bulk-string! in)
      \* (read-array! in)
      \return (let [lf (read-byte! in)]
                (when-not (== lf LF)
                  (throw (ex-info "protocol error: expected LF after CR" {:got lf})))
                nil)
      (read-inline-command! in b))))

(defn simple 
  [^String value]
  (->SimpleString value))


(defn- write-crlf!
  [^OutputStream out]
  (.write out CRLF))

(defn- write-str!
  [^OutputStream out ^String s]
  (.write out (.getBytes s "UTF-8")))

(defn- write-prefiexed-line!
  [^OutputStream out ^Character prefix ^String s]
  (.write out (int prefix))
  (write-str! out s)
  (write-crlf! out))

(defn write-reply!
  "Clojure データを RESP としてストリームに書く。
     nil          -> Null Bulk String ($-1)
     SimpleString -> +
     RespError    -> -
     整数          -> :
     文字列        -> Bulk String ($)
     シーケンシャル -> Array (*)"
  [^OutputStream out x]
  (cond
    (nil? x)
    (write-str! out "$-1\r\n")
    
    (instance? SimpleString x)
    (write-prefiexed-line! out \+ (:value x))
    
    (instance? RespError x)
    (write-prefiexed-line! out \- (:message x))
    
    (integer? x)
    (write-prefiexed-line! out \: (str x))
    
    (string? x)
    (let [^bytes bs (.getBytes ^String x "UTF-8")]
      (.write out (int \$))
      (write-str! out (str (alength bs)))
      (write-crlf! out)
      (.write out bs)
      (write-crlf! out))
    
    (sequential? x)
    (do
      (.write out (int \*))
      (write-str! out (str (count x)))
      (write-crlf! out)
      (doseq [item x]
        (write-reply! out item)))
    
    :else
    (throw (ex-info "cannot encode value as RESP" {:value x :type (type x)}))))

(defn encode
  ^bytes [x]
  (let [baos (ByteArrayOutputStream.)]
    (write-reply! baos x)
    (.toByteArray baos)))

(defn encode-str
  ^String [x]
  (String. (encode x) "UTF-8"))