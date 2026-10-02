(ns my-redis.rdb
  (:require [my-redis.db :as db]
            [my-redis.types.list :as dlist]
            [my-redis.types.zset :as zset])
  (:import [java.io File FileOutputStream BufferedOutputStream DataOutputStream
            DataInputStream ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Files CopyOption StandardCopyOption]
           [java.util.zip CRC32]))

(def ^:private ^String magic "MYRDB001")

(def ^:private type->tag
  {:string 0, :list 1, :hash 2, :set 3, :zset 4})

(def ^:private tag->type
  (into {} (map (fn [[k v]] [v k])) type->tag))

(defn- write-string! [^DataOutputStream out ^String s]
  (let [bs (.getBytes s "UTF-8")]
    (.writeInt out (alength bs))
    (.write out bs)))

(defn- write-strings! [^DataOutputStream out items]
  (.writeInt out (count items))
  (doseq [s items] (write-string! out s)))

(defn- format-score ^String [^double s]
  (cond 
    (= s Double/POSITIVE_INFINITY) "inf"
    (= s Double/NEGATIVE_INFINITY) "-inf"
    (and (== s (Math/rint s)) (< (Math/abs s) 1e17)) (str (long s))
    :else (str s)))

(defn- write-payload! [^DataOutputStream out type value]
  (case type
    :string (write-string! out value)
    :list (write-strings! out (dlist/to-vec value))
    :hash (write-strings! out (mapcat identity value))
    :set (write-strings! out (seq value))
    :zset (write-strings! out (mapcat (fn [[s m]] [(format-score s) m])
                                      (zset/entries value)))
    (throw (ex-info "rdb: unknown type" {:type type}))))

(defn- write-entry! [^DataOutputStream out k entry]
  (let [{:keys [type value expire-at]} entry]
    (.writeByte out (int (or (type->tag type)
                             (throw (ex-info "rdb: unknown type" {:key k :type type})))))
    (write-string! out k)
    (.writeLong out (long (or expire-at 0)))
    (write-payload! out type value)))

(defn- live-entries
  "期限切れを除いたエントリ。"
  [snapshot now]
  (remove (fn [[_ e]] (db/expired? e now)) snapshot))

(defn serialize ^bytes [snapshot now]
  (let [live (live-entries snapshot now)
        body (ByteArrayOutputStream.)
        out (DataOutputStream. body)]
    (.write out (.getBytes magic "UTF-8"))
    (.writeInt out (count live))
    (doseq [[k entry] live] (write-entry! out k entry))
    (.flush out)
    (let [payload (.toByteArray body)
          crc (doto (CRC32.) (.update payload))
          full (ByteArrayOutputStream.)
          fout (DataOutputStream. full)]
      (.write fout payload)
      (.writeLong fout (.getValue crc))
      (.flush fout)
      (.toByteArray full))))

(defn write-file! [^File file snapshot now]
  (let [bs (serialize snapshot now)
        tmp (File. (str (.getAbsolutePath file) ".tmp"))]
    (.mkdirs (.getParentFile (.getAbsoluteFile file)))
    (.delete tmp)
    (with-open [out (BufferedOutputStream. (FileOutputStream. tmp))]
      (.write out bs)
      (.flush out))
    (Files/move (.toPath tmp) (.toPath file)
                (into-array CopyOption
                            [StandardCopyOption/REPLACE_EXISTING
                             StandardCopyOption/ATOMIC_MOVE]))
    {:entries (count (live-entries snapshot now))
     :bytes (alength bs)}))

(defn- read-string! ^String [^DataInputStream in]
  (let [len (.readInt in)]
    (when (neg? len)
      (throw (ex-info "rdb: negative string length" {:len len})))
    (let [bs (byte-array len)]
      (.readFully in bs)
      (String. bs "UTF-8"))))

(defn- read-strings! [^DataInputStream in]
  (let [n (.readInt in)]
    (when (neg? n)
      (throw (ex-info "rdb: negative item count" {:count n})))
    (into [] (repeatedly n #(read-string! in)))))

(defn- read-entry! [^DataInputStream in]
  (let [tag (.readByte in)
        type (or (tag->type (int tag))
                 (throw (ex-info "rdb: unknown type tag" {:tag tag})))
        k (read-string! in)
        exp (.readLong in)
        cmds (case type
               :string [["SET" k (read-string! in)]]
               :list [(into ["RPUSH" k] (read-strings! in))]
               :hash [(into ["HSET" k] (read-strings! in))]
               :set [(into ["SADD" k] (read-strings! in))]
               :zset [(into ["ZADD" k] (read-strings! in))])]
    (if (pos? exp)
      (conj cmds ["PEXPIREAT" k (str exp)])
      cmds)))

(defn- verify-checksum! ^bytes [^bytes bs]
  (when (< (alength bs) (+ 8 (count magic) 4))
    (throw (ex-info "rdb: file too short" {:size (alength bs)})))
  (let [body-len (- (alength bs) 8)
        stored (.readLong (DataInputStream. (ByteArrayInputStream. bs body-len 8)))
        actual (.getValue (doto (CRC32.) (.update bs 0 body-len)))]
    (when-not (= stored actual)
      (throw (ex-info "rdb: checksum mismatch" {:stored stored :actual actual})))
    body-len))

(defn load-commands [^File file]
  (if-not (.exists file)
    []
    (let [bs (Files/readAllBytes (.toPath file))
          body-len (verify-checksum! bs)
          in (DataInputStream. (ByteArrayInputStream. bs 0 body-len))
          head (let [a (byte-array (count magic))]
                 (.readFully in a)
                 (String. a "UTF-8"))]
      (when-not (= magic head)
        (throw (ex-info "rdb: bad magic" {:got head :expected magic})))
      (let [n (.readInt in)]
        (into [] (mapcat identity) (repeatedly n #(read-entry! in)))))))
