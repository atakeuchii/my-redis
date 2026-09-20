(ns my-redis.command
  (:require [clojure.string :as str]
            [my-redis.resp :as resp]
            [my-redis.db :as db]
            [my-redis.types.list :as dlist]))

;; ---------- ハンドラ ----------
;; シグネチャ: (fn [ctx args] -> reply)
;;   ctx  = {:db <キースペース>}  … 後日 :aof :pubsub などが加わる
;;   args = コマンド名を除いた引数のベクタ
;; ソケットには触らない。返り値だけで応答を表す。

(def ^:private wrong-type-error
  (resp/error "WRONGTYPE Operation against a key holding the wrong kind of value"))

(def ^:private not-integer-error
  (resp/error "ERR value is not an integer or out of range"))

(def ^:private overflow-error
  (resp/error "ERR increment or decrement would overflow"))

(defn- byte-length ^long [^String s]
  (alength (.getBytes s "UTF-8")))

(defn- parse-long-or-nil
  [^String s]
  (try (Long/parseLong s)
       (catch NumberFormatException _ nil)))

(defn- cmd-ping [_ctx args]
  (if (seq args)
    (first args)
    (resp/simple "PONG")))

(defn- cmd-echo [_ctx [msg]]
  msg)

(defn- cmd-command [_ctx _args]
  [])

(defn- cmd-quit [_ctx _args]
  :quit)

(defn- parse-set-options
  "SET のオプションをパースする。
   成功: {:ttl <ミリ秒 or nil> :exists <:nx / :xx / nil>}
   失敗: {:error <RespError>}"
  [opts]
  (loop [opts (seq opts)
         acc  {:ttl nil :exists nil :ttl-unit nil}]
    (if-not opts
      (dissoc acc :ttl-unit)
      (let [o (str/upper-case (first opts))]
        (case o
          ("EX" "PX")
          (cond
            (:ttl-unit acc) {:error (resp/error "ERR syntax error")}
            (nil? (second opts)) {:error (resp/error "ERR syntax error")}
            :else
            (if-let [n (parse-long-or-nil (second opts))]
              (if (pos? n)
                (recur (nnext opts)
                       (assoc acc
                              :ttl (if (= o "EX") (* n 1000) n)
                              :ttl-unit o))
                {:error (resp/error "ERR invalid expire time in 'set' command")})
              {:error not-integer-error}))

          "NX"
          (if (:exists acc)
            {:error (resp/error "ERR syntax error")}
            (recur (next opts) (assoc acc :exists :nx)))

          "XX"
          (if (:exists acc)
            {:error (resp/error "ERR syntax error")}
            (recur (next opts) (assoc acc :exists :xx)))

          {:error (resp/error "ERR syntax error")})))))

(defn- cmd-set [ctx [k v & opts]]
  (let [parsed (parse-set-options opts)]
    (if-let [e (:error parsed)]
      e
      (let [{:keys [ttl exists]} parsed
            outcome (atom nil)]
        (db/update-entry!
         (:db ctx) k
         (fn [existing]
           (cond
             (and (= :nx exists) (some? existing))
             (do (reset! outcome nil) existing)
             
             (and (= :xx exists) (nil? existing))
             (do (reset! outcome nil) nil)
             
             :else
             (do (reset! outcome (resp/simple "OK"))
                 (cond-> (db/entry :string v)
                   ttl (assoc :expire-at (+ (db/now) ttl)))))))
        @outcome))))

(defn- cmd-get [ctx [k]]
  (let [v (db/typed-value (:db ctx) k :string nil)]
    (if (db/wrong-type? v)
      wrong-type-error
      (db/get-value (:db ctx) k))))

(defn- cmd-del [ctx args]
  (db/delete-many! (:db ctx) args))

(defn- cmd-exists [ctx args]
  (count (filter #(db/exists? (:db ctx) %) args)))

(defn- cmd-type [ctx [k]]
  (resp/simple (if-let [t (db/key-type (:db ctx) k)]
                 (name t)
                 "none")))

(defn- glob->regex
  "Redis の glob パターンを正規表現に変換する。"
  [pattern]
  (let [sb (StringBuilder.)]
    (loop [chars (seq pattern)]
      (when-let [c (first chars)]
        (case c
          \* (do (.append sb ".*") (recur (next chars)))
          \? (do (.append sb ".")  (recur (next chars)))
          \[ (let [[cls rest] (split-with #(not= % \]) (next chars))]
               (.append sb "[")
               (doseq [ch cls] (.append sb (java.util.regex.Pattern/quote (str ch))))
               (.append sb "]")
               (recur (next rest)))
          \\ (if-let [nxt (second chars)]
               (do (.append sb (java.util.regex.Pattern/quote (str nxt)))
                   (recur (nnext chars)))
               (do (.append sb "\\\\") (recur (next chars))))
          (do (.append sb (java.util.regex.Pattern/quote (str c)))
              (recur (next chars))))))
    (re-pattern (str "^" sb "$"))))

(defn- cmd-keys [ctx [pattern]]
  (let [re (glob->regex pattern)]
    (into [] (filter #(re-matches re %)) (db/keys (:db ctx)))))

(defn- cmd-flushdb [ctx _args]
  (db/clear! (:db ctx))
  (resp/simple "OK"))

(defn- cmd-dbsize [ctx _args]
  (db/size (:db ctx)))

(defn- incr-by!
  [ctx k ^long delta]
  (let [d (:db ctx)
        outcome (atom nil)]
    (db/update-entry!
     d k
     (fn [e]
       (cond
         (and (some? e) (not= :string (:type e)))
         (do (reset! outcome wrong-type-error)
             e)

         :else
         (let [n (parse-long-or-nil (if e (:value e) "0"))]
           (cond
             (nil? n)
             (do (reset! outcome not-integer-error)
                 e)

             (or (and (pos? delta) (> n (- Long/MAX_VALUE delta)))
                 (and (neg? delta) (< n (- Long/MIN_VALUE delta))))
             (do (reset! outcome overflow-error) e)

             :else
             (let [next (+ n delta)]
               (reset! outcome next)
               (db/entry :string (str next))))))))
    @outcome))

(defn- cmd-incr [ctx [k]] (incr-by! ctx k 1))
(defn- cmd-decr [ctx [k]] (incr-by! ctx k -1))

(defn- cmd-incrby [ctx [k delta]]
  (if-let [n (parse-long-or-nil delta)]
    (incr-by! ctx k n)
    not-integer-error))

(defn- cmd-decrby [ctx [k delta]]
  (if-let [n (parse-long-or-nil delta)]
    (incr-by! ctx k (- n))
    not-integer-error))

(defn- cmd-strlen [ctx [k]]
  (let [v (db/typed-value (:db ctx) k :string "")]
    (if (db/wrong-type? v)
      wrong-type-error
      (byte-length v))))

(defn- cmd-append [ctx [k suffix]]
  (let [outcome (atom nil)]
    (db/update-entry!
     (:db ctx) k
     (fn [e]
       (if (and (some? e) (not= :string (:type e)))
         (do (reset! outcome wrong-type-error) e)
         (let [next (str (if e (:value e) "") suffix)]
           (reset! outcome (byte-length next))
           (db/entry :string next)))))
    @outcome))

(defn- cmd-getset [ctx [k v]]
  (let [outcome (atom nil)]
    (db/update-entry!
     (:db ctx) k
     (fn [e]
       (if (and (some? e) (not= :string (:type e)))
         (do (reset! outcome wrong-type-error) e)
         (do (reset! outcome (:value e))
             (db/entry :string v)))))
    @outcome))

(defn- cmd-setnx [ctx [k v]]
  (let [outcome (atom nil)]
    (db/update-entry!
     (:db ctx) k
     (fn [e]
       (if (some? e)
         (do (reset! outcome 0) e)
         (do (reset! outcome 1)
             (db/entry :string v)))))
    @outcome))

(defn- cmd-mset [ctx args]
  (if (odd? (count args))
    (resp/error "ERR wrong number of arguments for 'mset' command")
    (let [pairs (partition 2 args)]
      (db/set-entries! (:db ctx)
                       (into {}
                             (map (fn [[k v]] [k (db/entry :string v)]) pairs)))
      (resp/simple "OK"))))

(defn- cmd-mget [ctx args]
  (let [snap (db/snapshot (:db ctx))]
    (mapv (fn [k]
            (let [e (db/entry-in snap k)]
              (when (= :string (:type e))
                (:value e))))
          args)))

(defn- list-update!
  "リストを更新する共通処理。f は現在のベクタ(無ければ [])を受け取り、
   [新しいベクタ 返り値] を返す。新しいベクタが空ならキーごと削除する。"
  [ctx k f]
  (let [outcome (atom nil)]
    (db/update-entry!
     (:db ctx) k
     (fn [e]
       (if (and (some? e) (not= :list (:type e)))
         (do (reset! outcome wrong-type-error) e)
         (let [current (if e (:value e) dlist/empty-list)
               [next ret] (f current)]
           (reset! outcome ret)
           (when-not (dlist/empty? next)
             (db/entry :list next))))))
    @outcome))

(defn- list-read
  "リストを読む共通処理。f は現在のベクタ(無ければ [])を受け取り、返り値を返す。"
  [ctx k f]
  (let [v (db/typed-value (:db ctx) k :list dlist/empty-list)]
    (if (db/wrong-type? v)
      wrong-type-error
      (f v))))

(defn- normalize-index
  ^long [^long i ^long len]
  (if (neg? i) (+ len i) i))

(defn- clamp-range
  [^long start ^long stop ^long len]
  (let [s (max 0 (normalize-index start len))
        e (min (dec len) (normalize-index stop len))]
    (when (and (<= s e) (< s len))
      [s e])))

(defn- cmd-rpush [ctx [k & vs]]
  (list-update! ctx k (fn [l]
                        (let [next (reduce dlist/push-right l vs)]
                          [next (dlist/count next)]))))

(defn- cmd-lpush [ctx [k & vs]]
  (list-update! ctx k (fn [l]
                        (let [next (reduce dlist/push-left l vs)]
                          [next (dlist/count next)]))))

(defn- cmd-rpop [ctx [k]]
  (list-update! ctx k (fn [l]
                        (if (dlist/empty? l)
                          [l nil]
                          [(dlist/pop-right l) (dlist/peek-right l)]))))

(defn- cmd-lpop [ctx [k]]
  (list-update! ctx k (fn [l]
                        (if (dlist/empty? l)
                          [l nil]
                          [(dlist/pop-left l) (dlist/peek-left l)]))))

(defn- cmd-llen [ctx [k]]
  (list-read ctx k dlist/count))

(defn- cmd-lrange [ctx [k start stop]]
  (let [s (parse-long-or-nil start)
        e (parse-long-or-nil stop)]
    (if (or (nil? s) (nil? e))
      not-integer-error
      (list-read ctx k
                 (fn [l]
                   (if-let [[from to] (clamp-range s e (dlist/count l))]
                     (dlist/subrange l from to)
                     []))))))

(defn- cmd-lindex [ctx [k i]]
  (if-let [n (parse-long-or-nil i)]
    (list-read ctx k
               (fn [l]
                 (dlist/nth l (normalize-index n (dlist/count l)))))
    not-integer-error))

(defn- cmd-lset [ctx [k i v]]
  (if-let [n (parse-long-or-nil i)]
    (list-update! ctx k
                  (fn [l]
                    (let [len (dlist/count l)
                          idx (normalize-index n len)]
                      (cond
                        (zero? len)
                        [l (resp/error "ERR no such key")]

                        (or (neg? idx) (>= idx len))
                        [l (resp/error "ERR index out of range")]

                        :else
                        [(dlist/assoc-nth l idx v) (resp/simple "OK")]))))
    not-integer-error))

(defn- hash-update! [ctx k f]
  (let [outcome (atom nil)]
    (db/update-entry!
     (:db ctx) k
     (fn [e]
       (if (and (some? e) (not= :hash (:type e)))
         (do (reset! outcome wrong-type-error) e)
         (let [current (if e (:value e) {})
               [next ret] (f current)]
           (reset! outcome ret)
           (when-let [nv (not-empty next)]
             (db/entry :hash nv))))))
    @outcome))

(defn- hash-read [ctx k f]
  (let [v (db/typed-value (:db ctx) k :hash {})]
    (if (db/wrong-type? v)
      wrong-type-error
      (f v))))

(defn- cmd-hset [ctx [k & fvs]]
  (if (or (empty? fvs) (odd? (count fvs)))
    (resp/error "ERR wrong number of arguments for 'hset' command")
    (hash-update! ctx k
                  (fn [m]
                    (let [pairs (partition 2 fvs)
                          added (count (remove #(contains? m (first %)) pairs))
                          next (reduce (fn [acc [f v]] (assoc acc f v)) m pairs)]
                      [next added])))))

(defn- cmd-hget [ctx [k f]]
  (hash-read ctx k #(get % f)))

(defn- cmd-hdel [ctx [k & fs]]
  (hash-update! ctx k
                (fn [m]
                  (let [removed (count (filter #(contains? m %) (distinct fs)))]
                    [(apply dissoc m fs) removed]))))

(defn- cmd-hgetall [ctx [k]]
  (hash-read ctx k (fn [m] (into [] (mapcat identity) m))))

(defn- cmd-hkeys [ctx [k]]
  (hash-read ctx k #(into [] (keys %))))

(defn- cmd-hvals [ctx [k]]
  (hash-read ctx k #(into [] (vals %))))

(defn- cmd-hlen [ctx [k]]
  (hash-read ctx k count))

(defn- cmd-hexists [ctx [k f]]
  (hash-read ctx k #(if (contains? % f) 1 0)))

(defn- cmd-hincrby [ctx [k f delta]]
  (if-let [d (parse-long-or-nil delta)]
    (hash-update! ctx k
                  (fn [m]
                    (let [current (get m f "0")
                          n (parse-long-or-nil current)]
                      (cond
                        (nil? n)
                        [m (resp/error "ERR hash value is not an integer")]
                        
                        (or (and (pos? d) (> n (- Long/MAX_VALUE d)))
                            (and (neg? d) (< n (- Long/MIN_VALUE d))))
                        [m (resp/error "ERR increment or decrement would overflow")]
                        
                        :else
                        (let [next (+ n d)]
                          [(assoc m f (str next)) next])))))
    not-integer-error))

(def command-table
  {"PING"    {:arity -1 :write? false :handler cmd-ping}
   "ECHO"    {:arity  2 :write? false :handler cmd-echo}
   "COMMAND" {:arity -1 :write? false :handler cmd-command}
   "QUIT"    {:arity  1 :write? false :handler cmd-quit}

   "SET"     {:arity -3 :write? true  :handler cmd-set}
   "GET"     {:arity  2 :write? false :handler cmd-get}
   "DEL"     {:arity -2 :write? true  :handler cmd-del}
   "EXISTS"  {:arity -2 :write? false :handler cmd-exists}
   "TYPE"    {:arity  2 :write? false :handler cmd-type}
   "KEYS"    {:arity  2 :write? false :handler cmd-keys}
   "FLUSHDB" {:arity -1 :write? true  :handler cmd-flushdb}

   "DBSIZE"  {:arity  1 :write? false :handler cmd-dbsize}
   "INCR"    {:arity  2 :write? true  :handler cmd-incr}
   "DECR"    {:arity  2 :write? true  :handler cmd-decr}
   "INCRBY"  {:arity  3 :write? true  :handler cmd-incrby}
   "DECRBY"  {:arity  3 :write? true  :handler cmd-decrby}
   
   "APPEND" {:arity 3 :write? true  :handler cmd-append}
   "STRLEN" {:arity 2 :write? false :handler cmd-strlen}
   "GETSET" {:arity 3 :write? true  :handler cmd-getset}
   "SETNX"  {:arity 3 :write? true  :handler cmd-setnx}
   "MSET"   {:arity -3 :write? true  :handler cmd-mset}
   "MGET"   {:arity -2 :write? false :handler cmd-mget}
   
   "RPUSH"  {:arity -3 :write? true  :handler cmd-rpush}
   "LPUSH"  {:arity -3 :write? true  :handler cmd-lpush}
   "RPOP"   {:arity  2 :write? true  :handler cmd-rpop}
   "LPOP"   {:arity  2 :write? true  :handler cmd-lpop}
   "LLEN"   {:arity  2 :write? false :handler cmd-llen}
   "LRANGE" {:arity  4 :write? false :handler cmd-lrange}
   "LINDEX" {:arity  3 :write? false :handler cmd-lindex}
   "LSET"   {:arity  4 :write? true  :handler cmd-lset}
   
   "HSET"     {:arity -4 :write? true  :handler cmd-hset}
   "HGET"     {:arity  3 :write? false :handler cmd-hget}
   "HDEL"     {:arity -3 :write? true  :handler cmd-hdel}
   "HGETALL"  {:arity  2 :write? false :handler cmd-hgetall}
   "HKEYS"    {:arity  2 :write? false :handler cmd-hkeys}
   "HVALS"    {:arity  2 :write? false :handler cmd-hvals}
   "HLEN"     {:arity  2 :write? false :handler cmd-hlen}
   "HEXISTS"  {:arity  3 :write? false :handler cmd-hexists}
   "HINCRBY"  {:arity  4 :write? true  :handler cmd-hincrby}})

(defn- arity-ok?
  [^long arity ^long n]
  (if (neg? arity)
    (>= n (- arity))
    (= n arity)))

(defn dispatch
  [ctx cmd]
  (if-not (seq cmd)
    :no-reply
    (let [raw (first cmd)
          name (str/upper-case raw)
          spec (get command-table name)]
      (cond
        (nil? spec)
        (resp/error (str "ERR unknown command '" raw "'"))

        (not (arity-ok? (:arity spec) (count cmd)))
        (resp/error (str "ERR wrong number of arguments for '" (str/lower-case name) "' command"))

        :else
        (try
          ((:handler spec) ctx (vec (rest cmd)))
          (catch Exception e
            (println "[command] error in" name ":" (.getMessage e))
            (resp/error "ERR internal error")))))))
