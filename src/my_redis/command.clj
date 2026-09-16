(ns my-redis.command
  (:require [clojure.string :as str]
            [my-redis.resp :as resp]
            [my-redis.db :as db]))

;; ---------- ハンドラ ----------
;; シグネチャ: (fn [ctx args] -> reply)
;;   ctx  = {:db <キースペース>}  … 後日 :aof :pubsub などが加わる
;;   args = コマンド名を除いた引数のベクタ
;; ソケットには触らない。返り値だけで応答を表す。

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

(defn- cmd-set [ctx [k v & opts]]
  (if (seq opts)
    (resp/error "ERR syntax error") ;; Day4で実装
    (do
      (db/set-value! (:db ctx) k :string v)
      (resp/simple "OK"))))

(defn- cmd-get [ctx [k]]
  (db/get-value (:db ctx) k))

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
   "DBSIZE"  {:arity  1 :write? false :handler cmd-dbsize}})

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
        ((:handler spec) ctx (vec (rest cmd)))))))
