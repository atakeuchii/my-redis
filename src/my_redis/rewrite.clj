(ns my-redis.rewrite
  "キースペースの現在の状態から、それを再現する最小のコマンド列を生成し、
   AOF を作り直す（= my-storage の compaction）。

   実行の分担:
     prepare  … 実行スレッド。起点（オフセット＋スナップショット）を不可分に取る
     build!   … 別スレッド。時間がかかるファイル生成
     commit!  … 実行スレッド。一瞬で終わる差し替え"
  (:require [my-redis.aof :as aof]
            [my-redis.db :as db]
            [my-redis.resp :as resp]
            [my-redis.types.list :as dlist]
            [my-redis.types.zset :as zset])
  (:import [java.io File FileOutputStream BufferedOutputStream OutputStream]
           [java.nio.file Files CopyOption StandardCopyOption]))

(def ^:const items-per-cmd
  "1 コマンドに詰める最大要素数。巨大なキーを分割するため。"
  64)

;; ---------- コマンド列の生成 ----------

(defn- format-score
  ^String [^double s]
  (cond
    (= s Double/POSITIVE_INFINITY) "inf"
    (= s Double/NEGATIVE_INFINITY) "-inf"
    (and (== s (Math/rint s)) (< (Math/abs s) 1e17)) (str (long s))
    :else (str s)))

(defn- chunked-commands [cmd-name k items]
  (map (fn [chunk] (into [cmd-name k] chunk))
       (partition-all items-per-cmd items)))

(defn entry-commands [k entry]
  (let [{:keys [type value expire-at]} entry
        cmds (case type
               :string [["SET" k value]]
               :list (chunked-commands "RPUSH" k (dlist/to-vec value))
               :hash (chunked-commands "HSET" k (mapcat identity value))
               :set  (chunked-commands "SADD" k (seq value))
               :zset (chunked-commands "ZADD" k
                                       (mapcat (fn [[s m]] [(format-score s) m])
                                               (zset/entries value)))
               (throw (ex-info "rewrite: unknown type" {:key k :type type})))]
    (if (and (seq cmds) expire-at)
      (concat cmds [["PEXPIREAT" k (str expire-at)]])
      cmds)))

(defn snapshot-commands
  "スナップショット（{k entry}）全体を再現するコマンド列。
   期限切れは now を基準にここで除外する（prepare を O(1) に保つため）。
   遅延シーケンスなので、書き出しながら生成される。"
  [snapshot now]
  (mapcat (fn [[k entry]]
            (when-not (db/expired? entry now)
              (entry-commands k entry)))
          snapshot))

(defn- write-commands!
  ^long [^OutputStream out cmds]
  (reduce (fn [n cmd] (resp/write-reply! out cmd) (inc n)) 0 cmds))


(defn prepare
  "rewrite の起点を取る。実行スレッドから呼ぶこと。
   オフセットとスナップショットの取得の間に書き込みが入ると、
   その書き込みがどちらにも含まれず失われるため、不可分に行う必要がある。
   期限切れの除外はしない（O(N) になるため build! に任せる）。"
  [db aof-handle]
  {:offset   (if aof-handle (aof/current-size aof-handle) 0)
   :snapshot (:data @db)
   :now      (db/now)})

(defn build!
  "スナップショットから一時ファイルを作る。rename はしない。
   時間がかかるので別スレッドで実行してよい。
   :copied-to は旧 AOF をどこまで読んだか。commit! で続きを拾うために返す。"
  [{:keys [offset snapshot now]} ^File aof-file]
  (let [tmp (File. (str (.getAbsolutePath aof-file) ".rewrite-tmp"))]
    (.delete tmp)
    (with-open [out (BufferedOutputStream. (FileOutputStream. tmp))]
      (let [n      (write-commands! out (snapshot-commands snapshot now))
            copied (if (.exists aof-file) (aof/copy-from! aof-file offset out) 0)]
        (.flush out)
        {:tmp tmp :commands n :copied-bytes copied :copied-to (+ offset copied)}))))

(defn commit!
  "一時ファイルを本番にアトミックに差し替える。実行スレッドから呼ぶこと。
   build! が読み終えた位置以降に追記されたぶんを、ここで最終的に拾う。
   実行スレッドで走るので、この最中に新たな書き込みは入らない。"
  [{:keys [^File tmp copied-to]} ^File aof-file]
  (let [old-size (.length aof-file)]
    (when (and (.exists aof-file) (< (long copied-to) old-size))
      (with-open [out (BufferedOutputStream. (FileOutputStream. tmp true))]
        (aof/copy-from! aof-file copied-to out)))
    (Files/move (.toPath tmp) (.toPath aof-file)
                (into-array CopyOption
                            [StandardCopyOption/REPLACE_EXISTING
                             StandardCopyOption/ATOMIC_MOVE]))
    {:old-size old-size :new-size (.length aof-file)}))
