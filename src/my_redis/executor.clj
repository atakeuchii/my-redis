(ns my-redis.executor
  "コマンド実行を1本のスレッドに直列化する。"
  (:import [java.util.concurrent LinkedBlockingQueue]))

(defrecord Executor [queue thread running?])

(def ^:private poison ::stop)

(defn start!
  "実行スレッドを起動する。
   handler は (fn [task] -> result)。task はキューに入れられた任意の値。"
  [handler]
  (let [queue (LinkedBlockingQueue.)
        running? (atom true)
        thread (Thread.
                (fn []
                  (loop []
                    (let [item (.take queue)]
                      (when-not (= item poison)
                        (let [[task p] item]
                          (deliver p (try
                                       {:ok (handler task)}
                                       (catch Throwable t
                                         {:error t}))))
                        (recur)))))
                "my-redis-executor")]
    (.setDaemon thread true)
    (.start thread)
    (->Executor queue thread running?)))

(defn submit!
  "タスクを投入し、実行スレッドが処理するまで待って結果を返す。
   実行中に例外が飛んだ場合は、この呼び出し元で再送出する。"
  [^Executor ex task]
  (if-not @(:running? ex)
    (throw (IllegalStateException. "executor is stopped"))
    (let [p (promise)]
      (.put ^LinkedBlockingQueue (:queue ex) [task p])
      (let [{:keys [ok error]} @p]
        (if error
          (throw error)
          ok)))))

(defn stop!
  [^Executor ex]
  (when (compare-and-set! (:running? ex) true false)
    (.put ^LinkedBlockingQueue (:queue ex) poison)
    (.join ^Thread (:thread ex) 1000))
  nil)

(defn queue-depth
  ^long [^Executor ex]
  (.size ^LinkedBlockingQueue (:queue ex)))
