(ns my-redis.expiry
  "能動的期限切れ。定期的にキースペースを掃除する。"
  (:require [my-redis.db :as db]))

(defrecord Cycler [thread running? stats])

(defn start!
  [cycle-fn {:keys [interval-ms] :or {interval-ms 100}}]
  (let [running? (atom true)
        stats (atom {:cycles 0 :removed 0 :max-cycle-ms 0})
        thread (Thread.
                (fn []
                  (while @running?
                    (let [start (System/nanoTime)
                          removed (try (cycle-fn)
                                       (catch Throwable t
                                         (println "[expiry] error:" (.getMessage t))
                                         0))
                          ms (/ (- (System/nanoTime) start) 1e6)]
                      (swap! stats (fn [s]
                                     (-> s
                                         (update :cycles inc)
                                         (update :removed + removed)
                                         (update :max-cycle-ms max ms)))))
                    (Thread/sleep interval-ms)))
                "my-redis-expiry")]
    (.setDaemon thread true)
    (.start thread)
    (->Cycler thread running? stats)))

(defn stop! [^Cycler c]
  (reset! (:running? c) false)
  (.interrupt ^Thread (:thread c))
  nil)

(defn stats [^Cycler c] 
  @(:stats c))
