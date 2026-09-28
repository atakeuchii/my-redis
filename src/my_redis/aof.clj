(ns my-redis.aof
  (:require [my-redis.resp :as resp])
  (:import [java.io File FileOutputStream BufferedOutputStream]
           [java.nio.channels FileChannel]))

(defrecord AOF [^FileOutputStream out ^BufferedOutputStream buf file fsync-mode fsync-thread running?])

(defn- fsync! [^AOF aof]
  (.flush ^BufferedOutputStream (:buf aof))
  (.force ^FileChannel (.getChannel ^FileOutputStream (:out aof)) false))

(defn open! [^File file fsync-mode]
  (.mkdirs (.getParentFile (.getAbsoluteFile file)))
  (let [out (FileOutputStream. file true)
        buf (BufferedOutputStream. out)
        running? (atom true)
        aof (->AOF out buf file fsync-mode nil running?)
        thread (when (= fsync-mode :everysec)
                 (doto (Thread.
                        (fn []
                          (try
                            (while @running?
                              (Thread/sleep 1000)
                              (locking out (fsync! aof)))
                            (catch InterruptedException _ nil)
                            (catch Exception e
                              (println "[aof] fsync error:" (.getMessage e)))))
                        "my-redis-aof-fsync")
                   (.setDaemon true)
                   (.start)))]
    (assoc aof :fsync-thread thread)))

(defn append! [^AOF aof cmd]
  (let [^BufferedOutputStream buf (:buf aof)]
    (locking (:out aof)
      (resp/write-reply! buf (vec cmd))
      (case (:fsync-mode aof)
        :always (fsync! aof)
        :everysec (.flush buf)
        :no (.flush buf))))
  nil)

(defn close! [^AOF aof]
  (reset! (:running? aof) false)
  (when-let [^Thread t (:fsync-thread aof)]
    (.interrupt t))
  (locking (:out aof)
    (try (fsync! aof) (catch Exception _ nil))
    (.close ^BufferedOutputStream (:buf aof)))
  nil)

(defn file-size ^long [^AOF aof]
  (.length ^File (:file aof)))
