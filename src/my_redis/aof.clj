(ns my-redis.aof
  (:require [my-redis.resp :as resp])
  (:import [java.io File FileOutputStream BufferedOutputStream OutputStream RandomAccessFile]
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
                              (locking out
                                (when @running?
                                  (fsync! aof))))
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

(defn replay!
  "AOF を先頭から読み、各コマンドを handler に渡す。
   [適用件数 健全なバイト数] を返す。
   末尾が壊れていれば、そこまでの件数と位置を返す。"
  [^File file handler]
  (if-not (.exists file)
    [0 0]
    (with-open [in (java.io.BufferedInputStream. (java.io.FileInputStream. file))]
      (loop [applied 0
             good-bytes 0]
        (let [cmd (try
                    (resp/read-reply in)
                    (catch java.io.EOFException _ ::eof)
                    (catch Exception e
                      (println "[aof] corrupt record:" (.getMessage e))
                      ::corrupt))]
          (cond
            (= cmd ::eof)     [applied good-bytes]
            (= cmd ::corrupt) [applied good-bytes]
            (nil? cmd)        (recur applied good-bytes)      ; 空行など
            :else
            (do (handler cmd)
                (recur (inc applied) (- (.length file) (.available in))))))))))

(defn truncate!
  "壊れた末尾を切り捨てる。"
  [^File file ^long size]
  (when (< size (.length file))
    (println (format "[aof] truncating %d -> %d bytes" (.length file) size))
    (with-open [raf (RandomAccessFile. file "rw")]
      (.setLength raf size))))

(defn current-size
  "現在のファイルサイズ。flush してから測る。"
  ^long [^AOF aof]
  (locking (:out aof)
    (.flush ^BufferedOutputStream (:buf aof))
    (.length ^File (:file aof))))

(defn copy-from!
  "src の offset 以降を dest に追記する。コピーしたバイト数を返す。"
  ^long [^File src ^long offset ^OutputStream dest]
  (with-open [in (RandomAccessFile. src "r")]
    (.seek in offset)
    (let [buf (byte-array 65536)]
      (loop [total 0]
        (let [n (.read in buf)]
          (if (neg? n)
            total
            (do (.write dest buf 0 n)
                (recur (+ total n)))))))))
