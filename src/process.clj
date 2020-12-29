(ns process
  (:require [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as jsql]
            [spotify.cache :as cache]
            [clojure.string :as str]
            [next.jdbc.prepare :as p])
  (:import [java.io File]))

(def listing-file "listing.txt")
(def db-file "spotify.sqlite")
(def db-spec {:dbtype "sqlite"
              :dbname db-file})

(defn reset
  []
  (io/delete-file (io/file db-file)))

(defn init-tables
  [ds]
  (jdbc/execute! ds ["PRAGMA SYNCHRONOUS=OFF"])
  (jdbc/execute! ds ["PRAGMA JOURNAL_MODE=OFF"])
  (jdbc/execute! ds ["PRAGMA LOCKING_MODE=EXCLUSIVE"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS artists (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL
                      );"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS albums (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        track_count INT NOT NULL,
                        release_date TEXT NOT NULL
                      );"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS artistalbums (
                        album_id TEXT NOT NULL,
                        artist_id TEXT NOT NULL,
                        FOREIGN KEY (album_id) REFERENCES albums (id),
                        FOREIGN KEY (artist_id) REFERENCES artists (id),
                        PRIMARY KEY (album_id, artist_id)
                      );"]))

(defn req-type
  [data]
  (cond
    (= "album" (:type data)) :album
    (and (= 1 (count (keys data))) (sequential? (:artists data))) :artists))

(defn insert-artists!
  [conn artists]
  (with-open [ps (jdbc/prepare conn ["INSERT OR REPLACE INTO artists (id,name) VALUES (?,?)"])]
    (p/execute-batch! ps (->> artists :artists (mapv (fn [artist] [(:id artist) (:name artist)]))))))

(defn insert-album!
  [conn album]
  (jdbc/with-transaction
    [tx conn]
    (jdbc/execute! tx ["INSERT OR REPLACE INTO albums (id,name,release_date,track_count) VALUES (?,?,?,?)"
                       (:id album)
                       (:name album)
                       (:release_date album)
                       (count (:tracks album))])
    (with-open [ps (jdbc/prepare tx ["INSERT OR REPLACE INTO artistalbums (album_id,artist_id) VALUES (?,?)"])]
      (p/execute-batch! ps (->> album :artists (mapv (fn [artist] [(:id album) (:id artist)])))))))

(defn process-cached-file
  [num-processed conn ^File f]
  (try
    (when-let [data (cache/cache-get (str/replace (.getAbsolutePath f) (str cache/cache-save-base-dir "/") ""))]
      (case (req-type data)
        :artists (insert-artists! conn data)
        :album (insert-album! conn data)
        nil))
    (finally
      (swap! num-processed inc))))

(defn run
  [_]
  (let [ds (jdbc/get-datasource db-spec)
        num-processed (atom 0)
        f (io/file listing-file)
        total-to-process (->> f io/reader line-seq count)
        running? (atom true)]
    (future
      (loop [step 0
             last-processed @num-processed]
        (let [processed @num-processed]
          (println (format "%s: processed %s/%s (+%s)" step processed total-to-process (- processed last-processed)))
          (when (and @running? (< processed total-to-process))
            (Thread/sleep 5000)
            (recur (inc step) processed)))))
    (try
      (init-tables ds)
      (with-open [conn (jdbc/get-connection ds)]
        (->> f
             io/reader
             line-seq
             (map (partial io/file cache/cache-save-base-dir))
             (filter #(.isFile %))
             (pmap #(process-cached-file num-processed conn %))
             (doall)))
      (finally
        (reset! running? false)))))

(comment
  (reset)
  (->> (line-seq (io/reader (io/file listing-file)))
       (map (partial io/file cache/cache-save-base-dir))
       (filter #(not= :album (req-type (cache/cache-get (str/replace (.getAbsolutePath %) (str cache/cache-save-base-dir "/") "")))))
       (pmap #(process-cached-file (atom 0) conn %))
       (take 5)
       #_(map #(cache/cache-get (str/replace (.getAbsolutePath %) (str cache/cache-save-base-dir "/") "")))
       (doall))
  (let [ds (jdbc/get-datasource db-spec)]
    #_(jsql/query ds ["select * from artistalbums;"])
    #_(init-tables ds)
    (with-open [conn (jdbc/get-connection ds)]
      #_(insert-album! conn {:id "test"
                            :name "testo"
                           :release_date "2015"
                           :tracks [1 2 3 4 5]
                           :artists [{:id "test"}]})
      #_(jsql/query conn ["select * from artistalbums;"])
      (->> (line-seq (io/reader (io/file listing-file)))
           (map (partial io/file cache/cache-save-base-dir))
           (filter #(= :artists (req-type (cache/cache-get (str/replace (.getAbsolutePath %) (str cache/cache-save-base-dir "/") "")))))
           (take 1)
           #_#_#_(pmap #(process-cached-file (atom 0) conn %))
           (take 20)
           #_(map #(cache/cache-get (str/replace (.getAbsolutePath %) (str cache/cache-save-base-dir "/") "")))
           (doall))))
  (do
    (reset)
    (let [ds (jdbc/get-datasource db-spec)]
      (init-tables ds)))
  (run nil))