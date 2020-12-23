(ns main
  (:require [clojure.core.async :refer [<! <!! >! >!! chan go go-loop put! timeout]]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [spotify.api :as api]
            [util :refer [format-diff map-vals pmapcat]]))

;; put your client id & secret into `env.edn`
(def env (-> "env.edn" slurp edn/read-string))
(def client-id (env :spotify-client-id))
(def client-secret (env :spotify-client-secret))

(def num-io-threads 50)

;; runs in a go block
(defn process-artist
  [client {:keys [frontier processed] :as data} artist {:keys [artist-chan album-chan]}]
  (if-not (contains? @(:artists processed) artist)
    (let [related-artists (->> artist
                               (api/related-artists client)
                               (keep (fn [{:keys [id]}]
                                       (when-not (contains? @(:artists processed) id)
                                         (>!! artist-chan id)))))
          artist-albums (->> artist
                             (api/artist-albums client)
                             (keep (fn [{:keys [id]}]
                                     (when-not (contains? @(:albums processed) id)
                                       (>!! album-chan id)))))]
      (doall (concat related-artists artist-albums))
      (swap! (:artists processed) conj artist)
      (swap! (:artists frontier) + (count related-artists) -1) ;; we processed one artist!
      (swap! (:albums frontier) + (count artist-albums)))
    (swap! (:artists frontier) - 1)))

(defn process-albums
  [client {:keys [frontier processed] :as data} albums]
  (let [processed-albums (->> albums
                              (api/albums client)
                              (group-by :id)
                              (map-vals #(-> % first
                                             (update :tracks count)
                                             (select-keys [:release_date :tracks]))))]
    (swap! (:albums processed) merge processed-albums)
    (swap! (:albums frontier) - (count processed-albums))))

(defn process-cached-album
  [client {:keys [frontier processed] :as data} {:keys [id] :as album}]
  (let [processed-albums {id (-> album
                                 (update :tracks count)
                                 (select-keys [:release_date :tracks]))}]
    (swap! (:albums processed) merge processed-albums)
    (swap! (:albums frontier) - 1)))

(def chan-size 2000000)

(defn run-async
  [client {:keys [frontier processed] :as data}]
  (let [{:keys [artist-chan album-chan done-chan] :as chans}
        {:artist-chan (chan chan-size)
         :album-chan (chan chan-size)
         :done-chan (chan 2)}
        running-artists (atom 0)
        running-albums (atom 0)]
    (->> (concat
           (->> (:artists frontier)
                (map #(put! artist-chan %)))
           (->> (:albums frontier)
                (map #(put! album-chan %))))
         (doall))
    (let [frontier {:albums (atom (count (:albums frontier)))
                    :artists (atom (count (:artists frontier)))}
          data (assoc data :frontier frontier)]
      ;; artist go-loop
      (go-loop
        []
        (if (zero? @(:artists frontier))
          (>! done-chan :artists)
          (do
            (let [in-flight @running-artists]
              (if-not (>= in-flight (/ num-io-threads 2))
                (go
                  (when (compare-and-set! running-artists in-flight (inc in-flight))
                    (try
                      (when-let [artist (<! artist-chan)]
                        (process-artist client data artist chans))
                      (finally
                        (swap! running-artists dec)))))
                (<! (timeout 1000))))
            (recur))))
      ;; album go-loop
      (go-loop
        [albums-to-process (atom [])]
        (let [in-flight @running-albums
              albums @albums-to-process
              can-run? (>= (/ num-io-threads 2) in-flight)]
          (when (and
                  (or (= (count albums) 20)
                      (zero? @(:albums frontier)))
                  can-run?)
            (when (compare-and-set! running-albums in-flight (inc in-flight))
              (reset! albums-to-process [])
              (go
                (try
                  (process-albums client data albums)
                  (finally
                    (swap! running-albums dec))))))
          (when-not can-run?
            (<! (timeout 1000)))
          (if (and (zero? @(:albums frontier)) (empty? @albums-to-process))
            (>! done-chan :albums)
            (do
              (when-let [new-album (when (and can-run? (< (count @albums-to-process) 20)) (<! album-chan))]
                (if-let [cached (api/cached-album new-album)]
                  (process-cached-album client data cached)
                  (swap! albums-to-process conj new-album)))
              (recur albums-to-process)))))
      ;; progress go-loop
      (future
        (loop
          [step 0
           last {:artists 0 :albums 0}]
          (Thread/sleep 5000)
          (let [num-processed {:artists (count @(:artists processed))
                               :albums (count @(:albums processed))}]
            (println (format "%s: \tprocessed %s (%s total) artists (%s/m), %s (%s total) albums (%s/m) (%s artists, %s albums in flight) (%s in flight requests)"
                             step
                             (- (:artists num-processed)
                                (:artists last))
                             (:artists num-processed)
                             (* (- (:artists num-processed)
                                   (:artists last))
                                12)
                             (- (:albums num-processed)
                                (:albums last))
                             (:albums num-processed)
                             (* (- (:albums num-processed)
                                   (:albums last))
                                12)
                             @running-artists
                             @running-albums
                             @(:in-flight-requests client)))
            (println (format "\t\t%s artists remaining, %s albums remaining"
                             @(:artists frontier)
                             @(:albums frontier)))
            (when-not (and (zero? @(:artists frontier))
                           (zero? @(:albums frontier)))
              (recur (inc step) num-processed)))))
      ;; wait for artists & albums to report they're finished
      (dotimes [_ 2]
        (<!! done-chan))
      processed)))

(defn run
  ([] (run nil))
  ([_]
   (System/setProperty "clojure.core.async.pool-size" (str num-io-threads))
   (let [client (api/new-client {:id client-id :secret client-secret})
         categories (api/all-categories client)
         playlists-for-categories (pmapcat #(api/category-playlists client (:id %)) categories)
         bootstrapped-tracks (->> playlists-for-categories
                                  (pmapcat #(api/playlist-tracks client (:id %)))
                                  (map :track))
         initial-ds {:frontier {:artists (->> bootstrapped-tracks
                                              (mapcat :artists)
                                              (map :id)
                                              (apply sorted-set))
                                :albums (->> bootstrapped-tracks
                                             (map :album)
                                             (map :id)
                                             (apply sorted-set))}
                     :processed {:artists (atom #{})
                                 :albums (atom {})}}
         final-data (run-async client initial-ds)
         tracks-by-day (->> final-data
                            :albums
                            vals
                            (apply concat)
                            (group-by (comp time/->date :release_date))
                            (map-vals (fn [as]
                                        {:albums (count as)
                                         :tracks (->> as (map :tracks) (apply +))})))]
     (spit "full.edn" final-data)
     (spit "output.edn" tracks-by-day))))

(comment
  (let [client (api/new-client {:id client-id :secret client-secret})
        category (-> (api/all-categories client) first)
        playlist (-> (api/category-playlists client (:id category)) first)
        tracks (->> (api/playlist-tracks client (:id playlist)) (map :track))
        album-ids (->> tracks
                       (map :album)
                       (map :id)
                       (apply sorted-set)
                       (take 5))]
    (api/albums client album-ids))

  (run)

  (loop [attempt 0]
    (println (format "attempt %s" attempt))
    (let [result
          (try
            (run)
            true
            (catch Throwable t
              (println "Failed...")
              (clojure.pprint/pprint t)
              false))]
      (if result
        (println "Done!!!")
        (recur (inc attempt))))))