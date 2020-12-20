(ns main
  (:require [clojure.core.async :refer [<! <!! >! chan go go-loop put! timeout]]
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
                                         (put! artist-chan id)))))
          artist-albums (->> artist
                             (api/artist-albums client)
                             (keep (fn [{:keys [id]}]
                                     (when-not (contains? @(:albums processed) id)
                                       (put! album-chan id)))))]
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

(def chan-size 2000000)

(defn run-async
  [client {:keys [frontier processed] :as data}]
  (let [{:keys [artist-chan album-chan done-chan] :as chans}
        {:artist-chan (chan chan-size)
         :album-chan (chan chan-size)
         :done-chan (chan 2)}]
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
        (when-let [artist (<! artist-chan)]
          (go (process-artist client data artist chans))
          (if (zero? @(:artists frontier))
            (>! done-chan :artists)
            (recur))))
      ;; album go-loop
      (go-loop
        [albums-to-process []]
        (when (or (= (count albums-to-process) 20)
                  (zero? @(:albums frontier)))
          (go (process-albums client data albums-to-process)))
        (if (zero? @(:albums frontier))
          (>! done-chan :albums)
          (let [new-album (<! album-chan)
                albums-to-process (if (= (count albums-to-process) 20)
                                    [new-album]
                                    (conj albums-to-process new-album))]
            (recur albums-to-process))))
      ;; progress go-loop
      (go-loop
        [step 0
         last {:artists 0 :albums 0}]
        (<! (timeout 5000))
        (let [num-processed {:artists (count @(:artists processed))
                             :albums (count @(:albums processed))}]
          (println (format "%s: \tprocessed %s (%s total) artists (%s/m), %s (%s total) albums (%s/m)"
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
                              12)))
          (println (format "\t\t%s artists remaining, %s albums remaining"
                           @(:artists frontier)
                           @(:albums frontier)))
          (when-not (and (zero? @(:artists frontier))
                         (zero? @(:albums frontier)))
            (recur (inc step) num-processed))))
      ;; wait for artists & albums to report they're finished
      (dotimes [_ 2]
        (<!! done-chan))
      processed)))

(def key->step-size
  {:artists 40
   :albums 200})

(defn take-from-frontier
  [frontier key]
  (->> (get frontier key) (take (key->step-size key))))

;; total API calls per call to run-step
(comment
  (let [api-calls-per-step (+ (* 2 (key->step-size :artists)) (/ (key->step-size :albums) 20))
        avg-s-per-step 15]
    {:api-calls-per-step api-calls-per-step
     :api-calls-per-second (float (/ api-calls-per-step avg-s-per-step))}))

;; a smarter algorithm would use core/async to ensure we're saturating
;; our i/o channels. but that would break our cache
;; (which depends on album/artist sort order)
(defn run-step
  [client {:keys [frontier processed]}]
  ;; for each artist in frontier:
  ;;   get their albums & add to album frontier
  ;;   get their related artists & add to artist frontier
  ;; for each album in frontier:
  ;;   get detailed album information
  (let [artists-to-process (take-from-frontier frontier :artists)
        albums-to-process (take-from-frontier frontier :albums)

        related-artists (->> artists-to-process
                             (pmapcat #(api/related-artists client %))
                             (map :id))
        artist-albums (->> artists-to-process
                           (pmapcat #(api/artist-albums client %))
                           (map :id))

        albums (->> albums-to-process
                    (partition-all 20)
                    (pmapcat #(api/albums client %))
                    (group-by :id)
                    (map-vals #(-> % first (update :tracks count) (select-keys [:release_date :tracks]))))

        new-processed-artists (swap! (:artists processed) set/union artists-to-process)
        new-processed-albums (swap! (:albums processed) merge albums)
        new-frontier {:artists (set/difference
                                 (set/union
                                   (:artists frontier)
                                   (apply sorted-set related-artists))
                                 new-processed-artists)
                      :albums (set/difference
                                (set/union
                                  (:albums frontier)
                                  (apply sorted-set artist-albums))
                                (keys new-processed-albums))}]
    {:frontier new-frontier
     :processed processed}))

(comment

  @(var-get #'clojure.core.async.impl.exec.threadpool/pool-size)
  (System/setProperty "clojure.core.async.pool-size" (str num-io-threads))
  (Long/getLong "clojure.core.async.pool-size")
  )

(defn run
  []
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
        #_#_final-data (loop [data initial-ds
                          step 0]
                     (let [before (time/now)
                           new-data (run-step client data)
                           after (time/now)
                           expected-steps (int (max (/ (-> new-data :frontier :artists count)
                                                       (key->step-size :artists))
                                                    (/ (-> new-data :frontier :albums count)
                                                       (key->step-size :albums))))]
                       (println (format "%s Step %s/%s (%s%%) (%sms): %s (%s) artists, %s (%s) albums in frontier; %s artists, %s albums processed"
                                        (time/now)
                                        step
                                        expected-steps
                                        (float (/ (Math/round (* 1000 100 (float (/ step expected-steps)))) 1000))
                                        (time/milliseconds-ago before after)
                                        (-> new-data :frontier :artists count)
                                        (format-diff (- (-> new-data :frontier :artists count)
                                                        (-> data :frontier :artists count)))
                                        (-> new-data :frontier :albums count)
                                        (format-diff (- (-> new-data :frontier :albums count)
                                                        (-> data :frontier :albums count)))
                                        (-> new-data :processed :artists count)
                                        (-> new-data :processed :albums keys count)))
                       (if (->> new-data :frontier vals (every? empty?))
                         (:processed new-data)
                         (recur new-data (inc step)))))
        tracks-by-day (->> final-data
                           :albums
                           vals
                           (apply concat)
                           (group-by (comp time/->date :release_date))
                           (map-vals (fn [as]
                                       {:albums (count as)
                                        :tracks (->> as (map :tracks) (apply +))})))]
    (spit "full.edn" final-data)
    (spit "output.edn" tracks-by-day)))

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