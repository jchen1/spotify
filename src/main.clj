(ns main
  (:require [clojure.edn :as edn]
            [spotify.api :as api]
            [clojure.set :as set]))

(def env (-> "env.edn" slurp edn/read-string))

(def client-id (env :spotify-client-id))
(def client-secret (env :spotify-client-secret))

(defn pmapcat
  [f d]
  (->> d
       (pmap f)
       (apply concat)))

(defn map-vals
  [f m]
  (->> m
       (map (fn [[k v]]
              [k (f v)]))
       (into {})))

(def key->step-size
  {:artists 20
   :albums 200})

(defn take-from-frontier
  [frontier key]
  (->> (get frontier key) sort (take (key->step-size key))))

(defn run-step
  [client {:keys [frontier processed]}]
  ;; get albums from an artist
  ;; get related artists from an artist
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
                    (map-vals first))

        new-processed-artists (set/union (:artists processed) artists-to-process)
        new-processed-albums (merge (:albums processed) albums)
        new-frontier {:artists (set/difference
                                 (set/union
                                   (:artists frontier)
                                   (set related-artists))
                                 new-processed-artists)
                      :albums (set/difference
                                (set/union
                                  (:albums frontier)
                                  (set artist-albums))
                                (keys new-processed-albums))}]
    {:frontier new-frontier
     :processed {:artists new-processed-artists
                 :albums new-processed-albums}}))

(defn run
  []
  (let [client (api/new-client {:id client-id :secret client-secret})
        categories (api/all-categories client)
        playlists-for-categories (pmapcat #(api/category-playlists client (:id %)) categories)
        bootstrapped-tracks (->> playlists-for-categories
                                 (pmapcat #(api/playlist-tracks client (:id %)))
                                 (map :track))
        initial-ds {:frontier {:artists (->> bootstrapped-tracks
                                             (mapcat :artists)
                                             (map :id)
                                             set)
                               :albums (->> bootstrapped-tracks
                                            (map :album)
                                            (map :id)
                                            set)}
                    :processed {:artists #{}
                                :albums {}}}
        final-data (loop [data initial-ds
                          step 0]
                     (println (format "Step %s: %s artists, %s albums in frontier; %s artists, %s albums processed"
                                      step
                                      (-> data :frontier :artists count)
                                      (-> data :frontier :albums count)
                                      (-> data :processed :artists count)
                                      (-> data :processed :albums keys count)))
                     (let [new-data (run-step client data)]
                       (if (->> new-data :frontier vals (every? empty?))
                         new-data
                         (recur new-data (inc step)))))
        tracks-by-day (->> final-data
                           :processed
                           :albums
                           vals
                           (apply concat)
                           (group-by (comp time/->date :release_date))
                           (map-vals (fn [as]
                                       {:albums (count as)
                                        :tracks (->> as (mapcat :tracks) count)})))]
    (spit "output.edn" tracks-by-day)))

(comment
  (run)
  (let [client (api/new-client {:id client-id :secret client-secret})
        playlist-id "37i9dQZF1DXcBWIGoYBM5M"
        tracks (api/playlist-tracks client playlist-id)]
    (:album (:track (first tracks)))))