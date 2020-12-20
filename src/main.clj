(ns main
  (:require [clojure.edn :as edn]
            [spotify.api :as api]
            [clojure.set :as set]))

;; put your client id & secret into `env.edn`
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
  {:artists 40
   :albums 200})

(defn take-from-frontier
  [frontier key]
  (->> (get frontier key) sort (take (key->step-size key))))

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

(defn format-diff
  [diff]
  (str (if (pos? diff) "+" "") diff))

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
                     (let [before (time/now)
                           new-data (run-step client data)
                           after (time/now)
                           expected-steps (int (max (/ (-> new-data :frontier :artists count)
                                                       (key->step-size :artists))
                                                    (/ (-> new-data :frontier :albums count)
                                                       (key->step-size :albums))))]
                       (println (format "Step %s/%s (%s%%) (%sms): %s (%s) artists, %s (%s) albums in frontier; %s artists, %s albums processed"
                                        step
                                        expected-steps
                                        (float (/ (Math/round (* 100 100 (float (/ step expected-steps)))) 100))
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
    (spit "full.edn" final-data)
    (spit "output.edn" tracks-by-day)))

(comment
  (run))