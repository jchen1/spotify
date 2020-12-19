(ns spotify.api
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [http :as http]
            [spotify.cache :as c]
            [spotify.common :refer [base-url]]
            [time :as time]))

(defn- refresh
  [{:keys [id secret] :as client}]
  (let [{:keys [access_token expires_in]} (-> (http/post "https://accounts.spotify.com/api/token"
                                                         {:basic-auth [id secret]
                                                          :form-params {:grant_type "client_credentials"}})
                                              :body
                                              (json/parse-string true))]
    (reset! (:token client) {:token access_token
                             :expires-at (time/plus-time (time/now) {:seconds expires_in})})))

(defn- maybe-refresh-client!
  [client]
  (cond-> client
          (or (nil? @(:token client))
              (time/< (time/now) (-> client :token deref :expires-at)))
          (refresh))
  client)

(defn- retry-request-fn
  [{:keys [status headers]}]
  (and (= 429 status)
       (let [retry-after (-> (get headers "retry-after" "0") Integer/parseInt inc)]
         (Thread/sleep (* 1000 retry-after))
         true)))

(defn- request
  ([client url] (request client url {}))
  ([client url {:keys [use-cache?] :as opts}]
   (let [use-cache? (not= false use-cache?)
         url (if (string/starts-with? url "http") url (str base-url url))]
     (if (and use-cache?
              (c/cache-hit? url))
       (c/cache-get url)
       (do
         (maybe-refresh-client! client)
         (let [{:keys [body status]} (http/get url (merge {:bearer-auth (-> client :token deref :token)
                                                           :retry-request-fn retry-request-fn}
                                                          opts))
               result (json/parse-string body true)]
           (when (and use-cache? (http/unexceptional-status? status))
             (c/cache-set! url result))
           result))))))

(defn- request-all
  ([client url] (request client url {}))
  ([client url {:keys [keypath] :as opts}]
   (let [result (request client url opts)
         keypath (cond->> keypath (keyword? keypath) (conj []))
         {:keys [limit next offset previous total items]} (get-in result keypath)]
     (if (nil? next)
       items
       (concat items
               (request-all client next opts))))))

(defn all-categories
  [client]
  (request-all client "/browse/categories" {:keypath :categories}))

(defn category-playlists
  [client category-id]
  (try
    (request-all client (format "/browse/categories/%s/playlists" category-id) {:keypath :playlists})
    (catch Throwable t
      (if (= (some-> t ex-data :status) 404)
        []
        (throw t)))))

(defn playlist-tracks
  [client playlist-id]
  (->> (request-all client (format "/playlists/%s/tracks" playlist-id) {:keypath nil})
       (filter #(= "track" (-> % :track :type)))))

(defn related-artists
  [client artist-id]
  (->> (request client (format "/artists/%s/related-artists" artist-id))
       :artists))

(defn artist-albums
  [client artist-id]
  (request-all client (format "/artists/%s/albums" artist-id) {:keypath nil}))

(defn albums
  [client album-ids]
  ;; uri-encoded "," is "%2C"
  (if (> (count album-ids) 0)
    (->> (request client (format "/albums?ids=%s" (string/join "%2C" (sort album-ids))))
         :albums)
    []))

(defn new-client
  [{:keys [id secret]}]
  (maybe-refresh-client! {:id id
                          :secret secret
                          :token (atom nil)}))