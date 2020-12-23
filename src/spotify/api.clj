(ns spotify.api
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [http :as http]
            [spotify.cache :as c]
            [spotify.common :refer [base-url]]
            [time :as time])
  (:import [java.security MessageDigest]
           [java.math BigInteger]))

(defn md5 [^String s]
  (let [algorithm (MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes s))]
    (format "%032x" (BigInteger. 1 raw))))

(defn- url->cache-key
  [url]
  (let [filename (-> url
                     ;; not actually nececssary - but i want to preserve my cache!
                     (string/replace (re-pattern base-url) "")
                     (string/replace #"\?" "_QMARK_")
                     (string/replace #"&" "_AMP_")
                     md5)]
    (str filename ".edn.gz")))

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
  [{:keys [token] :as client}]
  (cond-> client
          (or (nil? @token)
              (time/< (time/now) (-> @token :expires-at)))
          (refresh))
  client)

(defn- retry-request-fn
  [{:keys [rate-limited-until] :as client} {:keys [status headers]}]
  (or
    (and (>= status 500) (<= status 599))
    (and (= status 401)
         (refresh client))
    (and (= status 429)
         (let [retry-after (-> (get headers "retry-after" "0") Integer/parseInt inc)
               until (time/plus-time (time/now) {:seconds retry-after})
               [old-limit] (swap-vals! rate-limited-until (fn [old] (if (or (nil? old) (time/> until old)) until old)))]
           (when (> (time/milliseconds-ago old-limit until) 1000)
             (println (format "Rate limited until %s" until)))
           (Thread/sleep (* 1000 retry-after))
           true))))

(defn- request
  ([client url] (request client url {}))
  ([{:keys [token rate-limited-until in-flight-requests] :as client} url {:keys [use-cache?] :as opts}]
   (let [use-cache? (not= false use-cache?)
         url (if (string/starts-with? url "http") url (str base-url url))
         cache-key (url->cache-key url)]
     (or
       (when (and use-cache?
                  (c/cache-hit? cache-key))
         (c/cache-get cache-key))
       (do
         (when-let [until @rate-limited-until]
           (Thread/sleep (max 0 (time/milliseconds-ago (time/now) until)))
           (compare-and-set! rate-limited-until until nil))
         (swap! in-flight-requests inc)
         (try
           (maybe-refresh-client! client)
           (let [{:keys [body status]} (http/get url (merge {:bearer-auth (:token @token)
                                                             :retry-request-fn (partial retry-request-fn client)}
                                                            opts))
                 result (json/parse-string body true)]
             (when (and use-cache? (http/unexceptional-status? status))
               (c/cache-set! cache-key result))
             result)
           (finally
             (swap! in-flight-requests dec))))))))

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

(defn- album->cache-key
  [album-id]
  (md5 (format "/albums/%s" album-id)))

(defn cached-album
  [album-id]
  (c/cache-get (album->cache-key album-id)))

(defn albums
  [client album-ids]
  ;; uri-encoded "," is "%2C"
  (if (> (count album-ids) 0)
    (let [{cached true uncached false} (group-by #(-> % album->cache-key c/cache-hit?) album-ids)
          uncached-result (when (not-empty uncached)
                            (->> (request client (format "/albums?ids=%s" (string/join "%2C" (sort uncached))))
                                 :albums))
          cached-result (->> cached (map #(-> % album->cache-key c/cache-get)))]
      (->> uncached-result (map #(c/cache-set! (album->cache-key (:id %)) %)) doall)
      (concat cached-result uncached-result))
    []))

(defn new-client
  [{:keys [id secret]}]
  (maybe-refresh-client! {:id id
                          :secret secret
                          :token (atom nil)
                          :rate-limited-until (atom nil)
                          :in-flight-requests (atom 0)}))