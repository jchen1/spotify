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
          (or (nil? @(:token client)) (time/< (time/now) (-> client :token deref :expires-at)))
          (refresh))
  client)

(defn- request
  ([client url] (request client url {}))
  ([client url {:keys [use-cache?] :or {use-cache? true} :as opts}]
   (let [url (if (string/starts-with? url "http") url (str base-url url))]
     (if (and use-cache?
              (c/cache-hit? url))
       (c/cache-get url)
       (do
         (maybe-refresh-client! client)
         (let [result (-> (http/get url (merge {:bearer-auth (-> client :token deref :token)}
                                               opts))
                          :body
                          (json/parse-string true))]
           (when use-cache?
             (c/cache-set! url result)
             result)))))))

(defn- request-all
  ([client url] (request client url {}))
  ([client url {:keys [keyname] :as opts}]
   (let [result (request client url opts)
         {:keys [limit next offset previous total items]} (get result keyname)]
     (if (nil? next)
       items
       (concat items
               (request-all client next opts))))))

(defn all-categories
  [client]
  (request-all client "/browse/categories" {:keyname :categories}))

(defn category-playlists
  [client category-id]
  (request-all client (format "/browse/categories/%s/playlists" category-id) {:keyname :playlists}))

(defn new-client
  [{:keys [id secret]}]
  (maybe-refresh-client! {:id id
                          :secret secret
                          :token (atom nil)}))