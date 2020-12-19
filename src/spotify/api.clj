(ns spotify.api
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [http :as http]
            [time :as time]))

(def base-url "https://api.spotify.com/v1")

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
  ([client url opts]
   (let [_ (maybe-refresh-client! client)
         url (if (string/starts-with? url "http") url (str base-url url))]
     (-> (http/get url (merge {:bearer-auth (-> client :token deref :token)}
                              opts))
         :body
         (json/parse-string true)))))

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