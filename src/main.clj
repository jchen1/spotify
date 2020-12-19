(ns main
  (:require [clojure.edn :as edn]
            [spotify.api :as api]))

(def env (-> "env.edn" slurp edn/read-string))

(def client-id (env :spotify-client-id))
(def client-secret (env :spotify-client-secret))

(defn run
  []
  (let [client (api/new-client {:id client-id :secret client-secret})
        categories (api/all-categories client)
        playlists-for-categories (->> categories
                                      (pmap (fn [{:keys [id]}]
                                              (api/category-playlists client id)))
                                      (apply concat))]
    (clojure.pprint/pprint (count playlists-for-categories))))

(comment
  (run))