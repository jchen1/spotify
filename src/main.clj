(ns main
  (:require [spotify.api :as api]))

(defn run
  [{:keys [id secret]}]
  (let [client (api/new-client {:id id :secret secret})
        categories (api/all-categories client)]
    (clojure.pprint/pprint categories)))

(comment
  (run {:id "4e455fe0875740219120ca8a229af120" :secret "e6c4bae002e54d0d849966f92e088c8b"}))