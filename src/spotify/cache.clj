(ns spotify.cache
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [spotify.common :refer [base-url]])
  (:import [java.security MessageDigest]
           [java.math BigInteger]))

(def cache-save-base-dir "cache")

(defn md5 [^String s]
  (let [algorithm (MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes s))]
    (format "%032x" (BigInteger. 1 raw))))

(defn- url->cache-key
  [url]
  (let [filename (-> url
                     (string/replace (re-pattern base-url) "")
                     (string/replace #"\?" "_QMARK_")
                     (string/replace #"&" "_AMP_")
                     md5)]
    (str cache-save-base-dir "/" filename ".edn")))

(defn cache-hit?
  [url]
  (-> url url->cache-key io/file .exists))

(defn cache-get
  [url]
  (-> url url->cache-key slurp edn/read-string))

(defn cache-set!
  [url result]
  (let [cache-key (url->cache-key url)]
    (io/make-parents cache-key)
    (spit cache-key
          (pr-str result))))