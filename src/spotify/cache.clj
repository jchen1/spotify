(ns spotify.cache
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [spotify.common :refer [base-url]])
  (:import [java.security MessageDigest]
           [java.io PushbackReader]
           [java.math BigInteger]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

;; Simple disk cache. Could gzip contents if we become too space-constrained

(def cache-save-base-dir "cache")
(def gzip-cache-save-base-dir "gzip-cache")

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
    {:key (str cache-save-base-dir "/" filename ".edn")
     :gzip-key (str gzip-cache-save-base-dir "/" filename ".edn.gz")}))

(defn cache-hit?
  [url]
  (let [{:keys [key gzip-key]} (url->cache-key url)]
    (or (-> key io/file .exists)
        (-> gzip-key io/file .exists))))

(defn cache-set!
  [url result]
  (let [{:keys [gzip-key]} (url->cache-key url)]
    (io/make-parents gzip-key)
    (with-open [output (-> gzip-key io/file io/output-stream GZIPOutputStream. )]
      (io/copy (pr-str result) output))))

(defn cache-get
  [url]
  (let [{:keys [key gzip-key]} (url->cache-key url)
        gzip-file (io/file gzip-key)
        regular-file (io/file key)]
    (cond
      (.exists gzip-file)
      (with-open [input (-> gzip-file io/input-stream GZIPInputStream. io/reader PushbackReader. )]
        (edn/read input))
      (.exists regular-file)
      (let [res (-> regular-file slurp edn/read-string)]
        (cache-set! url res)
        (io/delete-file regular-file)
        res)
      :else
      (throw (ex-info "cache miss!" {:url url
                                     :key key
                                     :gzip-key gzip-key})))))
