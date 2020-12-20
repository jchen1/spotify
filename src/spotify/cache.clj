(ns spotify.cache
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [spotify.common :refer [base-url]])
  (:import [java.security MessageDigest]
           [java.io PushbackReader]
           [java.math BigInteger]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

;; simple GZIP cache. keys are md5 hashes

(def cache-save-base-dir "gzip-cache")

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
    (str cache-save-base-dir "/" filename ".edn.gz")))

(defn cache-hit?
  [url]
  (let [gzip-key (url->cache-key url)]
    (-> gzip-key io/file .exists)))

(defn cache-set!
  [url result]
  (let [gzip-key (url->cache-key url)]
    (io/make-parents gzip-key)
    (with-open [output (-> gzip-key io/file io/output-stream GZIPOutputStream.)]
      (io/copy (pr-str result) output))))

(defn cache-get
  [url]
  (let [gzip-key (url->cache-key url)
        gzip-file (io/file gzip-key)]
    (if
      (.exists gzip-file)
      (with-open [input (-> gzip-file io/input-stream GZIPInputStream. io/reader PushbackReader.)]
        (edn/read input))
      (throw (ex-info "cache miss!" {:url url
                                     :gzip-key gzip-key})))))
