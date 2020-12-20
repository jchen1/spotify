(ns spotify.cache
  (:refer-clojure :exclude [key])
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

(defn- key->full-key
  [key]
  (str cache-save-base-dir "/" key))

(defn cache-hit?
  [key]
  (-> key key->full-key io/file .exists))

(defn cache-set!
  [key result]
  (let [key (key->full-key key)]
    (io/make-parents key)
    (with-open [output (-> key io/file io/output-stream GZIPOutputStream.)]
      (io/copy (pr-str result) output))))

(defn cache-get
  [key]
  (let [f (-> (key->full-key key) io/file)]
    (when (.exists f)
      (with-open [input (-> f io/input-stream GZIPInputStream. io/reader PushbackReader.)]
        (edn/read input)))))
