(ns spotify.cache
  (:refer-clojure :exclude [key])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [spotify.common :refer [base-url]])
  (:import [java.io PushbackReader]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

;; simple GZIP cache. keys are md5 hashes

(def cache-save-base-dir "/Users/jchen/Documents/Projects/spotify-cache")

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
  ([key] (cache-get key false))
  ([key delete-on-fail?]
   (let [f (-> (key->full-key key) io/file)]
     (when (.exists f)
       (try
         (with-open [input (-> f io/input-stream GZIPInputStream. io/reader PushbackReader.)]
           (edn/read input))
         (catch Throwable t
           (when delete-on-fail?
             (io/delete-file f true))
           nil))))))