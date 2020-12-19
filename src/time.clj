(ns time
  (:refer-clojure :exclude [> >= < <= min max])
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as tf])
  (:import [java.text SimpleDateFormat]
           [java.util Date Locale]
           [org.joda.time DateTime DateTimeZone]))

(defn compare-op
  "A handy-dandy (compare) that sorts properly. e.g.

  (compare-op < 1 2 3 4) == (< 1 2 3 4), but done with (compare)"
  ([op x y] (op (compare x y) 0))
  ([op x y & args]
   (and (compare-op op x y)
        (apply compare-op op y args))))

(defn date?
  [d]
  (instance? Date d))

(defn >
  [& args]
  (apply compare-op clojure.core/> args))

(defn >=
  [& args]
  (apply compare-op clojure.core/>= args))

(defn <
  [& args]
  (apply compare-op clojure.core/< args))

(defn <=
  [& args]
  (apply compare-op clojure.core/<= args))

(defn plus-time
  "add n years/months/days/hours/mins/seconds/millis to a given date. to subtract, use negative integers"
  [date {:keys [years months days hours minutes seconds millis]
         :or {years 0 months 0 days 0 hours 0 minutes 0 seconds 0 millis 0} :as inc}]
  {:pre [(date? date)]
   :post [(= (type date) (type %))]}
  (-> (tc/from-date date)
      (t/plus (t/years years))
      (t/plus (t/months months))
      (t/plus (t/days days))
      (t/plus (t/hours hours))
      (t/plus (t/minutes minutes))
      (t/plus (t/seconds seconds))
      (t/plus (t/millis millis))
      (tc/to-date)))

(defn now
  []
  (Date.))