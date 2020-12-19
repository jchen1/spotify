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

(def default-date-formatters
  ["M-d-yyyy"
   "M/d/yyyy"
   "M-yyyy"
   "yyyy-M-d"
   "yyyy-M"
   "yyyy-MM-dd'T'HH:mm:ssZ"
   "yyyy-MM-dd HH:mm:ss zzz"
   "yyyy-MM-dd HH:mm:ss"
   "EEE MMM dd HH:mm:ss zzz yyyy"
   "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   "M/d/yyyy H:mm:ss"])

(defn formatter
  [fmt]
  (when-let [formatter (cond
                         (string? fmt) (tf/formatter fmt)
                         (keyword? fmt) (get tf/formatters fmt))]
    (tf/with-locale formatter Locale/US)))

(defn ->date
  ([d]
   (cond
     (integer? d)
     (Date. ^long d)
     :else
     (->date d default-date-formatters)))
  ([d allowed-formatters]
   (let [allowed-formatters (if-not (vector? allowed-formatters)
                              [allowed-formatters]
                              allowed-formatters)]
     (if (string? d)
       (->> allowed-formatters
            (keep (fn [fmt] (formatter fmt)))
            (keep #(try
                     (tc/to-date (tf/parse % d))
                     (catch Exception _ nil)))
            (first))
       (when (some? d)
         (tc/to-date d)))))
  ([d allowed-formatters tz-id]
     (let [allowed-formatters (if-not (vector? allowed-formatters)
                                [allowed-formatters]
                                allowed-formatters)]
       (->> allowed-formatters
            (keep (fn [fmt] (tf/with-zone
                              (formatter fmt)
                              (t/time-zone-for-id tz-id))))
            (keep #(try
                     (tc/to-date (tf/parse % d))
                     (catch Exception _ nil)))
            (first)))))

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

(defn milliseconds-ago
  ([t] (milliseconds-ago t (now)))
  ([t nowish]
   (let [t (tc/from-date t)]
     (- (tc/to-long nowish)
        (tc/to-long t)))))

(defn seconds-ago
  ([t] (seconds-ago t (now)))
  ([t nowish]
   (quot (milliseconds-ago t nowish) 1000)))

(defn minutes-ago
  ([t] (minutes-ago t (now)))
  ([t nowish]
   (quot (seconds-ago t nowish) 60)))

(defn hours-ago
  ([t] (hours-ago t (now)))
  ([t nowish]
   (let [t (tc/from-date t)]
     (/ (- (tc/to-long nowish)
           (tc/to-long t))
        1000 60 60))))

(defn hours-until
  ([t]
   (hours-until t (now)))
  ([t nowish]
   (hours-ago nowish t)))

(defn days-ago
  ([t] (days-ago t (now)))
  ([t nowish]
   {:pre [(date? t) (date? nowish)]}
   (if (< nowish t)
     (- (days-ago nowish t))
     (t/in-days (t/interval (tc/to-date-time t)
                            (tc/to-date-time nowish))))))

(defn days-until
  ([t]
   (days-until t (now)))
  ([t nowish]
   (days-ago nowish t)))

(defn months-ago
  ([t] (months-ago t (now)))
  ([t nowish]
   (if (< nowish t)
     (- (months-ago nowish t))
     (t/in-months (t/interval (tc/to-date-time t)
                              (tc/to-date-time nowish))))))

(defn years-ago
  ([t] (years-ago t (now)))
  ([t nowish]
   (if (< nowish t)
     (- (years-ago nowish t))
     (t/in-years (t/interval (tc/to-date-time t)
                             (tc/to-date-time nowish))))))