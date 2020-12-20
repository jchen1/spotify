(ns util)

(defn pmapcat
  [f d]
  (->> d
       (pmap f)
       (apply concat)))

(defn map-vals
  [f m]
  (->> m
       (map (fn [[k v]]
              [k (f v)]))
       (into {})))

(defn format-diff
  [diff]
  (str (if (pos? diff) "+" "") diff))