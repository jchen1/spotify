(ns http
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as http])
  (:import [java.io IOException]))

(defn http-exception
  [params result]
  (let [host (try (-> params :url http/parse-url :server-name)
                  (catch Throwable t (str (.getMessage t) "getting host")))]
    (ex-info (format "http <%s> error %d" host (:status result))
             (merge result
                    (select-keys params [:url])))))

(defn unexceptional-status?
  [status]
  (http/unexceptional-status? status))

(defn request
  "Wrapper around clj-http. The options are passed in through to clj-http except for
   - retries (default 3) which indicates how many attempts the request should make (cumulatively between bad HTTP responses
     and exceptions. NB: Any retries that occur as a result of a :retry-handler you pass in will not count towards this count.
   - retry-request-fn (default is (= 5xx status)) which should be a predicate which will be passed the result
     of the request and should determine it should be allowed to be retried."
  [{:keys [url method debug retries retry-request-fn throw-exceptions? bearer-auth]
    :or {throw-exceptions? true}
    :as params}]
  (let [params (merge {:conn-timeout 60000
                       :socket-timeout 60000}
                      params)
        retries (or retries 3)
        headers (merge (:headers params)
                       (when bearer-auth {"Authorization" (format "Bearer %s" bearer-auth)}))
        http-params (merge (when debug {:debug true :debug-body true})
                           (-> params
                               (assoc :throw-exceptions? false) ;;we don't throw so that we can handle ourselves
                               (dissoc :retries :successful-request-fn :silence?))
                           {:headers headers})
        retry #(request (assoc params :retries (dec retries)))
        retry-request-fn (or retry-request-fn http/server-error?)
        ;; make the request
        result (try (http/request http-params)
                    ;; only catch IOException since that is what clj-http retry-handler will catch
                    (catch IOException ioe ioe))]
    (cond
      ;; if clj-http had an IOException, we might retry, or rethrow
      (instance? IOException result) (if (< 0 retries)
                                       (retry)
                                       (throw result))

      ;; check if we should retry
      (and (< 0 retries) (retry-request-fn result)) (retry)

      ;; if it's not valid and we can throw exceptions, then throw, otherwise just return result
      (and throw-exceptions?
           (not (unexceptional-status? (:status result))))
      (throw (http-exception params result))

      :else result)))

(defn get
  ([url] (get url {}))
  ([url opts]
   (request (merge opts {:method :get :url url}))))

(defn post
  ([url] (post url {}))
  ([url opts]
   (request (merge opts {:method :post :url url}))))
