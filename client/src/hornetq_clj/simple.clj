(ns hornetq-clj.simple
  (:require [hornetq-clj.core-client :as core]
            [clojure.tools.logging :as log])
  (:import  [org.hornetq.api.core.client ClientSession]
            [java.util UUID]))

(def session (atom nil))

(def session-factory (atom nil))

(def session-identifier (atom nil))

(defn init
  [{:keys [host port user password identifier]
    :or {:host "localhost" :port 5445 :user "guest" :password "guest"
         :identifier (str "." (UUID/randomUUID))}
    :as options}]
  (log/info "hq clj is starting , with options :" options)
  (reset! session-identifier identifier)
  (reset! session-factory (core/netty-session-factory {:host host :port port}))
  (reset! session (core/session @session-factory user password nil))
  (.start @session)
  ;;使用hqclient clj的程序停止的时候必须相应停止hq,
  ;;为免使用者没正确停止，将停止代码直接放在这里
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop)))

(defn stop
  []
  (log/info "hq clj is stopped!")
  (when @session
    (.close @session)
    (reset! session nil))
  (when @session-factory
    (.close @session-factory)
    (reset! session-factory nil)))

(defn queue-exists?
  [^ClientSession s ^String queue-name]
  (let [q (core/query-queue s queue-name)]
    (and (not (nil? q)) (.isExists q))))

(defn ensure-queue
  [queue-name]
  (when-not (queue-exists? @session (str queue-name @session-identifier))
    (core/ensure-queue @session
                       (str queue-name @session-identifier)
                       {:address queue-name})))

(defn listen
  [queue-name handle-fn]
  {:pre [@session]}
  (ensure-queue queue-name)
  (let [consumer (core/create-consumer @session
                                       (str queue-name @session-identifier)
                                       nil)
        handler (core/message-handler (fn [hq-msg]
                                        (let [message (core/read-message-string hq-msg)]
                                          (log/debug :queue-name queue-name :received-simple-message message)
                                          (handle-fn message))))]
    (.setMessageHandler consumer handler)))

(def get-producer
  (memoize
   (fn [queue-name]
     (ensure-queue queue-name)
     (core/create-producer @session queue-name))))

(defn publish
  [queue-name message]
  {:pre [@session]}
  (let [producer (get-producer queue-name)
        hq-msg (core/create-message @session false)]
    (log/debug :queue-name queue-name :send-simple-message message)
    (core/write-message-string hq-msg (str message))
    (core/send-message producer hq-msg queue-name)))


(comment
  (init {:host "192.168.0.173" :port 5445})
  (listen "greeting" prn)
  (publish "greeting" "hello,zzwu!"))
