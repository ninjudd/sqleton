(ns flatland.sqleton.connection
  (:use [clojure.string :only [join]]
        [clojure.java.jdbc :only [with-connection]]
        [flatland.useful.exception :only [exception-map]])
  (:import com.jolbox.bonecp.BoneCPDataSource))

(def ^:dynamic *datasource* nil)

(defonce config (atom {}))

(defn jdbc-path [{:keys [host port database]}]
  (str "//" host ":" (or port 5432) "/" database))

(defn add-subname [datasource]
  (if (:subname datasource)
    datasource
    (assoc datasource
      :subname (jdbc-path datasource))))

(defn jdbc-uri [config]
  (str "jdbc:postgresql:" (jdbc-path config)))

(defn bonecp-datasource* [{:keys [host port database user password] :as config}]
  (let [ds (BoneCPDataSource.)]
    (.setJdbcUrl ds (jdbc-uri config))
    (when user (.setUsername ds user))
    (when password (.setPassword ds password))
    ds))

(def bonecp-datasource (memoize (fn [config]
                                  (assoc config
                                    :datasource (bonecp-datasource* config)))))

(def last-exception (atom false))

(defn overrides [source-name]
  (into {} (for [key [:host :port :database :user :password]
                 :let [value (System/getProperty (join "." (map name ["sqleton" source-name key])))]
                 :when value]
             [key value])))

(defn datasource
  "Returns a map appropriate for either a jndi or traditional data source."
  [source-name]
  (let [defaults (get-in @config [:db source-name])]
    (-> (if-let [pool-config (:pooled defaults)]
          (bonecp-datasource (merge pool-config (overrides source-name)))
          (into {:classname   "org.postgresql.Driver"
                 :subprotocol "postgresql"}
                (if (:name defaults)
                  defaults
                  (add-subname (merge defaults
                                      (overrides source-name))))))
        (with-meta {:name source-name}))))

(def pg-connection-error #{"08000" "08001" "08003" "08004" "08006" "28000" "3D000"})

(defmacro with-db
  "Create a connection to a specific db if there is not one for this thread already."
  [source-name & forms]
  `(if (= ~source-name (:name (meta *datasource*)))
     (do ~@forms)
     (binding [*datasource* (datasource ~source-name)]
       (try
         (with-connection *datasource*
           ~@forms)
         (catch java.sql.SQLException e#
           (reset! last-exception {:time (System/currentTimeMillis)
                                   :exception e#})
           (if (pg-connection-error (.getSQLState e#))
             (throw (java.sql.SQLException.
                     (str "Could not connect to: " (pr-str *datasource*)) e#))
             (throw e#)))))))