(ns flatland.sqleton.connection
  (:use [clojure.string :only [join]]
        [clojure.java.jdbc :only [with-connection]]
        [flatland.useful.exception :only [exception-map]]))

(def ^:dynamic *datasource* nil)

(def config (atom {}))

(def last-exception (atom false))

(defn overrides [source-name]
  (into {} (for [key [:host :port :database :user :password]
                 :let [value (System/getProperty (join "." (map name ["sqleton" source-name key])))]
                 :when value]
             [key value])))

(defn add-subname [{:keys [host port database] :as datasource}]
  (if (:subname datasource)
    datasource
    (assoc datasource
      :subname (str "//" host ":" (or port 5432) "/" database))))

(defn datasource
  "Returns a map appropriate for either a jndi or traditional data source."
  [source-name]
  (let [defaults (get-in @config [:db source-name])]
    (with-meta
      (into {:classname   "org.postgresql.Driver"
             :subprotocol "postgresql"}
            (if (:name defaults)
              defaults
              (add-subname (merge defaults
                                  (overrides source-name)))))
      {:name source-name})))

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