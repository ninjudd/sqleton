(ns sqleton.connection
  (:use cake
        [bake.core :only [in-cake-jvm?]]
        [clojure.contrib.string :only [join split]]
        [clojure.contrib.sql :only [with-connection]]))

(def *datasource* nil)

(defn load-user-config [name]
  (let [overrides (java.io.File. (str (System/getProperty "user.home") "/.sqleton/" (:artifact-id *project*) ".clj"))]
    (when (.exists overrides)
      (get (read-string (slurp overrides)) name))))

(defn datasource
  "Returns a map appropriate for either a jndi or traditional data source."
  [name]
  (with-meta
    (if (or (in-cake-jvm?) (nil? (get-in *project* [:db name :name])))
      (let [defaults (get-in *project* [:db name])
            config   (merge defaults (load-user-config name))
            {:keys [host port database user password subprotocol]} config]
        {:classname   "org.postgresql.Driver"
         :subprotocol subprotocol
         :user        (or user (System/getProperty "user.name"))
         :password    password
         :subname     (str "//" host ":" (or port 5432) "/" database)})
      (get-in *project* [:db name]))
    {:name name}))

(defmacro with-db
  "Create a connection to a specific db if there is not one for this thread already."
  [name & forms]
  `(if (= ~name (:name (meta *datasource*)))
     (do ~@forms)
     (binding [*datasource* (datasource ~name)]
       (with-connection *datasource*
         ~@forms))))