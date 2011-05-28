(ns tasks
  (:use cake cake.core
        (sqleton core connection)
        [clojure.contrib.sql :as sql]))

(defn prepare [db]
  (with-db db
    (when-not (sql/with-query-results result ["SELECT name FROM migrations LIMIT 1"]
                (first result))
      (do (println "creating sqleton migrations table...")
        (do-commands "CREATE TABLE migrations (
                      name        varcha [[]] r(255),
                      migrated_at timestamp without time zone DEFAULT now() NOT NULL,
                      user        varchar(30),
                      PRIMARY KEY(name)
                    );")))))

(deftask migrate
  (doseq [db (keys (:db *project*))]
    (prepare db)))