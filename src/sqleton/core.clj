(ns sqleton.core
  (:require [clojure.java.jdbc :as sql]))

(defn execute-file [file]
  (sql/do-commands (slurp file)))