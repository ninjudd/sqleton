(ns sqleton.core
  (:require [clojure.contrib.sql :as sql]))

(defn execute-file [file]
  (sql/do-commands (slurp file)))