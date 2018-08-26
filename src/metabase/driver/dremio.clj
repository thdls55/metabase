(ns metabase.driver.dremio
  (:require [clojure
             [string :as str]]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.dremio.generic-sql :as sql]
            [metabase.util
             [honeysql-extensions :as hx]
             [ssh :as ssh] ]))



(defrecord DremioDriver []
  clojure.lang.Named
  (getName [_] "Dremio"))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  METHOD IMPLS                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- column->base-type
  [column-type]
  ({:BOOLEAN       :type/Boolean
    :VARBINARY     :type/*
    :DATE          :type/Date
    :FLOAT         :type/Float
    :DECIMAL       :type/Decimal
    :DOUBLE        :type/Float
    :INTERVAL      :type/DateTime
    :INT           :type/Integer 	
    :BIGINT        :type/BigInteger
    :TIME          :type/Time
    :TIMESTAMP     :type/DateTime
    :VARCHAR       :type/Text
    (keyword "BIT VARYING")                :type/*
    (keyword "CHARACTER VARYING")          :type/Text
    (keyword "DOUBLE PRECISION")           :type/Float
    :MAP           :type/* 	
    :LIST          :type/*} column-type))

(defn- connection-details->spec
  [{:keys [host port dbname]
    :or   {host "localhost", port 31010, dbname ""}
    :as   opts}]
  (merge {:classname   "com.dremio.jdbc.Driver"
          :subprotocol "dremio"
          :subname     (str "direct=" host ":" port ";schema=" dbname)}
         (dissoc opts :host :port :dbname)))

(defn- date [unit expr]
  (case unit
    :default         expr))

(defn- string-length-fn [field-key]
  (hsql/call :char_length (hx/cast :VARCHAR field-key)))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
  (case seconds-or-milliseconds
    :seconds      (hsql/call :to_timestamp expr)
    :milliseconds (recur (hx// expr 1000) :seconds)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                        IDRIVER & ISQLDRIVER METHOD MAPS                                        |
;;; +----------------------------------------------------------------------------------------------------------------+

(u/strict-extend DremioDriver
  driver/IDriver
  (merge
   (sql/IDriverSQLDefaultsMixin)
   {
    :details-fields                    (constantly (ssh/with-tunnel-config
                                                     [driver/default-host-details
                                                      (assoc driver/default-port-details :default 31010)
                                                      driver/default-dbname-details
                                                      driver/default-user-details
                                                      driver/default-password-details
                                                      ]))

})
  sql/ISQLDriver
  (merge
   (sql/ISQLDriverDefaultsMixin)
   {:column->base-type         (u/drop-first-arg column->base-type)
    :connection-details->spec  (u/drop-first-arg connection-details->spec)
    :date                      (u/drop-first-arg date)
    :string-length-fn          (u/drop-first-arg string-length-fn)
    :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(driver/register-driver! :dremio (DremioDriver.))
