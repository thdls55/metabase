(ns metabase.task.task-history-cleanup
  (:require [clojure.tools.logging :as log]
            [metabase.task :as task]
            [clojurewerkz.quartzite
             [jobs :as jobs]
             [triggers :as triggers]]
            [metabase.models.task-history :as thist]
            [puppetlabs.i18n.core :refer [trs]]
            [clojurewerkz.quartzite.schedule.cron :as cron]))

(def ^:private ^:const job-key     "metabase.task.task-history-cleanup.job")
(def ^:private ^:const trigger-key "metabase.task.task-history-cleanup.trigger")

(defonce ^:private job     (atom nil))
(defonce ^:private trigger (atom nil))

(def ^:private ^:const history-rows-to-keep 100000)

(jobs/defjob TaskHistoryCleanup
  [ctx]
  (log/debug "Cleaning up task history")
  (let [result (thist/cleanup-task-history! history-rows-to-keep)]
    (log/debug (trs "Task history cleanup successful, rows were {0} deleted"
                    (when-not result (trs "not"))))))

(defn task-init
  "Job initialization"
  []
  ;; build our job
  (reset! job (jobs/build
               (jobs/of-type TaskHistoryCleanup)
               (jobs/with-identity (jobs/key job-key))))
  ;; build our trigger
  (reset! trigger (triggers/build
                   (triggers/with-identity (triggers/key trigger-key))
                   (triggers/start-now)
                   (triggers/with-schedule
                     ;; run every day at midnight
                     (cron/cron-schedule "0 0 * * * ? *"))))
  ;; submit ourselves to the scheduler
  (task/schedule-task! @job @trigger))
