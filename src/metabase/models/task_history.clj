(ns metabase.models.task-history
  (:require [metabase.models.interface :as i]
            [metabase.util :as u]
            [toucan
             [db :as db]
             [models :as models]]))

(models/defmodel TaskHistory :task_history)

(defn cleanup-task-history!
  "Deletes older TaskHistory rows. Will order TaskHistory by `ended_at` and delete everything after
  `num-rows-to-keep`. This is intended for a quick cleanup of old rows."
  [num-rows-to-keep]
  (db/simple-delete! TaskHistory :id [:in {:select   [:id]
                                           :from     [TaskHistory]
                                           :offset   num-rows-to-keep
                                           :order-by [[:ended_at :desc]]}]))

(u/strict-extend (class TaskHistory)
  models/IModel
  (merge models/IModelDefaults
         {:types (constantly {:task_details :json, :engine :keyword})})
  i/IObjectPermissions
  (merge i/IObjectPermissionsDefaults
         {:can-read?  i/superuser?
          :can-write? i/superuser?}))
