(ns metabase.api.meta.table-test
  "Tests for /api/meta/table endpoints."
  (:require [expectations :refer :all]
            [metabase.db :refer :all]
            [metabase.http-client :as http]
            [metabase.models.field :refer [Field]]
            [metabase.test-data :refer :all]
            [metabase.test.util :refer [deserialize-dates match-$]]))

;; ## GET /api/meta/table?org
;; These should come back in alphabetical order and include relevant metadata
(expect [{:description nil, :entity_type nil, :name "CATEGORIES", :rows 75, :entity_name nil, :active true, :id (table->id :categories), :db_id @db-id}
         {:description nil, :entity_type nil, :name "CHECKINS", :rows 1000, :entity_name nil, :active true, :id (table->id :checkins), :db_id @db-id}
         {:description nil, :entity_type nil, :name "USERS", :rows 15, :entity_name nil, :active true, :id (table->id :users), :db_id @db-id}
         {:description nil, :entity_type nil, :name "VENUES", :rows 100, :entity_name nil, :active true, :id (table->id :venues), :db_id @db-id}]
  (->> ((user->client :rasta) :get 200 "meta/table" :org @org-id)
       (map #(dissoc % :db :created_at :updated_at)))) ; don't care about checking nested DB, and not sure how to compare `:created_at` / `:updated_at`

;; ## GET /api/meta/table/:id/fields
(expect [(match-$ (sel :one Field :id (field->id :categories :name))
           {:description nil
            :table_id (table->id :categories)
            :special_type nil
            :name "NAME"
            :updated_at $
            :active true
            :id (field->id :categories :name)
            :field_type "dimension"
            :position 0
            :preview_display true
            :created_at $
            :base_type "TextField"})
         (match-$ (sel :one Field :id (field->id :categories :id))
           {:description nil
            :table_id (table->id :categories)
            :special_type nil
            :name "ID"
            :updated_at $
            :active true
            :id (field->id :categories :id)
            :field_type "dimension"
            :position 0
            :preview_display true
            :created_at $
            :base_type "BigIntegerField"})]
  (->> ((user->client :rasta) :get 200 (format "meta/table/%d/fields" (table->id :categories)))
       (map (fn [field]
              (deserialize-dates field :created_at :updated_at)))))
