(ns nl.sanderdijkhuis.ontology.next
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.pprint :refer [pprint]]
            [datascript.core :as d])
  (:import (java.io File)
           (java.net URI)))

(defn read-definitions! [^File path]
  (into [] (comp (filter #(.isFile %))
                 (filter #(not= (.getAbsolutePath path) (.getParent %)))
                 (map io/reader)
                 (map #(json/read % :key-fn keyword))) (file-seq path)))

(defn is-a-references [db]
  (->> db
       (d/q '[:find ?e ?a
              :where [?e :schema "concept"] [?e :is-a ?a-id] [?a :id ?a-id]])
       (map (fn [[e a]] [:db/add e :concept/is-a a]))))

(defn add-indices []
  (map-indexed #(assoc %2 :db/id (- (inc %1)))))

(defn references-references [db]
  (->> db
       (d/q '[:find ?concept ?source (pull ?reference [:note])
              :where [?concept :references ?reference] [?reference :key ?key] [?source :id ?key]])
       (map (fn [[concept source {:keys [note]}]]
              (if note
                {:reference/concept concept :reference/source source :reference/note note}
                {:reference/concept concept :reference/source source})))
       (into [] (add-indices))))

(defn expand-definitions [x]
  (let [index (into {} (comp (filter (comp #{"concept"} :schema)) (map (juxt :id :db/id))) x)]
    (letfn [(expand [d] (if (vector? d)
                          (let [[h & t] d]
                            {:sexp/operation h
                             :sexp/arguments (map expand t)})
                          (if-let [v (index d)]
                            {:sexp/atom (index d)}
                            (throw (Exception. (str "could not find '" d "' while expanding"))))))]
      (map (fn [{d :definition :as v}]
             (if d
               (-> v (assoc :annotation/definition (expand d)) (dissoc :definition))
               v))
       x))))

#_
(defn xp-desc []
  (fn [xf]
    (fn
      ([] (xf))
      ([result] (xf (expand-definitions result)))
      ([result input] (xf result input)))))

(defn add-path [{s :schema i :id lang :language pkg :package :as x}]
  (assoc x :path/absolute (case s
                            "concept" [:concepts/by-id i]
                            "annotation" [:annotations/by-language-package-id lang pkg i]
                            [:references/by-id i])))

(defn add-edit-link [{s :schema id :id lang :language pkg :package :as x}]
  (let [prefix "https://github.com/IBM/datascienceontology/blob/master/"]
    (case s
      "concept" (assoc x :edit/link (str prefix "concept/" id ".yml"))
      "annotation" (assoc x :edit/link (str prefix "annotation/" lang "/" pkg "/" id ".yml"))
      x)))

(comment
  (let [input "/home/sander/src/datascienceontology/build"
        schema {:is-a                  {:db/cardinality :db.cardinality/many}
                :concept/is-a          {:db/cardinality :db.cardinality/many}
                :references            {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                :annotation/definition {:db/isComponent true :db/valueType :db.type/ref}
                :sexp/arguments        {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                :sexp/atom             {:db/valueType :db.type/ref}
                :reference/concept     {:db/valueType :db.type/ref}}
        conn (d/create-conn schema)]
    (->> (read-definitions! (io/as-file input))
         (mapcat #(cond (map? %) [%] (vector? %) %))
         (into [] (add-indices))
         expand-definitions
         (map add-path)
         (map add-edit-link)
         (d/transact! conn))
    (d/transact! conn (is-a-references @conn))
    (d/transact! conn (references-references @conn))
    (d/q '[:find [(pull ?e [:id :name :description :kind :definition
                            {(:reference/_concept :as :concept/references)
                             [{:reference/source [:DOI :author :issued :publisher :title :type :edition :title-short :volume :container-title :issue]}
                              :reference/note]}
                            :db/id
                            {:concept/is-a [:name :db/id]}
                            :external :kind :inputs :outputs
                            :edit/link])
                  ...]
           :where [?e :schema "concept"] [?e :inputs]]
         @conn)))

;; create index :db/id -> absolute-path, then walk through each doc and replace :db/id with relative-path

(comment
  (let [data [{:id :foo :description [:compose [:product [:id :transformation-model] [:copy :data]]
                                      [:id :data]]
               :name "f o o"
               :schema "concept"}
              {:id :transformation-model :schema "concept" :name "transformation model"}
              {:id :data :schema "concept" :name "d a t a"}]
        schema {:annotation/definition {:db/isComponent true :db/valueType :db.type/ref}
                :sexp/arguments     {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                :sexp/atom          {:db/valueType :db.type/ref}}
        conn (d/create-conn schema)]
    (->> data
         (into [] (add-indices))
         expand-definitions
         (map add-path)
         (d/transact! conn))
    #_(d/transact! conn (expand-definitions @conn))
    (d/q '[:find (pull ?e [:id :name :schema {:annotation/definition [{:sexp/atom [:name :id :path/absolute]} :sexp/operation {:sexp/arguments ...}]}])
           :where [?e :id :foo]]
         @conn)))
