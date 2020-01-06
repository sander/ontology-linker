(ns nl.sanderdijkhuis.ontology.next
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [datascript.core :as d])
  (:import (java.io File)))

(defn read-definitions! [^File path]
  (into [] (comp (filter #(.isFile %))
                 (filter #(not= (.getAbsolutePath path) (.getParent %)))
                 (map io/reader)
                 (map #(json/read % :key-fn keyword))) (file-seq path)))

(defn add-indices []
  (map-indexed #(assoc %2 :db/id (- (inc %1)))))

(defn is-a-references-tx [db]
  (map (fn [[e a]] [:db/add e :concept/is-a a])
       (d/q '[:find ?e ?a :where [?e :schema "concept"] [?e :is-a ?a-id] [?a :id ?a-id]] db)))

(defn references-references-tx [db]
  (into [] (comp (map (fn [[concept source {:keys [note]}]]
                        (into {} (filter val) {:reference/concept concept :reference/source source :reference/note note})))
                 (add-indices))
        (d/q '[:find ?concept ?source (pull ?reference [:note])
               :where [?concept :references ?reference] [?reference :key ?key] [?source :id ?key]] db)))

(defn inputs-references-tx [db]
  (->> db
       (d/q '[:find ?concept (pull ?port [:description :name]) ?port-concept
              :where [?concept :inputs ?port] [?port :type ?type] [?port-concept :id ?type]])
       (map (fn [[concept {:keys [description name]} type]]
              {:port/input-for concept :port/name name :port/description description :port/type type}))
       (map (fn [m] (into {} (filter val) m)))
       (into [] (add-indices))))

(defn outputs-references-tx [db]
  (->> db
       (d/q '[:find ?concept (pull ?port [:description :name]) ?port-concept
              :where [?concept :outputs ?port] [?port :type ?type] [?port-concept :id ?type]])
       (map (fn [[concept {:keys [description name]} type]]
              {:port/output-for concept :port/name name :port/description description :port/type type}))
       (map (fn [m] (into {} (filter val) m)))
       (into [] (add-indices))))

(defn external-references-tx [db]
  (->> db
       (d/q '[:find ?concept ?references
              :where [?concept :external ?references]])
       (mapcat (fn [[concept refs]]
                 (map (fn [[key id]]
                        {:external/to concept :external/site (name key) :external/key id
                         :external/url (case key
                                         :wikipedia (str "https://en.wikipedia.org/wiki/" id)
                                         :wikidata (str "https://www.wikidata.org/wiki/" id))})
                      refs)))
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

(def schema
  {:is-a                  {:db/cardinality :db.cardinality/many}
   :concept/is-a          {:db/cardinality :db.cardinality/many}
   :references            {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :annotation/definition {:db/isComponent true :db/valueType :db.type/ref}
   :sexp/arguments        {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :sexp/atom             {:db/valueType :db.type/ref}
   :reference/concept     {:db/valueType :db.type/ref}
   :inputs                {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :outputs               {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :port/input-for        {:db/valueType :db.type/ref}
   :port/output-for       {:db/valueType :db.type/ref}
   :port/type             {:db/valueType :db.type/ref}
   :external/to           {:db/valueType :db.type/ref}})

(comment
  (let [input "/home/sander/src/datascienceontology/build"
        conn (d/create-conn schema)]
    (->> (read-definitions! (io/as-file input))
         (mapcat #(cond (map? %) [%] (vector? %) %))
         (into [] (add-indices))
         (expand-definitions)
         (map add-path)
         (map add-edit-link)
         (d/transact! conn))
    (doseq [tx [is-a-references-tx references-references-tx inputs-references-tx outputs-references-tx external-references-tx]]
      (d/transact! conn (tx @conn)))
    (d/q '[:find [(pull ?e [:name :description :kind :definition :edit/link :path/absolute
                            {(:reference/_concept :as :concept/references) [{:reference/source [:DOI :author :issued :publisher :title :type :edition :title-short :volume :container-title :issue] } :reference/note]}
                            {(:port/_input-for :as :concept/inputs) [:port/name :port/description {:port/type [:name :path/absolute]}]}
                            {(:port/_output-for :as :concept/outputs) [:port/name :port/description {:port/type [:name :path/absolute]}]}
                            {(:external/_to :as :concepts/external-links) [:external/key :external/site :external/url]}
                            {:concept/is-a [:name :path/absolute]}])
                  ...]
           :where [?e :schema "concept"] [?e :references]]
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
