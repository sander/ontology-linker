(ns nl.sanderdijkhuis.ontology.next
  "Functions to create JSON-LD from an ontology structured like:
   https://github.com/IBM/datascienceontology"
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.walk :as walk]
            [datascript.core :as d]
            [clojure.string :as str])
  (:import (java.io File)))

(defn- add-unique-temporary-ids [] (map-indexed #(assoc %2 :db/id (- (inc %1)))))
(defn- replace-key-if-found [k f] (fn [x] (if-let [v (k x)] (merge (f v) (dissoc x k)) x)))

(defn read-definitions!
  "Reads json files recursively from path into a flat vector with keyword keys."
  [^File path]
  (into [] (comp (filter #(.isFile %))
                 (filter #(not= (.getAbsolutePath path) (.getParent %)))
                 (map io/reader)
                 (map #(json/read % :key-fn keyword)))
        (file-seq path)))

(defn transaction-to-set-is-a-entity-ids
  "Sets :concept/is-a to entity IDs based on :is-a."
  [db]
  (map (fn [[e a]] [:db/add e :concept/is-a a])
       (d/q '[:find ?e ?a :where [?e :schema "concept"] [?e :is-a ?a-id] [?a :id ?a-id] [?a :schema "concept"]] db)))

(defn transaction-to-define-references
  "Defines new entities based on concept :references."
  [db]
  (into [] (comp (map (fn [[concept source {:keys [note]}]]
                        (into {} (filter val) {:reference/concept concept :reference/source source :reference/note note})))
                 (add-unique-temporary-ids))
        (d/q '[:find ?concept ?source (pull ?reference [:note])
               :where [?concept :references ?reference] [?reference :key ?key] [?source :id ?key]] db)))

#_(defn transaction-to-define-ports
    "Defines new entities based on :inputs and :outputs."
    [db]
    (into [] (comp
                 (map (fn [x] (println x) x))
                 (map (fn [[concept {:keys [description name slot]} type field]])
                      {({:inputs :port/input-for :outputs :port/output-for} field) concept
                       :port/name name
                       :port/description description
                       :port/type type
                       :port/slot slot})
                 (map #(into {} (filter val) %))
                 (add-unique-temporary-ids))
            (d/q '[:find ?concept (pull ?port [:description :name :slot]) ?port-concept ?field
                   :in $ [?field ...]
                   :where [?concept ?field ?port] #_[?port :type ?type] #_[?port-concept :id ?type]]
                 db [:inputs :outputs])))

(defn transaction-to-define-external-links
  "Defines new entities based on :external."
  [db]
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
       (into [] (add-unique-temporary-ids))))

(defn update-with-linked-sexp-definitions
  "Updates a list of entities with linked sexp definitions where applicable."
  [x]
  (let [index (into {} (comp (filter (comp #{"concept"} :schema)) (map (juxt :id :db/id))) x)]
    (letfn [(expand [d] (if (vector? d)
                          (let [[h & t] d] {:sexp/operation h :sexp/arguments (map expand t)})
                          {:sexp/atom (index d)}))]
      (map (fn [{d :definition :as v}] (if d (assoc v :annotation/definition (expand d)) v)) x))))

(defn add-absolute-path
  [{s :schema i :id lang :language pkg :package :as x}]
  (assoc x :path/absolute (case s
                            "concept" [:concepts/by-id i]
                            "annotation" [:annotations/by-language-package-id lang pkg i]
                            [:references/by-id i])))

(defn add-edit-link
  [{s :schema id :id lang :language pkg :package :as x}]
  (let [prefix "https://github.com/IBM/datascienceontology/blob/master/"]
    (merge x (case s
               "concept" {:link/edit (str prefix "concept/" id ".yml")}
               "annotation" {:link/edit (str prefix "annotation/" lang "/" pkg "/" id ".yml")}
               nil))))

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
   :external/to           {:db/valueType :db.type/ref}
   :slots {:db/isComponent true :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :slot/definition {:db/valueType :db.type/ref}})

(def concept-pattern
  [:name :description :kind :link/edit :path/absolute
   {(:reference/_concept :as :concept/references) [{:reference/source [:DOI :author :issued :publisher :title :type
                                                                       :edition :title-short :volume :container-title
                                                                       :issue]}
                                                   :reference/note]}
   {(:port/_input-for :as :definition/inputs) [:port/name :port/description {:port/type [:name :path/absolute]}]}
   {(:port/_output-for :as :definition/outputs) [:port/name :port/description {:port/type [:name :path/absolute]}]}
   {(:external/_to :as :concepts/external-links) [:external/key :external/site :external/url]}
   {:concept/is-a [:name :path/absolute]}
   {:annotation/definition [{:sexp/atom [:name :id :path/absolute]} :sexp/operation {:sexp/arguments '...}]}
   {:definition/slots [:slot/definition :slot]}])

(defn relativize [base path]
  (loop [result [] wd (drop-last 1 base)]
    (cond
      (= wd path) result
      (= wd (drop-last 1 path)) (conj result (last path))
      (empty? wd) (concat result path)
      :else (recur (conj result :..) (drop-last 1 wd)))))

(defn path->iri [path]
  (str/join \/ (map #(case %
                       :.. ".."
                       :concepts/by-id "concepts"
                       :annotations/by-language-package-id "annotations"
                       :index "index"
                       :concepts/alphabetical-by-name (str (namespace %) \- (name %))
                       :annotations/grouped-by-language-and-package (str (namespace %) \- (name %))
                       %) path)))

(defn doc->json-filename-and-content
  [output]
  (fn [{b :path/absolute :as r}]
    [(str (io/as-file output) \/ (path->iri b) ".json")
     (json/write-str
       (walk/postwalk (replace-key-if-found :path/absolute (fn [id] {"@id" (str (path->iri (relativize b id)) ".json")}))
                      (dissoc r :path/absolute)))]))

(defn- concept-index [x] (into {} (comp (filter (comp #{"concept"} :schema)) (map (juxt :id :db/id))) x))

#_
(defn update-with-linked-slot-types
  [x]
  (let [index (concept-index x)]
    (map (replace-key-if-found :slots (fn [slots]
                                        (println :slots slots)
                                        {:definition/slots (map (replace-key-if-found :definition (fn [d] {:slot/definition (index d)}))
                                                                slots)}))
         x)))

(defn transaction-to-define-ports
  "Defines new entities based on :inputs and :outputs."
  [db]
  (into [] (comp
             #_(map (fn [x] (println x) x))
             (filter (constantly false))
             #_(map (fn [[concept {:keys [description name slot]} type field]]
                      {({:inputs :port/input-for :outputs :port/output-for} field) concept
                       :port/name name
                       :port/description description
                       :port/type type
                       :port/slot slot}))
             #_(map #(into {} (filter val) %))
             #_(add-unique-temporary-ids))
        (d/q '[:find ?concept (pull ?port [:description :name :slot :type :definition])
               :in $ [?field ...]
               :where [?concept ?field ?port] #_[?port :type ?type] #_[?port-concept :id ?type]]
             db [:inputs :outputs])))

(defn concepts-alphabetical-by-name [db]
  {:path/absolute [:concepts/alphabetical-by-name]
   ::letters
   (->> db
        (d/q '[:find [(pull ?e [:name :path/absolute]) ...] :where [?e :schema "concept"]])
        (sort-by :name)
        (group-by (comp str/lower-case first :name))
        (sort-by first)
        (map (fn [[letter concepts]] {::letter letter ::concepts concepts})))})

(defn annotations-grouped-by-language-and-package [db]
  {:path/absolute [:annotations/grouped-by-language-and-package]
   ::languages
   (->> db
        (d/q '[:find [(pull ?e [:language :package :name :path/absolute]) ...] :where [?e :schema "annotation"]])
        (sort-by :path/absolute)
        (group-by (fn [{:keys [:language :package]}] [language package]))
        (sort-by first)
        (map (fn [[package annotations]] {::package package ::annotations annotations}))
        (group-by (comp first ::package))
        (map (fn [[language packages]] {::language language ::packages packages})))}
  #_(into [] (->> (vals annotations)
                  (sort-by annotation-name)
                  (map (fn [{:keys [language package id] :as a}]
                         {:name (annotation-name a) ::local-absolute-id [:annotations/by-id language package id]}))
                  (group-by #(let [{[_ language package] ::local-absolute-id} %] [language package]))
                  (sort-by first)
                  (map (fn [[package annotations]] {::package package ::annotations annotations}))
                  (group-by (comp first ::package))
                  (map (fn [[language packages]] {::language language ::packages packages})))))

(def index {:path/absolute [:index]
            :ontology/title "Data Science Ontology"
            :indices [{:index/name "Index of concepts" :path/absolute [:annotations/grouped-by-language-and-package]}
                      {:index/name "Index of annotations" :path/absolute [:concepts/alphabetical-by-name]}]})

(comment
    (let [input "/home/sander/src/datascienceontology/build"
          output "/home/sander/src/datascienceontology/json-ld"
          conn (d/create-conn schema)]
      (->> (read-definitions! (io/as-file input))
           (mapcat #(cond (map? %) [%] (vector? %) %))
           (into [] (add-unique-temporary-ids))
           (update-with-linked-sexp-definitions)
           #_(update-with-linked-slot-types)
           #_(filter :definition/slots)
           #_(map pprint)
           (map add-absolute-path)
           (map add-edit-link)
           (d/transact! conn))
      (doseq [tx [transaction-to-set-is-a-entity-ids
                  transaction-to-define-references
                  transaction-to-define-ports
                  transaction-to-define-external-links]]
        (d/transact! conn (tx @conn)))
      #_(concepts-alphabetical-by-name @conn)
      #_(annotations-grouped-by-language-and-package @conn)
      #_(into [] (->> (vals concepts)
                      (sort-by :id)
                      (map (fn [{:keys [id]}] {::local-absolute-id [:concepts/by-id id]}))
                      (group-by (comp str/lower-case first second ::local-absolute-id))
                      (sort-by first)
                      (map (fn [[letter concepts]] {::letter letter ::concepts concepts}))))
      (->>
           (conj (d/q `[:find [(~@['pull '?e concept-pattern]) ...] :in ~@'[$ [?schema ...]] :where ~'[?e :schema ?schema]]
                      @conn ["concept" "annotation"])
                 (concepts-alphabetical-by-name @conn)
                 (annotations-grouped-by-language-and-package @conn)
                 index)
           #_(filter (constantly false))
           (map (doc->json-filename-and-content output))
           (map (fn [[path json]] (doto path io/make-parents (spit json)))))))

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
         (into [] (add-unique-temporary-ids))
         update-with-linked-sexp-definitions
         (map add-absolute-path)
         (d/transact! conn))
    #_(d/transact! conn (update-with-linked-sexp-definitions @conn))
    (d/q '[:find (pull ?e [:id :name :schema {:annotation/definition [{:sexp/atom [:name :id :path/absolute]} :sexp/operation {:sexp/arguments ...}]}])
           :where [?e :id :foo]]
         @conn)))
