(ns nl.sanderdijkhuis.ontology.linker
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.io File)))

(defn read-definitions! [^File path]
  (into [] (comp (filter #(.isFile %))
                 (filter #(not= (.getAbsolutePath path) (.getParent %)))
                 (map io/reader)
                 (map #(json/read % :key-fn keyword))) (file-seq path)))

(defn concept? [doc] (= "concept" (:schema doc)))
(defn annotation? [doc] (= "annotation" (:schema doc)))
(defn annotation-name [{:keys [id name]}] (or name id))

(defn concepts-alphabetical-by-name [{concepts :concepts/by-id}]
  (->> (vals concepts)
       (sort-by :id)
       (map (fn [{:keys [id]}] {::local-absolute-id [:concepts/by-id id]}))
       (group-by (comp str/lower-case first second ::local-absolute-id))
       (sort-by first)
       (map (fn [[letter concepts]] {::letter letter ::concepts concepts}))))

(defn annotations-grouped-by-language-and-package [{annotations :annotations/by-id}]
  (->> (vals annotations)
       (sort-by annotation-name)
       (map (fn [{:keys [language package id] :as a}]
              {:name (annotation-name a) ::local-absolute-id [:annotations/by-id language package id]}))
       (group-by #(let [{[_ language package] ::local-absolute-id} %] [language package]))
       (sort-by first)
       (map (fn [[package annotations]] {::package package ::annotations annotations}))
       (group-by (comp first first))
       (map (fn [[language packages]] {::language language ::packages packages}))))

(def index {:annotations {::local-absolute-id [:annotations/grouped-by-language-and-package]}
            :concepts {::local-absolute-id [:concepts/alphabetical-by-name]}})

(defn list->db [defs]
  {:index index
   :concepts/by-id    (into {} (comp (filter concept?) (map (juxt :id identity))) defs)
   :annotations/by-id (into {} (comp (filter annotation?) (map (juxt (juxt :language :package :id) identity))) defs)})

(defn db->indexed-db [db]
  (assoc db :concepts/alphabetical-by-name (concepts-alphabetical-by-name db)
            :annotations/grouped-by-language-and-package (annotations-grouped-by-language-and-package db)))

(defn db->files [db]
  (into {} (mapcat (fn [[key docs]]
                     (cond
                       (#{:concepts/by-id :annotations/by-id} key) (map (fn [[id doc]] [(flatten [key id]) doc]) docs)
                       :else [[[key] docs]]))
                   db)))

(defn relativize [base path]
  (loop [result [] wd (drop-last 1 base)]
    (cond
      (= wd path) result
      (= wd (drop-last 1 path)) (conj result (last path))
      (empty? wd) (concat result path)
      :else (recur (conj result :..) (drop-last 1 wd)))))

(defn local-absolute-id->relative-id [base]
  #(if-let [path (::local-absolute-id %)]
     {::local-relative-id (relativize base path)}
     %))

(defn path->iri [path]
  (str/join \/ (map #(case %
                       :.. ".."
                       :concepts/by-id "concepts"
                       :annotations/by-id "annotations"
                       :index "index"
                       :concepts/alphabetical-by-name (str (namespace %) \- (name %))
                       :annotations/grouped-by-language-and-package (str (namespace %) \- (name %))
                       %) path)))

(defn enrich-definition [{c :definition :as v}]
  (if c (assoc v :definition {::local-absolute-id [:concepts/by-id c]}) v))

(defn enrich-slots [{s :slots :as v}]
  (if s (assoc v :slots (map enrich-definition s)) v))

(defn enrich-isa [{a :is-a :as v}]
  (if a (assoc v :is-a (map (fn [s] {::local-absolute-id [:concepts/by-id s]}) (flatten [a]))) v))

(def enrich (comp enrich-definition enrich-slots enrich-isa))

(defn build-linked-data!
  "Reads recursively a folder of JSON concept definitions and code annotations, writes JSON-LD to output."
  [input output]
  (->> (read-definitions! (io/as-file input))
       (map enrich)
       list->db
       db->indexed-db
       db->files
       (into {} (map (fn [[base doc]]
                       [base (walk/postwalk (local-absolute-id->relative-id base) doc)])))
       (walk/postwalk #(if-let [id (::local-relative-id %)]
                         {"@id" (str (path->iri id) ".json") :name (last id)}
                         %))
       (map (fn [[id val]] [(str (io/as-file output) \/ (path->iri id) ".json") (json/write-str val)]))
       (map (fn [[path json]] (doto path io/make-parents (spit json))))))

(comment
  (build-linked-data! "/home/sander/src/datascienceontology/build"
                      "/home/sander/src/datascienceontology/json-ld"))