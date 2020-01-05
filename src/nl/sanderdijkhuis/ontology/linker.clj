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
  (into [] (->> (vals concepts)
                (sort-by :id)
                (map (fn [{:keys [id]}] {::local-absolute-id [:concepts/by-id id]}))
                (group-by (comp str/lower-case first second ::local-absolute-id))
                (sort-by first)
                (map (fn [[letter concepts]] {::letter letter ::concepts concepts})))))

(defn annotations-grouped-by-language-and-package [{annotations :annotations/by-id}]
  (into [] (->> (vals annotations)
                (sort-by annotation-name)
                (map (fn [{:keys [language package id] :as a}]
                       {:name (annotation-name a) ::local-absolute-id [:annotations/by-id language package id]}))
                (group-by #(let [{[_ language package] ::local-absolute-id} %] [language package]))
                (sort-by first)
                (map (fn [[package annotations]] {::package package ::annotations annotations}))
                (group-by (comp first ::package))
                (map (fn [[language packages]] {::language language ::packages packages})))))

(def index {:annotations {::local-absolute-id [:annotations/grouped-by-language-and-package]}
            :concepts    {::local-absolute-id [:concepts/alphabetical-by-name]}})

(defn list->db [defs]
  {:concepts/by-id    (into {} (comp (filter concept?) (map (juxt :id identity))) defs)
   :annotations/by-id (into {} (comp (filter annotation?) (map (juxt (juxt :language :package :id) identity))) defs)})

(defn db->indexed-db [db]
  (assoc db :index             index
            :concepts/alphabetical-by-name (concepts-alphabetical-by-name db)
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

(defn link-sexp [s]
  (cond
    (string? s) {::local-absolute-id [:concepts/by-id s]}
    :else (let [[h & t] s] (cons h (map link-sexp t)))))

(defn enrich-definition [{c :definition :as v}]
  (if c (assoc v :definition (link-sexp c)) v))

(defn enrich-slots [{s :slots :as v}]
  (if s (assoc v :slots (map enrich-definition s)) v))

(defn enrich-isa [{a :is-a :as v}]
  (if a (assoc v :is-a (map (fn [s] {::local-absolute-id [:concepts/by-id s]}) (flatten [a]))) v))

(defn enrich-ontology [v]
  (if (map? v) (assoc v ::ontology {::local-absolute-id [:index]}) v))

(defn add-links-to-concept-inputs-or-outputs [is]
  (map (fn [{type :type :as i}]
         (if type (assoc i :type {::local-absolute-id [:concepts/by-id type]}) i))
       is))

(defn enrich-inputs [{is :inputs :as v}]
  (if is (assoc v :inputs (add-links-to-concept-inputs-or-outputs is)) v))

(defn enrich-outputs [{is :outputs :as v}]
  (if is (assoc v :outputs (add-links-to-concept-inputs-or-outputs is)) v))

(def enrich (comp enrich-definition enrich-slots enrich-isa enrich-ontology enrich-inputs enrich-outputs))

(defn find-index-route "from https://stackoverflow.com/a/45769560" [x coll]
  (letfn [(path-in [y]
            (cond
              (= y x) '()
              (coll? y) (let [[failures [success & _]]
                              (->> y
                                   (map path-in)
                                   (split-with not))]
                          (when success (cons (count failures) success)))))]
    (path-in coll)))

(defn incoming-links [db local-absolute-id])

(defn local-absolute-id? [m]
  (and (map? m) (= (count m) 1) (= (first (keys m)) ::local-absolute-id)))
(defn reverse-links [m]
  (letfn [(flatten-keys [m]
            (cond
              (local-absolute-id? m) {[] m}
              (vector? m) (into {}
                                (for [[k v] (map-indexed vector m)
                                      [ks v'] (flatten-keys v)]
                                  [(cons k ks) v']))
              (map? m) (into {}
                             (for [[k v] m
                                   [ks v'] (flatten-keys v)]
                               [(cons k ks) v']))
              :else {[] m}))]
    (->> (flatten-keys m)
         (filter (comp local-absolute-id? val))
         (map #(apply with-meta %))
         (group-by (comp ::local-absolute-id meta)))))

(defn db->db-with-reverse-links [db]
  (assoc db ::reverse-links (->> (reverse-links db)
                                 (filter #(not (#{[:index] [:annotations/grouped-by-language-and-package] [:concepts/alphabetical-by-name]} (key %))))
                                 (map (fn [[id links]]
                                        [id (into [] (comp (filter #(not (#{[:concepts/alphabetical-by-name]} (first %))))
                                                           #_(map (fn [[src prop]] {::property prop} src)))
                                                  links)]))
                                 (filter #(not (empty? (second %))))
                                 (into {})))
  ;; groeperen: id -> relatie (pak alleen eerste) -> brondoc (met context?)
  #_(map (fn [[index-key nodes]]
           [index-key (map (fn [[id node]]
                             [id (assoc node ::incoming-links (into {} (incoming-links db (flatten [index-key id]))))])
                        nodes)])
         db))

(defn build-linked-data!
  "Reads recursively a folder of JSON concept definitions and code annotations, writes JSON-LD to output."
  [input output]
  (->> (read-definitions! (io/as-file input))
       (map enrich)
       list->db
       db->indexed-db
       db->files
       db->db-with-reverse-links
       ::reverse-links

       #_(filter #(= "concept" (:schema (second %))))
       #_(into {} (map (fn [[base doc]]
                         [base (walk/postwalk (local-absolute-id->relative-id base) doc)])))
       #_(walk/postwalk #(if-let [id (::local-relative-id %)]
                           {"@id" (str (path->iri id) ".json") :name (last id)}
                           %))
       #_(map (fn [[id val]] [(str (io/as-file output) \/ (path->iri id) ".json") (json/write-str val)]))
       #_(map (fn [[path json]] (doto path io/make-parents (spit json))))))

(comment
  (build-linked-data! "/home/sander/src/datascienceontology/build"
                      "/home/sander/src/datascienceontology/json-ld"))

(comment
  (defn incoming [x coll]
    #_(loop [y coll results '()]
        (cond
          (= y x) (conj results '())
          (map? y)))
    (letfn [(path-in [y]
              (cond
                (= y x) '()
                (map? y)))]

      (path-in coll)))
  (defn local-absolute-id? [m]
    (and (map? m) (= (count m) 1) (= (first (keys m)) ::local-absolute-id)))
  (defn flatten-keys [m]
    (cond
      (local-absolute-id? m) {[] m}
      (vector? m) (into {}
                        (for [[k v] (map-indexed vector m)
                              [ks v'] (flatten-keys v)]
                          [(cons k ks) v']))
      (map? m) (into {}
                     (for [[k v] m
                           [ks v'] (flatten-keys v)]
                       [(cons k ks) v']))
      :else {[] m}))
  (defn reverse-links [m]
    (letfn [(flatten-keys [m]
              (cond
                (local-absolute-id? m) {[] m}
                (vector? m) (into {}
                                  (for [[k v] (map-indexed vector m)
                                        [ks v'] (flatten-keys v)]
                                    [(cons k ks) v']))
                (map? m) (into {}
                               (for [[k v] m
                                     [ks v'] (flatten-keys v)]
                                 [(cons k ks) v']))
                :else {[] m}))]
      (->> (flatten-keys m)
           (filter (comp local-absolute-id? val))
           (map #(apply with-meta %))
           (group-by (comp ::local-absolute-id meta)))))
  (def coll {:foo [:bar {::local-absolute-id :id}]
             :baz {::local-absolute-id :id2}
             2 [1 2 [3 {::local-absolute-id :id}]]})
  (reverse-links coll)
  (find-index-route :id {:foo [:bar {::local-absolute-id :id}]
                         :baz {::local-absolute-id :id}}))