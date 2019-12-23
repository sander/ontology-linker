# Ontology linker

Creates JSON-LD from a built [Data Science Ontology](https://www.datascienceontology.org/).

This JSON-LD can be published, for example as part of a continuous integration workflow, to make sharing and browsing easier.

To use:

    (use 'nl.sanderdijkhuis.ontology.linker)

    (build-linked-data! "/home/sander/src/datascienceontology/build"
                        "/home/sander/src/datascienceontology/json-ld")
