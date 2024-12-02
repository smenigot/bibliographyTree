# bibliographyTree : Analyse de Graphes d'Articles Scientifiques

Ce projet permet de créer et d'analyser un graphe de relations entre articles scientifiques à partir de mots-clés et de catégories de journaux. Le graphe est construit en utilisant des métadonnées d'articles comme les DOI (identifiants d'articles), leurs titres, auteurs, années de publication et journaux. Les relations entre les articles sont extraites à partir de leurs références bibliographiques et citations.

## Objectifs

L'objectif principal de ce projet est de :

- **Construire un graphe dirigé** basé sur les articles scientifiques, où les nœuds sont les articles et les arêtes représentent les relations entre ces articles (références et citations).
- **Analyser ce graphe** pour détecter des communautés d'articles, identifier les articles les plus centraux selon diverses métriques (comme la centralité de degré), et visualiser ces relations.

## Fonctionnalités

- **Création du graphe** : À partir d'une liste de DOI et des métadonnées associées, le graphe des relations entre les articles est construit. Les articles sont reliés par des arêtes basées sur leurs citations et références bibliographiques.
  
- **Filtrage par mots-clés et catégories de journaux** : Le graphe peut être construit en sélectionnant des articles en fonction de mots-clés ou de catégories spécifiques de journaux.

- **Analyse de centralité** : Le projet offre des outils pour analyser la centralité des articles dans le graphe, par exemple, la centralité de degré, permettant d'identifier les articles les plus influents.

- **Détection de communautés** : Utilisation d'algorithmes de détection de communautés (comme l'algorithme de modularité glouton) pour identifier des groupes d'articles qui sont fortement interconnectés.

- **Visualisation** : Le graphe est visualisé avec des nœuds colorés en fonction de l'année de publication, et des tailles proportionnelles à leur nombre de connexions (degré).


