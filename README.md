# 🏷️ Siretizator Lite – Identifiez les SIRET en un clic

> Une application légère pour enrichir vos fichiers avec des identifiants SIRET fiables, à partir du nom et de la commune.

---

##  Essayez l'application

🟢 Application déployée ici :  
👉 **[Siretizator Lite →](https://siretizator.onrender.com/)**  

---

##  Objectif du projet

**Siretizator Lite** est un outil conçu pour faciliter l'enrichissement de bases de données clients en ajoutant les identifiants SIRET correspondants. À partir du nom de l'entreprise et de la commune, l'application utilise des techniques de correspondance floue pour identifier le SIRET le plus probable.

---

##  Schéma du pipeline

![Schéma Siretizator Lite](Schema_Siret.png)

Ce schéma illustre les étapes clés du processus :

### Étapes du traitement :

1. **Chargement des données**  
   Le fichier d’entrée contient les noms, villes, et éventuellement adresses ou codes postaux des entités à sirétiser.

2. **Nettoyage et normalisation**  
   Les noms et villes sont traités pour homogénéiser les chaînes (typographie, casse, accents…), à l’aide de `pandas`.

3. **Enrichissement métier**  
   Des variables comme le **code hexaposte** ou la **catégorie juridique** sont ajoutées pour guider le matching.

4. **Matching SIRENE**  
   Le cœur de la sirétisation repose sur un matching flou (`RapidFuzz`) entre les entités à enrichir et la base nationale des entreprises **SIRENE**.

5. **API finale**  
   Le tout est encapsulé dans une API `FastAPI`, accessible pour des requêtes individuelles ou par lot.

---

## Stack technique
- Python, pandas, RapidFuzz, FastAPI
- Déploiement sur Render

---

##  À propos des données

Les correspondances SIRET sont établies à partir de la base **Sirene®**, fournie par l’Insee.  
Cette base, accessible en open data, est publiée sous la **licence Etalab “Open Licence”**.

🔗 [sirene.fr](https://www.sirene.fr)  
🔗 [data.gouv.fr – Sirene](https://www.data.gouv.fr/fr/datasets/r/)

Pour des raisons de performance, cette application utilise une **version allégée** de la base Sirene, limitée aux **entités clientes dans les marchés publics** — c’est-à-dire les **acheteurs** tels que les municipalités, régions, ministères, etc.


---
###  Application dérivée d’un projet en production

Cette application est une version allégée du projet principal **Siretizator**, conçu pour retrouver les SIRET de n’importe quelle entité (entreprise, collectivité, association...) à partir de données partielles.  

Le processus de sirétisation utilisé ici a été développé durant ma thèse, dans le cadre de la création de la base **FOPPA** ([lien Zenodo](https://zenodo.org/records/10879932)), qui recense les marchés publics français publiés au TED entre 2010 et 2020.

📁 Découvrez les projets associés :  
- **[Siretizator – GitHub](https://github.com/LucasPotin98/Siretizator)**  
- **[FoppaInit – GitHub](https://github.com/LucasPotin98/FoppaInit)**

---

## 👨‍💻 Auteur

Projet développé par **[Lucas Potin](https://lucaspotin98.github.io/)**  
*Data Scientist – Modélisation & Graphes*

