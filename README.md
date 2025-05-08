# 🏷️ Siretizator Lite – Identifiez les SIRET en un clic

> Une application légère pour enrichir vos fichiers avec des identifiants SIRET fiables, à partir du nom et de la commune.

---

## 🚀 Essayez l'application

🟢 Application déployée ici :  
👉 **[Siretizator Lite →](https://siretizator.onrender.com/)**  

---

## 🎯 Objectif du projet

**Siretizator Lite** est un outil conçu pour faciliter l'enrichissement de bases de données clients en ajoutant les identifiants SIRET correspondants. À partir du nom de l'entreprise et de la commune, l'application utilise des techniques de correspondance floue pour identifier le SIRET le plus probable.

---

## 🧭 Schéma du pipeline

![Schéma du pipeline Siretizator Lite](assets/siretizator_pipeline.png)

Ce schéma illustre les étapes clés du processus :

1. **Chargement des données** : Importation du fichier CSV contenant les noms d'entreprises et les communes.
2. **Prétraitement** : Nettoyage et normalisation des données pour améliorer la qualité des correspondances.
3. **Correspondance floue** : Utilisation de l'algorithme RapidFuzz pour comparer les entrées avec la base SIRENE.
4. **Enrichissement** : Ajout des identifiants SIRET correspondants aux données initiales.
5. **Exportation** : Téléchargement du fichier enrichi au format CSV.

---

## 🧠 À propos des données

Les données utilisées proviennent de la base SIRENE, qui contient des informations sur les entreprises françaises. Pour des raisons de performance, une version allégée de cette base est utilisée dans cette application.

---

## 🛠️ Technologies utilisées

- **Python** : Langage principal de développement.
- **Streamlit** : Framework pour la création de l'interface utilisateur.
- **Pandas** : Manipulation et analyse des données.
- **RapidFuzz** : Algorithme de correspondance floue pour identifier les SIRET.
- **Base SIRENE** : Source des données d'entreprises.

---

## 👨‍💻 Auteur

Projet développé par **[Lucas Potin](https://lucaspotin98.github.io/)**  
*Data Scientist – Modélisation & Graphes*

