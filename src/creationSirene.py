import pandas as pd

# Charger les CSV
df1 = pd.read_csv("../data/raw/EtabPart0.csv")
df2 = pd.read_csv("../data/raw/Sigles.csv")
df3 = pd.read_csv("../data/raw/ChangementsNoms.csv")

# 1. Filtrer les SIRET commençant par "1" ou "2"
print(df1)
df1['siret'] = df1['siret'].astype(str)
df1 = df1[df1['siret'].str.startswith(('1', '2'))]

# 2. Garder un seul sigle par SIREN (df2)
df2 = df2.drop_duplicates(subset='siren', keep='first')

# 3. Garder l'avant-dernier NomAncien par SIREN (df3)
# Ajouter un index temporaire pour trier correctement
df3['_order'] = df3.groupby('siren').cumcount()
# Récupérer le rang max par siren
last_index = df3.groupby('siren')['_order'].transform('max')
# Filtrer pour garder l'avant-dernier uniquement
df3_filtered = df3[df3['_order'] == last_index - 1].drop(columns=['_order'])

# 4. Fusions
df_merged = df1.merge(df2, on="siren", how="left")
df_merged = df_merged.merge(df3_filtered, on="siren", how="left")


# Renommer les colonnes
# sigleUniteLegale --> sigle
df_merged.rename(columns={"sigleUniteLegale": "sigle"}, inplace=True)
# Nom --> nomAncien
df_merged.rename(columns={"Nom": "nomAncien"}, inplace=True)

# 5. Export final
df_merged.to_csv("../data/processed/sirene.csv", index=False)