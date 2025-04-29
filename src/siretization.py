import pandas as pd
import sqlite3
from rapidfuzz import process, fuzz

def match_siret_ligne(nom, adresse, sirene_df, zipcode=None, catJuridiqueDetecte=None, typeActiviteDetecte=None, n_best=5,
                      seuil_nomUnite=70, seuil_nomEnseigne=70, seuil_nomAncien=75, seuil_sigle=10,seuil_confiance=150):
    """
    Cherche les meilleurs candidats pour une ligne client :
    1. Fuzzy matching sur nom (plusieurs champs)
    2. Départage sur adresse
    - catJuridiqueDetecte peut être None, une valeur, ou plusieurs valeurs séparées par '-'
    - typeActiviteDetecte peut être None ou une valeur
    """

    # 0. Filtrage initial
    sirene_temp = sirene_df.copy()

    #Si y'a le zipcode, on filtre avec le departement i.e les 2 premiers chiffres
    if zipcode:
        sirene_temp = sirene_temp[sirene_temp["cp"].str[:2] == zipcode[:2]]
    if catJuridiqueDetecte and catJuridiqueDetecte != "nan":
        cat_list = str(catJuridiqueDetecte).split('-')
        sirene_temp = sirene_temp[sirene_temp["CatJuridique"].isin(cat_list)]
    if typeActiviteDetecte and typeActiviteDetecte != "nan":
        sirene_temp = sirene_temp[sirene_temp["TypeActivite"] == typeActiviteDetecte]
    
    if sirene_temp.empty:
        return None
    sirene_temp = sirene_temp.reset_index(drop=True)
    candidats = []

    # 1. Fuzzy matching sur les différents champs de nom
    champs = {
        "nomUnite": (sirene_temp["nomUnite"], seuil_nomUnite),
        "nomEnseigne": (sirene_temp["nomEnseigne"], seuil_nomEnseigne),
        "nomAncien": (sirene_temp["nomAncien"], seuil_nomAncien),
        "sigle": (sirene_temp["sigle"], seuil_sigle),
    }

    for champ, (serie, seuil) in champs.items():
        res = process.extract(
            nom,
            serie.fillna(""),
            scorer=fuzz.token_set_ratio,
            score_cutoff=seuil,
            limit=n_best
        )
        for match_str, score, idx in res:
            candidats.append((idx, score, champ))

    if not candidats:
        return None  # Aucun match sur le nom

    # 2. Fuzzy matching sur l'adresse parmi les candidats retenus
    resultats = []
    for idx, score_nom, champ in candidats:
        ligne = sirene_temp.iloc[idx]
        adr_sirene = f"{ligne['num']} {ligne['typevoie']} {ligne['libelle']} {ligne['ville']}".strip()
        score_adr = fuzz.token_set_ratio(adresse, adr_sirene)
        score_total = score_nom + score_adr
        resultats.append((idx, score_total, score_nom, score_adr, champ, adr_sirene))
    # 3. Sélection du meilleur candidat global avec départage
    best_score_total = max([r[1] for r in resultats])

    # Tous les candidats qui ont le meilleur score total
    candidats_best = [r for r in resultats if r[1] == best_score_total]

    if len(candidats_best) == 1:
        best = candidats_best[0]
    else:
        # Plusieurs ex-aequo ➔ départager
        meilleur_score_strict = -1
        best = None
        for candidat in candidats_best:
            idx, _, score_nom, score_adr, champ, adr_sirene = candidat
            nom_candidat = str(sirene_temp.iloc[idx]["nomUnite"])
            score_strict = fuzz.ratio(nom.upper(), nom_candidat.upper())  # Comparaison stricte
            if score_strict > meilleur_score_strict:
                meilleur_score_strict = score_strict
                best = candidat
    if best[1] < seuil_confiance:
        return None
    else:
        return {
                "siret": sirene_temp.iloc[best[0]]["siret"],
                "nom_match": sirene_temp.iloc[best[0]]["nomUnite"],
                "adresse_match": best[5],
                "score_nom": best[2],
                "score_adr": best[3],
                "champ_nom": best[4],
                "score_total": best[1],
            }

def match_siret_dataset(df_client, sirene_df, n_best=5, seuil_nomUnite=70, seuil_nomEnseigne=70, seuil_nomAncien=75, seuil_sigle=100,seuil_confiance=150):
    """
    Applique le matching siret sur tout un DataFrame df_client.
    df_client : doit contenir au moins les colonnes 'name', 'address', 'city'
    sirene_df : DataFrame Sirene préfiltré
    Retourne df_client enrichi avec les colonnes de matching
    """
    from tqdm import tqdm
    tqdm.pandas()  # pour avoir une barre de progression dans .apply()

    # Créer une colonne adresse complète pour matcher
    df_client["full_address"] = df_client["address"].fillna('') + " " + df_client["city"].fillna('')

    # Appliquer ligne par ligne
    df_client["match_result"] = df_client.progress_apply(
        lambda row: match_siret_ligne(
            nom=row["name"],
            adresse=row["full_address"],
            sirene_df=sirene_df,
            zipcode=row["zipcode"],
            catJuridiqueDetecte=row.get("catJuridiqueDetecte"),
            typeActiviteDetecte=row.get("typeActiviteDetecte"),
            n_best=n_best,
            seuil_nomUnite=seuil_nomUnite,
            seuil_nomEnseigne=seuil_nomEnseigne,
            seuil_nomAncien=seuil_nomAncien,
            seuil_sigle=seuil_sigle,
            seuil_confiance=seuil_confiance
        ),
        axis=1
    )

    # Extraire les résultats dans des colonnes
    df_client["siret_match"] = df_client["match_result"].apply(lambda x: x["siret"] if x else None)
    df_client["nom_match"] = df_client["match_result"].apply(lambda x: x["nom_match"] if x else None)
    df_client["adresse_match"] = df_client["match_result"].apply(lambda x: x["adresse_match"] if x else None)
    df_client["score_total"] = df_client["match_result"].apply(lambda x: x["score_total"] if x else 0)
    df_client["champ_nom_match"] = df_client["match_result"].apply(lambda x: x["champ_nom"] if x else None)

    return df_client

