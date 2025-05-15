import pandas as pd
import numpy as np 
import re


def clean_names(series: pd.Series) -> pd.Series:
    # Convertir tout en majuscule
    series = series.str.upper()
    
    # Normalisation des caractères accentués
    series = series.str.normalize('NFKD').apply(lambda x: x.encode('ascii', errors='ignore').decode('utf-8') if pd.notnull(x) else x)

    
    # Règles de nettoyage supplémentaires
    rules = {
        r"&APOS;"          : " ",  # Remplacer &APOS; par un espace
        r"'"               : " ",  # Remplacer l'apostrophe par un espace
        r"-"               : " ",  # Remplacer le tiret par un espace
        r"\([^)]*\)"       : "",  # Supprimer tout ce qui est entre parenthèses
        r";[A-Z, ]+"       : "",  # Supprimer les fragments après un point-virgule
        r"- [A-Z, ]+"      : "",  # Supprimer les fragments après un tiret
        r"\s+"             : " ",  # Remplacer plusieurs espaces par un seul
    }
    
    # Appliquer les règles et nettoyer les espaces
    series = series.replace(rules, regex=True).str.strip()
    
    # Retourner la série nettoyée
    return series


def clean_address(series: pd.Series) -> pd.Series:
    # Tout en majuscule
    series = series.str.upper()
    
    # Normalisation unicode (suppression des accents)
    series = series.str.normalize('NFKD').apply(lambda x: x.encode('ascii', errors='ignore').decode('utf-8') if pd.notnull(x) else x)

    
    # Règles de nettoyage
    rules = {
        r"&APOS;"           : " ",     # Remplacer &APOS; par un espace
        r"&AMP"             : "&",     # Remplacer &AMP par &
        r"(\d);"            : r"\1 ",  # Ajouter un espace après un chiffre suivi d'un point-virgule
        r"(\d)-(\d):"       : r"\1 ",  # Ajouter un espace après un chiffre suivi d'un tiret et d'un deux-points
        r"(\d)/(\d)"        : r"\1 ",  # Ajouter un espace après un chiffre suivi d'un slash
        r"(\d);(\d)"        : r"\1 ",  # Ajouter un espace après un chiffre suivi d'un point-virgule
        r"BIS"              : "",      # Supprimer "BIS"
        r"([A-Z])-([A-Z])"  : r"\1 \2",# Séparer deux lettres majuscules jointes par un tiret
        r"'"                : " ",     # Remplacer l'apostrophe par un espace
        r";"                : " ; ",   # Ajouter des espaces autour des points-virgules
        r"(^[A-Z])"         : r" \1",   # Ajouter un espace devant la première lettre majuscule
        r" +"               : " ",     # Remplacer plusieurs espaces par un seul
        r"\bBP [0-9, ]+"     : "",      # Supprimer les boîtes postales (BP)
        r"\bCEDEX [0-9, ]+"  : " ",     # Supprimer CEDEX et numéro
        r"\bCS [0-9, ]+"     : " ",     # Supprimer CS et numéro
    }
    
    # Application des règles
    series = series.replace(rules, regex=True).str.strip()
    
    return series



def clean_city(series: pd.Series) -> pd.Series:
    # Tout en majuscule
    series = series.str.upper()
    
    # Normalisation unicode (suppression des accents)
    series = series.str.normalize('NFKD').apply(lambda x: x.encode('ascii', errors='ignore').decode('utf-8') if pd.notnull(x) else x)

    
    # Règles de nettoyage
    rules = {
        r"CEDEX"    : "",   # Supprimer "CEDEX"
        r"-"        : " ",  # Remplacer tirets par espaces
        r" SP "     : "",   # Supprimer " SP "
        r"&APOS;"   : " ",  # Remplacer &APOS; par espace
        r"'"        : " ",  # Remplacer apostrophes par espace
        r"\d"       : "",   # Supprimer tous les chiffres
        r"\)"       : "",   # Supprimer les parenthèses fermantes
        r"\("       : "",   # Supprimer les parenthèses ouvrantes
        r"\s+"      : " ",  # Remplacer plusieurs espaces par un seul 
    }
    
    # Application des règles
    series = series.replace(rules, regex=True).str.strip()

    # Dans Hexaposte : ST X est représenté par SAINT X
    # On converti alors les ST en début de ville en SAINT
    series = series.str.replace(r"\bST\b", "SAINT", regex=True)
    
    return series


def clean_zipcode(series: pd.Series) -> pd.Series:
    series = series.str.replace(r"[^\d]+", "", regex=True).str.strip()
    return series.apply(lambda x: x if len(str(x)) == 5 else None)


def normalize_names(series: pd.Series) -> pd.Series:
    rules = pd.read_csv("data/utils/names.csv", sep=";", encoding="utf-8")

    # 2. Appliquer les règles regex/replacement
    normalized_series = series.copy()

    for _, row in rules.iterrows():
        pattern = row['regex']
        replacement = row['replacement']
        # Appliquer la regex en ignorant la casse (re.IGNORECASE)
        normalized_series = normalized_series.str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)

    return normalized_series


def hexaposte(ville_serie: pd.Series,zip_serie: pd.Series) -> pd.Series:
    #On load hexaposte
    hexaposte_df = pd.read_csv('data/utils/hexaposte.csv', sep=';', encoding='utf8', dtype={'code_postal': str})
    ville_to_cp = dict(zip(hexaposte_df['libelle_d_acheminement'].str.upper(), hexaposte_df['code_postal']))

    # On construit une série de CP trouvés via la ville
    zip_trouve = ville_serie.map(ville_to_cp)
    
    # Si CP original est NaN, on prend celui trouvé via Hexaposte
    zip_complet = zip_serie.fillna(zip_trouve)

    return zip_complet


def construire_adresses(address_series):
    """
    Normalise et reconstruit les adresses à partir d'une série d'adresses.
    
    Args:
        address_series (pd.Series): Série contenant les adresses brutes.

    Returns:
        pd.Series: Série contenant les adresses reconstruites et normalisées.
    """
    # Chargement des correspondances type de voie (chemin fixé en dur)
    chemin_libelles = "data/utils/Libelle.csv"
    libellesConvert = pd.read_csv(chemin_libelles,sep=";",dtype=str)

    # Copie de la série pour ne pas modifier l'original
    address_clean = address_series.copy()

    # Remplacement des types de voie
    for idx, row in libellesConvert.iterrows():
        Seekingpattern = r"\b" + row["Nom_Voie"] + r"\b"
        Appliedpattern = row["Nom_Voie_Nomalise"] 
        address_clean = address_clean.replace(regex=Seekingpattern, value=Appliedpattern)

    # Compilation des motifs
    listeLibel = "|".join(libellesConvert["Nom_Voie_Nomalise"])
    endingschar = r"\b"

    # Regex unique : optionnel numéro + type voie + libellé
    reg = rf"(?:([0-9]+)\s)?({listeLibel})\s([A-Z0-9 ]+)({endingschar})"

    # Extraction des composants
    extracted = address_clean.str.extract(reg)
    num_voie = extracted[0]
    code_voie = extracted[1]
    libel_voie = extracted[2]

    # Reconstruction des adresses
    adresse_finale = (
        num_voie.fillna('') +
        " " + 
        code_voie.fillna('') + 
        " " + 
        libel_voie.fillna('')
    ).str.strip()

    # Si l'extraction a échoué, on remet l'adresse originale
    adresse_finale = np.where(
        (code_voie.isna()) | (libel_voie.isna()), 
        address_clean, 
        adresse_finale
    )

    return pd.Series(adresse_finale, index=address_series.index)
    

def ajouterCatJuridique(df_client):
    """
    Ajoute les colonnes 'catJuridiqueDetecte' et 'typeActiviteDetecte' au DataFrame client
    """

    # Chargement 
    mapping_df = pd.read_csv("data/utils/CatJuridique.csv", sep=';')

    cat_detecte = []
    act_detecte = []

    for _, row in df_client.iterrows():
        nom_client = row["name"]
        correspondances = []

        for _, map_row in mapping_df.iterrows():
            pattern = map_row["Nom"]
            catjur_list = map_row["catJuridique"].split("-")
            typeact = map_row["typeActivite"] if map_row["typeActivite"] else None

            if re.search(pattern, nom_client, flags=re.IGNORECASE):
                for catjur in catjur_list:
                    correspondances.append((catjur, typeact))

        if correspondances:
            # Choix simple : prendre la première correspondance
            cats = "-".join(sorted(set([c[0] for c in correspondances])))
            act = correspondances[0][1]
            cat_detecte.append(cats)
            act_detecte.append(act)
        else:
            cat_detecte.append(None)
            act_detecte.append(None)

    # Ajout des colonnes au DataFrame
    df_client["catJuridiqueDetecte"] = cat_detecte
    df_client["typeActiviteDetecte"] = act_detecte

    #STR pour les colonnes
    df_client["catJuridiqueDetecte"] = df_client["catJuridiqueDetecte"].astype(str)
    df_client["typeActiviteDetecte"] = df_client["typeActiviteDetecte"].astype(str)

    return df_client

