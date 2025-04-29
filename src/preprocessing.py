import pandas as pd
import numpy as np 


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
    return series


def find_cat_Juridique(series: pd.Series) -> pd.Series:
    catJuridique = pd.Series(index=series.index, dtype=str)
    return catJuridique

def hexaposte(ville_serie: pd.Series,zip_serie: pd.Series) -> pd.Series:
    #On load hexaposte
    hexaposte_df = pd.read_csv('data/raw/hexaposte.csv', sep=';', encoding='utf8', dtype={'code_postal': str})
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
    chemin_libelles = "data/raw/Libelle.csv"
    libellesConvert = pd.read_csv(chemin_libelles, dtype=str)

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
    
# Test de la fonction
if __name__ == "__main__":
    # Exemple d'utilisation
    address_series = pd.Series([
        "12 AVENUE DES CHAMPS ELYSEES",    # Cas standard avec numéro
        "RUE DE LA PAIX",                   # Cas sans numéro
        "PLACE DE L OPERA",                 # Cas sans numéro
        "9 BOULEVARD HAUSSMANN ",         # Cas standard avec numéro
        "CHEMIN INCONNU",                    # Adresse sans type reconnu
        None                                 # Adresse vide
    ])
    
    cleaned_addresses = construire_adresses(address_series)
    print(cleaned_addresses)