import pandas as pd
import numpy as np
from siretizator.preprocessing import clean_names, clean_address, clean_city, clean_zipcode,hexaposte,construire_adresses,normalize_names

def test_clean_names():
    input_series = pd.Series([
        "societe abc", # Minuscule
        "CAFÉ DES AMIS", # Accent
        "ENTREPRISE L'AVENIR", # Apostrophe
        "SOCIÉTÉ ABC (agence)",  #Parenthèses
        None
    ])
    expected_series = pd.Series([
        "SOCIETE ABC",
        "CAFE DES AMIS",
        "ENTREPRISE L AVENIR",
        "SOCIETE ABC",
        None
    ])
    
    output_series = clean_names(input_series)
    
    pd.testing.assert_series_equal(output_series, expected_series)




def test_total_names():
    input_series = pd.Series([
        "commune de paris", # Minuscule + commune
        "CA DE MONTPELLIER (departement 3)", # CA + parenthèses avec piège
        "SIVU INTERCOMMUNAL", # Apostrophe
        "SOCIÉTÉ ABC",  #Parenthèses
        None
    ])
    expected_series = pd.Series([
        "MAIRIE DE PARIS",
        "COMMUNAUTE D AGGLOMERATION DE MONTPELLIER",
        "SYNDICAT INTERCOM INTERCOMMUNAL",
        "SOCIETE ABC",
        None
    ])
    
    output_series = clean_names(input_series)
    output_series = normalize_names(output_series)
    
    pd.testing.assert_series_equal(output_series, expected_series)

def test_clean_address():
    input_series = pd.Series([
        "123 rue de la Paix", # Adresse complète
        "456 avenue des Champs-Élysées", # Adresse avec accent
        "BP 12345", # Boîte postale
        "CS 67890", # CS
        "CEDEX 12345", # CEDEX
        None
    ])
    expected_series = pd.Series([
        "123 RUE DE LA PAIX",
        "456 AVENUE DES CHAMPS ELYSEES",
        "",
        "",
        "",
        None
    ])
    
    output_series = clean_address(input_series)
    
    pd.testing.assert_series_equal(output_series, expected_series)

def test_clean_city():
    input_series = pd.Series([
        "Paris", # Ville normale
        "Lyon", # Ville normale
        "Marseille CEDEX 1", # Ville avec CEDEX
        "Toulouse SP 2", # Ville avec SP
        "Bordeaux &APOS;Ville", # Ville avec &APOS;
        "ST-DENIS", # Ville avec tiret + SAINT
        "LE ST DENIS", # Ville avec SAINT
        None
    ])
    expected_series = pd.Series([
        "PARIS",
        "LYON",
        "MARSEILLE",
        "TOULOUSE",
        "BORDEAUX VILLE",
        "SAINT DENIS", # ST devient SAINT
        "LE SAINT DENIS", # ST devient SAINT
        None
    ])
    
    output_series = clean_city(input_series)
    
    pd.testing.assert_series_equal(output_series, expected_series)

def test_hexaposte():
    ville_series = pd.Series([
        "SAINT ETIENNE", # Ville avec ST
        "SAINT ETIENNE", # Ville normale
        "SAINT ETIENNE", # Ville avec ST
        "LAPENNE", # Ville normale
        "PARIS", # Ville normale
        "RYTEFCD", # Ville erronée
        None
    ])

    zipcode_series = pd.Series([
        "42000", # Code postal normal
        "43000", # Code postal erroné, on ne le remplace pas
        None, # pas de CP
        None, # pas de CP
        None,
        None,
        None
    ])

    expected_series = pd.Series([
        "42000", # Ville avec ST
        "43000", # Ville normale
        "42100", # Ville avec ST
        "09500", # Ville normale
        "75017", # Ville normale
        np.nan, # Ville erronée
        np.nan
    ])

    output_series = hexaposte(ville_series, zipcode_series)

    pd.testing.assert_series_equal(output_series, expected_series)


def test_construire_adresses():
    address_series = pd.Series([
        "12 AVENUE DES CHAMPS ELYSEES",    # Cas standard avec numéro
        "12 AV DES CHAMPS ELYSEES",    # Cas standard deja normalisé
        "RUE DE LA PAIX",                   # Cas sans numéro
        "PLACE DE L OPERA",                 # Cas sans numéro
        "9 BOULEVARD HAUSSMANN",         # Cas standard avec numéro
        "CHEMIN INCONNU",        
        "TOTO",            # Adresse sans type reconnu
        None                                 # Adresse vide
    ])

    expected_series = pd.Series([
        "12 AV DES CHAMPS ELYSEES",    # Avenue normalisée
        "12 AV DES CHAMPS ELYSEES",    # Avenue déjà normalisée
        "RUE DE LA PAIX",              # Rue normalisée
        "PL DE L OPERA",             # Place normalisée
        "9 BD HAUSSMANN",              # Boulevard normalisé
        "CHE INCONNU", 
        "TOTO",             # Pas de correspondance, l'adresse reste inchangée
        None                         # None → doit rester None (ou NaN dans la série)
    ])

    output_series = construire_adresses(address_series)

    pd.testing.assert_series_equal(output_series, expected_series)
