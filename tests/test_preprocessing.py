import pandas as pd
import numpy as np
from preprocessing import clean_names, clean_address, clean_city, clean_zipcode,hexaposte

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