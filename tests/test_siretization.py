import pandas as pd
from siretization import match_siret_dataset
from preprocessing import clean_names, clean_address, clean_city, clean_zipcode, hexaposte, construire_adresses, normalize_names, ajouterCatJuridique

def test_match_siret_dataset():
    # 1. Données de test (mini dataset client)
    df_client_test = pd.DataFrame({
        "name": ["MAIRIE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "DEPARTEMENT DU VAUCLUSE"],
        "address": ["PL DE L'HOTEL DE VILLE", "1 RUE THOMAS BECKET", "RUE VIALA"],
        "city": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "zipcode": ["75004", "76130", "84000"],
        "catJuridiqueDetecte": ["7210-7229", None, "7220"],
        "typeActiviteDetecte": ["84.11Z", None, "84.11Z"]
    })

    # 2. Base sirène
    df_sirene = pd.read_csv("data/processed/sirene.csv", dtype=str)

    # 3. Appel de la fonction
    df_result = match_siret_dataset(df_client_test, df_sirene, n_best=100)

    # 4. Vérification du résultat
    expected_sirets = pd.Series(["21750001600019", "19761904200017", "22840001600017"])

    pd.testing.assert_series_equal(
        df_result["siret_match"].astype(str).reset_index(drop=True),
        expected_sirets.astype(str),
        check_names=False,
        check_dtype=False
    )

def test_api_siretization():
    # 1. Simulation de l'entrée API

    df_client_test = pd.DataFrame({
        "name": ["commune DE PARIS", "universite DE rouen", "DEPARTEMENT DU VAUCLUSE"],
        "address": ["PLACE DE L'HOTEL DE VILLE", "RUE THOMAS BECKET", "RUE VIALA"],
        "city": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "zipcode": ["75004", "76130", None]
    })

    ## Nettoyage des données
    df_client_test["name"] = clean_names(df_client_test["name"])
    df_client_test["name"] = normalize_names(df_client_test["name"])


    #
    df_client_test["city"] = clean_city(df_client_test["city"])

    #
    df_client_test["zipcode"] = clean_zipcode(df_client_test["zipcode"])
    df_client_test["zipcode"] = hexaposte(df_client_test["city"], df_client_test["zipcode"])

    df_client_test["address"] = clean_address(df_client_test["address"])
    df_client_test["address"] = construire_adresses(df_client_test["address"])

    #Enrichissement
    df_client_test = ajouterCatJuridique(df_client_test)

    # 2. Base sirène
    df_sirene = pd.read_csv("data/processed/sirene.csv",dtype=str)

    print(df_client_test)
    for col in df_client_test.columns:
        print(f"{col}: {df_client_test[col].unique()}")
    # 3. Appel de la fonction
    df_result = match_siret_dataset(df_client_test, df_sirene, n_best=100)

    # 4. Vérification du résultat
    expected_sirets = pd.Series(["21750001600019", "19761904200017", "22840001600017"])
    
    pd.testing.assert_series_equal(
        df_result["siret_match"].astype(str).reset_index(drop=True),
        expected_sirets.astype(str),
        check_names=False,
        check_dtype=False
    )
    
