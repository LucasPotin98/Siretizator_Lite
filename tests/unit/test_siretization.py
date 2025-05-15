import pandas as pd
from siretizator.siretization import match_siret_dataset
from siretizator.preprocessing import (
    clean_names, normalize_names,
    clean_city, clean_zipcode, hexaposte,
    clean_address, construire_adresses,
    ajouterCatJuridique
)


def test_match_siret_dataset_mocked():
    # Données clients de test (3 lignes)
    df_client_test = pd.DataFrame({
        "name": ["MAIRIE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "DEPARTEMENT DU VAUCLUSE"],
        "address": ["PL DE L'HOTEL DE VILLE", "1 RUE THOMAS BECKET", "RUE VIALA"],
        "city": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "zipcode": ["75004", "76130", "84000"],
        "catJuridiqueDetecte": ["7210-7229", None, "7220"],
        "typeActiviteDetecte": ["84.11Z", None, "84.11Z"]
    })

    # Base SIRENE  avec juste les lignes attendues
    df_sirene_mock = pd.DataFrame({
        "siret": ["21750001600019", "19761904200017", "22840001600017"],
        "siren": ["217500016", "197619042", "228400016"],
        "nomEnseigne": ["MAIRIE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "CONSEIL GENERAL DE VAUCLUSE"],
        "nomUnite": ["VILLE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "DEPARTEMENT DU VAUCLUSE"],
        "num": ["4", "1", ""],
        "typevoie": ["PL", "RUE", "RUE"],
        "libelle": ["HOTEL VILLE ESP LIBERATION", "THOMAS BECKET", "VIALA"],
        "cp": ["75004", "76130", "84000"],
        "ville": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "TypeActivite": ["84.11Z", "85.42Z", "84.11Z"],
        "CatJuridique": ["7229", "7383", "7220"],
        "sigle": ["", "", ""],
        "nomAncien": ["", "UNIVERSITE DE ROUEN", ""],
    })

    # Appel de la fonction
    df_result = match_siret_dataset(df_client_test, df_sirene_mock, n_best=10)

    # Vérification
    expected = pd.Series(["21750001600019", "19761904200017", "22840001600017"])
    pd.testing.assert_series_equal(
        df_result["siret_match"].astype(str).reset_index(drop=True),
        expected,
        check_names=False,
        check_dtype=False
    )


def test_api_like_preprocessing_and_match():
    # Simulation d'entrée API non nettoyée
    df_client = pd.DataFrame({
        "name": ["commune DE PARIS", "universite DE rouen", "DEPARTEMENT DU VAUCLUSE"],
        "address": ["PLACE DE L'HOTEL DE VILLE", "RUE THOMAS BECKET", "RUE VIALA"],
        "city": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "zipcode": ["75004", "76130", None]
    })

    # Nettoyage & enrichissement
    df_client["name"] = clean_names(df_client["name"])
    df_client["name"] = normalize_names(df_client["name"])
    df_client["city"] = clean_city(df_client["city"])
    df_client["zipcode"] = clean_zipcode(df_client["zipcode"])
    df_client["zipcode"] = hexaposte(df_client["city"], df_client["zipcode"])
    df_client["address"] = clean_address(df_client["address"])
    df_client["address"] = construire_adresses(df_client["address"])
    df_client = ajouterCatJuridique(df_client)

    # Base SIRENE mock identique à l’autre test
    df_sirene_mock = pd.DataFrame({
        "siret": ["21750001600019", "19761904200017", "22840001600017"],
        "siren": ["217500016", "197619042", "228400016"],
        "nomEnseigne": ["MAIRIE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "CONSEIL GENERAL DE VAUCLUSE"],
        "nomUnite": ["VILLE DE PARIS", "UNIVERSITE DE ROUEN NORMANDIE", "DEPARTEMENT DU VAUCLUSE"],
        "num": ["4", "1", ""],
        "typevoie": ["PL", "RUE", "RUE"],
        "libelle": ["HOTEL VILLE ESP LIBERATION", "THOMAS BECKET", "VIALA"],
        "cp": ["75004", "76130", "84000"],
        "ville": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
        "TypeActivite": ["84.11Z", "85.42Z", "84.11Z"],
        "CatJuridique": ["7229", "7383", "7220"],
        "sigle": ["", "", ""],
        "nomAncien": ["", "UNIVERSITE DE ROUEN", ""],
    })

    df_result = match_siret_dataset(df_client, df_sirene_mock, n_best=10)
    expected = pd.Series(["21750001600019", "19761904200017", "22840001600017"])

    pd.testing.assert_series_equal(
        df_result["siret_match"].astype(str).reset_index(drop=True),
        expected,
        check_names=False,
        check_dtype=False
    )
