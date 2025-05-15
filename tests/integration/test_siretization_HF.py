import pandas as pd
from siretizator.siretization import match_siret_dataset
from siretizator.preprocessing import (
    clean_names,
    clean_address,
    clean_city,
    clean_zipcode,
    hexaposte,
    construire_adresses,
    normalize_names,
    ajouterCatJuridique,
)
import pytest
from datasets import load_dataset


@pytest.mark.slow
def test_api_siretization():
    # 1. Simulation de l'entrée API

    df_client_test = pd.DataFrame(
        {
            "name": [
                "commune DE PARIS",
                "universite DE rouen",
                "DEPARTEMENT DU VAUCLUSE",
            ],
            "address": ["PLACE DE L'HOTEL DE VILLE", "RUE THOMAS BECKET", "RUE VIALA"],
            "city": ["PARIS", "MONT SAINT AIGNAN", "AVIGNON"],
            "zipcode": ["75004", "76130", None],
        }
    )

    ## Nettoyage des données
    df_client_test["name"] = clean_names(df_client_test["name"])
    df_client_test["name"] = normalize_names(df_client_test["name"])

    #
    df_client_test["city"] = clean_city(df_client_test["city"])

    #
    df_client_test["zipcode"] = clean_zipcode(df_client_test["zipcode"])
    df_client_test["zipcode"] = hexaposte(
        df_client_test["city"], df_client_test["zipcode"]
    )

    df_client_test["address"] = clean_address(df_client_test["address"])
    df_client_test["address"] = construire_adresses(df_client_test["address"])

    # Enrichissement
    df_client_test = ajouterCatJuridique(df_client_test)

    dataset = load_dataset(
        "LucasPotin98/SIRENE_Client",
        data_files="sirene.csv",
        split="train",  # tout est dans 'train' par défaut
    )

    # Convertir en DataFrame Pandas si besoin :
    df_sirene = dataset.to_pandas().astype(str)

    # 3. Appel de la fonction
    df_result = match_siret_dataset(df_client_test, df_sirene, n_best=100)

    # 4. Vérification du résultat
    expected_sirets = pd.Series(["21750001600019", "19761904200017", "22840001600017"])

    pd.testing.assert_series_equal(
        df_result["siret_match"].astype(str).reset_index(drop=True),
        expected_sirets.astype(str),
        check_names=False,
        check_dtype=False,
    )
