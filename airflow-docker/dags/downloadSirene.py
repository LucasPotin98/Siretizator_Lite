import urllib.request
import zipfile
import pandas as pd
import os
from pathlib import Path

from tqdm import tqdm

class DownloadProgressBar(tqdm):
    def update_to(self, blocks=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(blocks * bsize - self.n)

def download_file_with_tqdm(url, filename):
    filename_str = str(filename)
    with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, desc=filename_str) as t:
        urllib.request.urlretrieve(url, filename_str, reporthook=t.update_to)

def download_latest_sirene():
    base_dir = Path("/opt/airflow/data")
    etab_dir = base_dir / "sireneEtab"
    unite_dir = base_dir / "sireneUnite"

    etab_dir.mkdir(parents=True, exist_ok=True)
    unite_dir.mkdir(parents=True, exist_ok=True)

    url_etab = "https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip"
    url_unite = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"

    download_file_with_tqdm(url_etab, base_dir / "StockEtablissement_utf8.zip")
    with zipfile.ZipFile(base_dir / "StockEtablissement_utf8.zip", "r") as zip_ref:
        zip_ref.extractall(etab_dir)
    os.remove(base_dir / "StockEtablissement_utf8.zip")

    download_file_with_tqdm(url_unite, base_dir / "StockUniteLegale_utf8.zip")
    with zipfile.ZipFile(base_dir / "StockUniteLegale_utf8.zip", "r") as zip_ref:
        zip_ref.extractall(unite_dir)
    os.remove(base_dir / "StockUniteLegale_utf8.zip")



def parse_sirene(chunksize=100000):

    base_dir = Path("/opt/airflow/data")
    etab_path = base_dir / "sireneEtab/StockEtablissement_utf8.csv"
    unite_path = base_dir / "sireneUnite/StockUniteLegale_utf8.csv"

    # Partie unité
    dicoNom = {}
    dicoType = {}

    for chunk in pd.read_csv(unite_path, chunksize=chunksize, dtype=str, low_memory=False):
        chunk = chunk.fillna("")
        first_siren = chunk.iloc[0]["siren"]

        # Early stop : si tout le chunk est au-delà des clients
        if first_siren[0] >= "3":
            break
        
        # Filtrer uniquement les SIRET clients (1 ou 2)
        chunk = chunk[chunk["siren"].str.startswith(("1", "2"))]
        if chunk.empty:
            continue

        for _, row in chunk.iterrows():
            siren = row["siren"]

            if row["denominationUniteLegale"]:
                temp = row["denominationUniteLegale"]
            elif row["denominationUsuelle1UniteLegale"]:
                temp = row["denominationUsuelle1UniteLegale"]
            elif row["denominationUsuelle2UniteLegale"]:
                temp = row["denominationUsuelle2UniteLegale"]
            elif row["denominationUsuelle3UniteLegale"]:
                temp = row["denominationUsuelle3UniteLegale"]
            else:
                temp = row["prenom1UniteLegale"] + " " + row["nomUniteLegale"]

            dicoNom[siren] = temp
            dicoType[siren] = row["categorieJuridiqueUniteLegale"]


    # Partie établissement
    all_data = []

    for chunk in pd.read_csv(etab_path, chunksize=chunksize, dtype=str, low_memory=False):
        chunk = chunk.fillna("")

        # Early stop : si tout le chunk est au-delà des clients
        if chunk.iloc[0]["siret"][0] >= "3":
            break

        # Filtrer uniquement les SIRET clients (1 ou 2)
        chunk = chunk[chunk["siret"].str.startswith(("1", "2"))]
        if chunk.empty:
            continue

        rows = []
        for _, row in chunk.iterrows():
            siren = row["siren"]
            siret = row["siret"]
            num = row["numeroVoieEtablissement"]
            typevoie = row["typeVoieEtablissement"]
            libelle = row["libelleVoieEtablissement"]
            cp = row["codePostalEtablissement"]
            ville = row["libelleCommuneEtablissement"]
            activite = row["activitePrincipaleEtablissement"]

            e1 = row["enseigne1Etablissement"]
            e2 = row["enseigne2Etablissement"]
            e3 = row["enseigne3Etablissement"]
            denom = row["denominationUsuelleEtablissement"]
            nomUnite = dicoNom.get(siren, "")
            catJuridique = dicoType.get(siren, "")

            if e1:
                nomEnseigne = e1
            elif denom:
                nomEnseigne = denom
            elif e2:
                nomEnseigne = e2
            elif e3:
                nomEnseigne = e3
            else:
                nomEnseigne = nomUnite

            rows.append((siret, siren, nomEnseigne, nomUnite, num, typevoie, libelle, cp, ville, activite, catJuridique))

        df_chunk = pd.DataFrame(rows, columns=["siret", "siren", "nomEnseigne", "nomUnite", "num", "typevoie", "libelle", "cp", "ville", "TypeActivite", "CatJuridique"])
        df_chunk["ville"] = (
            df_chunk["ville"]
            .str.replace(r"[0-9]+", "", regex=True)
            .str.replace("CEDEX", "", regex=False)
            .str.replace("-", " ", regex=False)
            .str.replace("'", " ", regex=False)
            .str.strip()
        )

        all_data.append(df_chunk)

    df_final = pd.concat(all_data, ignore_index=True)

    # Astuce pour les mairies
    mask = df_final["nomEnseigne"] == "MAIRIE"
    df_final.loc[mask, "nomEnseigne"] = "MAIRIE DE " + df_final.loc[mask, "ville"]


    # Supprimer les fichiers CSV initiaux
    etab_path.unlink()
    unite_path.unlink()

    # Supprimer les dossiers
    (base_dir / "sireneEtab").rmdir()
    (base_dir / "sireneUnite").rmdir()

    return df_final


def save_sirene_dataset(df, output_path="/opt/airflow/data/processed/sirene.csv.gz"):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, compression="gzip")
    print(f"Dataset sauvegardé en CSV.gz : {output_path}")
