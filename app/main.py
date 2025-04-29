from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from src.siretization import match_siret_ligne, match_siret_dataset,siretization
import base64
import io
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()

# Charger une fois la base SIRENE
sirene_df = pd.read_csv('data/processed/sirene.csv.gz', compression='gzip', dtype=str)


# Modèle pour une demande simple
class SiretAgent(BaseModel):
    name: str
    city: str
    address: Optional[str] = None
    zipcode: Optional[str] = None

class SiretOne(BaseModel):
    agent: SiretAgent
    seuil_confiance: Optional[int] = 150

class SiretBulk(BaseModel):
    agents: List[SiretAgent]
    seuil_confiance: Optional[int] = 150

class SiretDF(BaseModel):
    csv_base64: str  # Si tu veux envoyer un fichier CSV encodé par exemple
    seuil_confiance: Optional[int] = 150



@app.post("/siretize")
def siretize(request: SiretOne):
    """
    Endpoint pour sirétiser un seul agent.
    """
    #Creer un dataframe avec une seule ligne
    df_client = pd.DataFrame({
        "name": [request.agent.name],
        "address": [request.agent.address],
        "city": [request.agent.city],
        "zipcode": [request.agent.zipcode]
    })

    df_result = siretization(df_client, sirene_df, request.seuil_confiance)
    # Extraire le premier résultat
    match = df_result.iloc[0].to_dict()
    # Vérifier si un match a été trouvé
    if df_result.empty or match.get("siret_match") is None:
        return {"message": "Aucun match trouvé"}

    else:
        #retourner le match
        match = {
            "siret": match["siret_match"],
            "nom_match": match["nom_match"],
            "adresse_match": match["adresse_match"],
            "score_total": match["score_total"],
            "champ_nom_match": match["champ_nom_match"]
        }
        return match

@app.post("/siretize_bulk")
def siretize_bulk(request: SiretBulk):
    df_client = pd.DataFrame([{
        "name": agent.name,
        "address": agent.address,
        "city": agent.city,
        "zipcode": agent.zipcode
    } for agent in request.agents])

    df_result = siretization(df_client, sirene_df, request.seuil_confiance)
    # Extraire le premier résultat
    match = df_result.iloc[0].to_dict()
    # Vérifier si un match a été trouvé
    responses = []
    for i, match in df_result.iterrows():
        if pd.isna(match["siret_match"]):
            responses.append({"index": i, "message": "Aucun match trouvé"})
        else:
            responses.append({
                "index": i,
                "siret": match["siret_match"],
                "nom_match": match["nom_match"],
                "adresse_match": match["adresse_match"],
                "score_total": match["score_total"],
                "champ_nom_match": match["champ_nom_match"]
            })

    return responses


@app.post("/siretize_df")
def siretize_df(request: SiretDF):
    """
    Endpoint pour sirétiser un fichier CSV envoyé en base64.
    """

    # 1. Décoder le base64
    try:
        decoded = base64.b64decode(request.csv_base64)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Erreur de décodage base64 : " + str(e))

    # 2. Lire en DataFrame
    try:
        df_client = pd.read_csv(io.BytesIO(decoded))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Erreur de lecture du CSV : " + str(e))

    # 3. Vérification des colonnes attendues
    expected_columns = {"name", "address", "city", "zipcode"}
    if not expected_columns.issubset(df_client.columns):
        raise HTTPException(status_code=400, detail=f"Le CSV doit contenir au minimum les colonnes {expected_columns}")

    df_result = siretization(df_client, sirene_df, request.seuil_confiance)

    responses = []
    for i, match in df_result.iterrows():
        if pd.isna(match["siret_match"]):
            responses.append({
                "siret": None,
                "nom_match": None,
                "adresse_match": None,
                "score_total": None,
                "champ_nom_match": None,
                "message": "Aucun match trouvé"
            })
        else:
            responses.append({
                "siret": match["siret_match"],
                "nom_match": match["nom_match"],
                "adresse_match": match["adresse_match"],
                "score_total": match["score_total"],
                "champ_nom_match": match["champ_nom_match"],
                "message": None
            })

    df_response = pd.DataFrame(responses)

    # 7. Fusionner df_client et df_response
    df_final = pd.concat([df_client.reset_index(drop=True), df_response], axis=1)

    # 8. Conversion du résultat final en CSV
    stream = io.StringIO()
    df_final.to_csv(stream, index=False)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=siretization_enriched.csv"}
    )
