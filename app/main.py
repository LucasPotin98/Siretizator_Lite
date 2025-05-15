from fastapi import FastAPI, UploadFile, File, Request, Body
from fastapi.responses import JSONResponse,HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from siretizator.siretization import match_siret_ligne, match_siret_dataset,siretization
import base64
import io
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
app = FastAPI()

# Charger une fois la base SIRENE
df_sirene = pd.read_csv(
    "https://huggingface.co/datasets/LucasPotin98/SIRENE_Client/resolve/main/sirene.csv",
    dtype=str,
    keep_default_na=False
)

# Configurer le moteur de templates
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})



# Modèle pour une demande simple
class SiretAgent(BaseModel):
    name: str
    city: str
    address: Optional[str] = None
    zipcode: Optional[str] = None

class SiretOne(BaseModel):
    agent: SiretAgent
    seuil_confiance: Optional[int] = 150

    model_config = {
        "json_schema_extra": {
            "example": {
                "agent": {
                    "name": "ville de paris",
                    "address": "place de l'hôtel de ville",
                    "city": "Paris",
                    "zipcode": "75004"
                },
                "seuil_confiance": 150
            }
        }
    }

class SiretBulk(BaseModel):
    agents: List[SiretAgent]
    seuil_confiance: Optional[int] = 150

    model_config = {
        "json_schema_extra": {
            "example": {
                "agents": [
                    {
                        "name": "ville de paris",
                        "address": "place de l'hôtel de ville",
                        "city": "Paris",
                        "zipcode": "75004"
                    },
                    {
                        "name": "université de rouen",
                        "address": "rue thomas becket",
                        "city": "mont saint-aignan"
                    }
                ],
                "seuil_confiance": 150
            }
        }
    }



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
