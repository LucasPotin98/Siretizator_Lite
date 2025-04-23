def siretization(database):
    """Siretization step"""
    cursor = database.cursor()
    datas = pd.read_sql_query("SELECT * FROM AgentsSiretiser", database,dtype=str) 
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,ids TEXT,type TEXT, PRIMARY KEY(agentId))"
    sql = cursor.execute(request)
    start = time.time()

    ###Creation of the databases
    cwd = os.getcwd()
    data_path = cwd+'/data/opening/*.csv'   ##Opening database
    d_p = cwd+'/data/Etab/*.csv'            # Every facilities in SIRENE
    d_s =cwd +'/data/foppaFiles/Sigles.csv' # Acronyms
    data_path_noms =cwd +'/data/foppaFiles/ChangementsNoms.csv' #Name modification during time

    bc = BlazingContext()
    col_types = ["str","str","str","str","str","str","str","str","str","str","str","str"]
    bc.create_table("etablissement",d_p,dtype=col_types)
    col_types = ["str","str"]
    bc.create_table('noms',data_path_noms,dtype=col_types)
    col_types = ["str","str","str"]
    bc.create_table('ouvertures',data_path,dtype=col_types)
    col_types = ["str","str"]
    bc.create_table('sigles',d_s,dtype=col_types)

    taille=len(datas)

    datas = datas.assign(siretPropose=0)
    datas = datas.assign(score=0)
    datas = datas.assign(NomSirene=0)
    datas = datas.assign(AdresseSirene=0)
    datas = datas.assign(VilleSirene=0)
    datas = datas.assign(CPSirene=0)


    dicoAssociation ={}
    ####Creation of specific datasets for CAEs

    ##CAF
    dfCAF = bc.sql("select * from etablissement WHERE TypeActivite = '84.30C' AND CatJuridique='8110'")
    dicoAssociation["8110"] = dfCAF
    ##CCI
    dfCCI= bc.sql("select * from etablissement WHERE TypeActivite = '94.11Z' AND CatJuridique='7381'")
    dicoAssociation["7381"] = dfCCI
    #dfSIVU
    dfSyndic= bc.sql("select * from etablissement WHERE CatJuridique='7353'")
    dicoAssociation["7353"] = dfSyndic
    #dfSyndicatMixte
    dfSyndicMixte= bc.sql("select * from etablissement WHERE CatJuridique='7354' OR CatJuridique='7355'")
    dicoAssociation["7354"] = dfSyndicMixte
    ##CCAS
    dfCCAS= bc.sql("select * from etablissement WHERE TypeActivite = '88.99B' AND CatJuridique='7361'")
    dicoAssociation["7361"] = dfCCAS
    ##Mairies
    dfMairie = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7210'")
    dicoAssociation["7210"] = dfMairie
    ##Departement
    dfDepartement = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7220'")
    dicoAssociation["7220"] = dfDepartement
    ##Region
    dfRegion = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7230'")
    dicoAssociation["7230"] = dfRegion
    ##Communauté d'agglo
    dfComAgglo = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND (CatJuridique='7348' OR CatJuridique='7344') ")
    dicoAssociation["7348"] = dfComAgglo
    dicoAssociation["7344"] = dfComAgglo
    ##Communauté de Communes
    dfComCom = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7346'")
    dicoAssociation["7346"] = dfComCom
    ##Centre hospitalier
    dfChu = bc.sql("select * from etablissement WHERE TypeActivite = '86.10Z' AND CatJuridique='7364'")
    dicoAssociation["7364"] = dfChu
    end = time.time()
    for j in range(0,taille):
        ############## If the Siret is here, we replace the values immediatly:
        if len(str(datas["siret"][j]))>9:
            request = "SELECT nomUnite,num,typevoie,libelle,ville,cp FROM newtable WHERE siret = '"+str(datas["siret"][j])+ "'"
            gdf = bc.sql(request)
            if len(gdf)>0:
                datas["siretPropose"][j] = gdf['siret'][0]
                datas["score"][j] = maxi
                datas["NomSirene"][j] = gdf['nomUnite'][0]
                datas["AdresseSirene"][j] = str(gdf['num'][0])+" "+ str(gdf['typevoie'][0])+" "+ str(gdf['libelle'][0])
                datas["VilleSirene"][j] = gdf['ville'][0]
                datas["CPSirene"][j] = gdf['cp'][0]
            else: 
                print("Error : the Siret is incorrect. We will try to siretize")
                datas["siret"][j]=""
        else:
            start = time.time()
            gdf2 = pd.DataFrame()
            gdf3 = pd.DataFrame()
            gdf4 = pd.DataFrame()
            
            date = str(datas["date"][j])
            adresse = str(datas["address"][j])+ " "+str(datas["city"][j])
            nom = str(datas["name"][j])
            cat = str(int(float(datas["catJuridique"][j])))
            CP = str(datas["zipcode"][j])
            if len(CP)==4: ###Securite
                    CP = "0"+CP
            if len(CP)<5 or date=="nan": ##Sortie
                continue
            depart = CP[0:2]
            
            ######PREMIER FILTRAGE
            if cat in dicoAssociation:
                gdf = dicoAssociation[cat]
                bc.create_table("newtable",gdf)
                request = "SELECT nomEnseigne,nomUnite,siret,siren,num,typevoie,libelle,ville,CatJuridique,cp FROM newtable WHERE cp LIKE '"+ depart + "%'"
                gdf = bc.sql(request)
            else:
                request = "SELECT nomEnseigne,nomUnite,siret,siren,num,typevoie,libelle,ville,CatJuridique,cp FROM etablissement WHERE cp LIKE '"+ depart + "%' AND siret IN(select siret from ouvertures WHERE '"+date+"' BETWEEN date_debut AND date_fin)"
                gdf = bc.sql(request)
                nbCP = len(gdf)
            if len(gdf)>0:
                ### Jointure ACRONYMES
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                r = "SELECT * from newtable LEFT JOIN sigles ON newtable.siren = sigles.siren"
                gdf = bc.sql(r)
                
                ### Jointure ancienNoms
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                r = "SELECT * from newtable LEFT JOIN noms ON newtable.siren = noms.siren"
                gdf = bc.sql(r)
                
                ##
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                end = time.time()
                start=time.time()
                
                if len(gdf)>0:
                    vrainom = nom
                    noms=gdf["nomEnseigne"].to_arrow().to_pylist()
                    noms2=gdf["nomUnite"].to_arrow().to_pylist()
                    ancienNoms=gdf["Nom"].to_arrow().to_pylist()
                    sirets=gdf["siren"].to_arrow().to_pylist()
                    results = []
                    results2 = []
                    results3 = []
                    results4=[]
                    if cat in dicoAssociation:
                        sigles = []
                    else:
                        sigles=gdf["sigleUniteLegale"].to_arrow().to_pylist()
                        
                    if len(vrainom)<6:
                        results = process.extract(vrainom,noms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=500)
                        results2 = process.extract(vrainom,sigles,scorer=fuzz.token_set_ratio,score_cutoff=100,limit=100)
                        results3 = process.extract(vrainom,noms2,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                        results4 = process.extract(vrainom,ancienNoms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                    else:
                        results = process.extract(vrainom,noms,scorer=fuzz.token_set_ratio,score_cutoff=60,limit=500)
                        results2 = process.extract(vrainom,sigles,scorer=fuzz.token_set_ratio,score_cutoff=100,limit=100)
                        results3 = process.extract(vrainom,noms2,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                        results4 = process.extract(vrainom,ancienNoms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                    candidats = results+results2+results3+results4
                    end = time.time()
                    start=time.time()
                    gdf=gdf.fillna("")
                    temp = []
                    for i in range(len(candidats)):
                        temp.append(str(gdf["num"][candidats[i][2]])+" "+str(gdf["typevoie"][candidats[i][2]])+" "+str(gdf["libelle"][candidats[i][2]])+" "+str(gdf["ville"][candidats[i][2]]))
                    resultatsFinaux = process.extract(adresse,temp,scorer=fuzz.token_set_ratio,score_cutoff=0,limit=50)
                    scoreTotal = []
                    for num in range(len(resultatsFinaux)):
                        scoreTotal.append(int(resultatsFinaux[num][1])+int(candidats[resultatsFinaux[num][2]][1]))
                    scoreTotal = np.array(scoreTotal)
                    if len(scoreTotal)>0:
                        maxi = int(np.max(scoreTotal))
                        gagnant = np.argwhere(scoreTotal == np.amax(scoreTotal))
                        if len(gagnant)<2:
                            gagnant = int(gagnant[0])
                        else:
                            departage = []
                            for gagnantPossible in gagnant:
                                numgagnant = int(gagnantPossible)
                                position = int(candidats[int(resultatsFinaux[numgagnant][2])][2])
                                if pd.isna(gdf["nomEnseigne"][position]):
                                    gdf["nomEnseigne"][position] = ""
                                if pd.isna(gdf["nomUnite"][position]):
                                    gdf["nomUnite"][position] = ""
                                if pd.isna(gdf["sigleUniteLegale"][position]):
                                    gdf["sigleUniteLegale"][position] = ""
                                if pd.isna(gdf["Nom"][position]):
                                    gdf["Nom"][position] = ""
                                score = max(fuzz.ratio(vrainom,gdf["nomEnseigne"][position]),fuzz.ratio(vrainom,gdf["nomUnite"][position]),fuzz.ratio(vrainom,gdf["sigleUniteLegale"][position]),fuzz.ratio(vrainom,gdf["Nom"][position]))
                                departage.append(score)
                            gagnantDepartage = int(np.argmax(departage))
                            gagnant = int(gagnant[gagnantDepartage])
                        position = int(candidats[int(resultatsFinaux[gagnant][2])][2])
                    if len(candidats)>0:
                        datas["siretPropose"][j] = gdf['siret'][position]
                        datas["score"][j] = maxi
                        datas["NomSirene"][j] = gdf['nomUnite'][position]
                        datas["AdresseSirene"][j] = str(gdf['num'][position])+" "+ str(gdf['typevoie'][position])+" "+ str(gdf['libelle'][position])
                        datas["VilleSirene"][j] = gdf['ville'][position]
                        datas["CPSirene"][j] = gdf['cp'][position]
                    end = time.time()
                
    ids = np.array(datas["ids"]) 
    types = np.array(datas["type"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"])
    newSirets = np.array(datas["siretPropose"])
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    nomSirene = np.array(datas["NomSirene"])
    adresseSirene = np.array(datas["AdresseSirene"])
    villeSirene = np.array(datas["VilleSirene"])
    cpSirene = np.array(datas["CPSirene"])
    for i in range(len(sirets)):
        if len(str(newSirets[i]))>9:
            names[i] = nomSirene[i]
            addresses[i] = adresseSirene[i]
            citys[i] = villeSirene[i]
            zipcodes[i] = cpSirene[i]
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 
    catJuridique = np.array(datas["catJuridique"]) 

    for i in range(len(ids)):
        sql = ''' INSERT OR IGNORE INTO AgentsSiretiser(agentId,name,siret,address,city,zipcode,country,date,catJuridique,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i],types[i])
        cursor.execute(sql,val)
    database.commit()
    return database