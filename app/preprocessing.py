import pandas as pd
import numpy as np 


def clean_names(series: pd.Series) -> pd.Series:
    series = series.str.upper()
    series = series.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    rules = {
        r"&APOS;"         : " ",
        r"'"              : " ",
        r"-"              : " ",
        r"\([^)]*\)"      : "",
        r";[A-Z, ]+"      : "",
        r"- [A-Z, ]+"     : ""
    }
    return series.replace(rules, regex=True)

def clean_address(series: pd.Series) -> pd.Series:
    series = series.str.upper()
    series = series.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    rules = {
        r"&APOS;"        : " ",
        r"&AMP"          : "&",
        r"(\d);"         : r"\1 ",
        r"(\d)-(\d):"    : r"\1 ",
        r"(\d)/(\d)"     : r"\1 ",
        r"(\d);(\d)"     : r"\1 ",
        r"BIS"           : "",
        r"([A-Z])-([A-Z])" : r"\1 \2",
        r"'"             : " ",
        r";"             : " ; ",
        r"(^[A-Z])"      : r" \1",
        r" +"            : " ",
        r" BP [0-9, ]+"  : "",
        r" CEDEX [0-9, ]+" : " ",
        r" CS [0-9, ]+"  : " ",
    }
    return series.replace(rules, regex=True).str.strip()

def clean_city(series: pd.Series) -> pd.Series:
    series = series.str.upper()
    series = series.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    rules = {
        r"CEDEX"     : "",
        r"-"         : " ",
        r" SP "      : "",
        r"&APOS;"    : " ",
        r"'"         : " ",
        r"\d"        : "",
        r"\)"        : "",
        r"\("        : ""
    }
    return series.replace(rules, regex=True).str.strip()

def clean_zipcode(series: pd.Series) -> pd.Series:
    series = series.str.replace(r"[^\d]+", "", regex=True).str.strip()
    return series.apply(lambda x: x if len(str(x)) == 5 else None)


def normalize_names(series: pd.Series) -> pd.Series:
    return series

def normalize_adresses(series: pd.Series) -> pd.Series:
    return series 

def normalize_zipcodes(series: pd.Series) -> pd.Series:
    return series

def find_cat_Juridique(series: pd.Series) -> pd.Series:
    catJuridique = pd.Series(index=series.index, dtype=str)
    return catJuridique
