from fastapi import FastAPI
import pandas as pd

from pathlib import Path


MOVIE_CSV_PATH = Path(__file__).parents[1].joinpath("data", "movies_db.csv").resolve()
app = FastAPI(title="Pelis y Series")

@app.get("/")
async def read_root():
     return "Base de datos de películas dispobibles en distintas plataformas"

@app.get("/get_max_duration/")
async def get_max_duration(year: int, platform: str, duration_type: str) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)

    # Filter columns
    filtered_df = df.loc[
        (df["release_year"] == year if year else True)
        & (df["platform"] == platform.lower() if platform else True)
        & (df["duration_type"] == duration_type.lower() if duration_type else True)
    ]

    # Get movie with max duration
    max_duration_movie = filtered_df.loc[filtered_df["duration_int"].idxmax()]["title"]
    return {"pelicula": max_duration_movie}


@app.get("/get_score_count/")
async def get_score_count(platform: str, scored: float, year: int) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)
    
    # Filter DataFrame by platform and year
    filtered_df = df.loc[
        (df["platform"] == platform.lower())
        & (df["release_year"] == year)
    ]

    # Count movies with score higher or equal to "scored" param
    movie_count = filtered_df.loc[
        filtered_df["score_avg"] >= scored
    ].groupby("platform")["id"].count()[platform.lower()].item()

    return {
        "plataforma": platform.title(),
        "cantidad": movie_count,
        "anio": year,
        "score": scored
    }


@app.get("/get_count_platform/")
async def get_count_platform(platform: str) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)
    
    # Filter DataFrame by platform
    filtered_df = df.loc[df["platform"] == platform.lower()]

    # Count movies by platform
    quantity_by_platform = filtered_df.groupby("platform")["id"].count()[platform.lower()].item()

    return {
        "plataforma": platform.title(),
        "peliculas": quantity_by_platform
    }


@app.get("/get_actor/")
async def get_actor(platform: str, year: int) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)

    # Filter DataFrame by platform and year
    filtered_df = df.loc[
        (df["platform"] == platform.lower())
        & (df["release_year"] == year)
    ]

    # Split actors string into a list and convert to DataFrame
    actors_df = filtered_df["cast"].str.split(", ", expand=True)

    # Obtain most repeated actor
    actor_count = actors_df.stack().value_counts().reset_index()
    actor_repeated = actor_count.iloc[0]["index"]
    actor_apparitions = actor_count.iloc[0,1].item()

    return {
        "plataforma": platform.title(),
        "anio": year,
        "actor": actor_repeated.title(),
        "apariciones": actor_apparitions
    }


@app.get("/prod_per_country/")
async def prod_per_country(prod_type: str, country: str, year: int) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)

    # Filter columns
    filtered_df = df.loc[
        (df["country"] == country.lower())
        & (df["release_year"] == year)
    ]

    # Group by product type: either movie or tv show, and get the total sum
    product_quantity = filtered_df.groupby("type")["id"].count()[prod_type.lower()].item()

    return {
        "pais": country.title(),
        "anio": year,
        "peliculas": product_quantity
    }


@app.get("/get_contents/")
async def get_contents(rating: str) -> dict:
    df = pd.read_csv(MOVIE_CSV_PATH)
    
    # Filter columns
    filtered_df = df.loc[df["rating"] == rating.lower()]

    # Group by type and count the total sum of elements
    total_elements = filtered_df.groupby("type")["id"].count().sum().item()

    return {
        "rating": rating.upper(),
        "contenido": total_elements
    }