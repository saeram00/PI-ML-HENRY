from fastapi import FastAPI
import pandas as pd

app = FastAPI(title="Pelis y Series")

@app.get("/")
async def read_root():
     return "Base de datos de películas dispobibles en distintas plataformas"

@app.get("/get_max_duration/{year}_{platform}_{duration_type}")
async def get_max_duration(year: int, platform: str, duration_type: str):
    df = pd.read_csv("movies_db.csv")
    # Filter columns
    filtered_df = df.loc[
        (df["year"] == year if year else True) &
        (df["platform"] == platform if platform else True) &
        (df["duration_type"] == duration_type if duration_type else True)
    ]

    # Get movie with max duration
    max_duration_movie = filtered_df.loc[filtered_df["duration_int"].idxmax()]
    return max_duration_movie["title"]


@app.get("/get_score_count/{platform}_{scored}_{year}")
def get_score_count(platform: str, scored: float, year: int) -> int:
    df = pd.read_csv("movies_db.csv")
    
    # Filter DataFrame by platform and year
    filtered_df = df.loc[(df["platform"] == platform) & (df["year"] == year)]

    # Count movies with score higher than "scored" param
    movie_count = filtered_df.loc[filtered_df["score"] > scored].groupby("platform")["title"].count()[platform]

    return movie_count


@app.get("/get_count_platform/{platform}")
async def get_count_platform(platform: str) -> int:
    df = pd.read_csv("movies_db.csv")
    
    # Filter DataFrame by platform
    filtered_df = df[df["platform"] == platform]

    # Count movies by platform
    quantity_by_platform = filtered_df.groupby("platform")["title"].count()[0]

    return quantity_by_platform


@app.get("/get_actor/{platform}_{year}")
async def get_actor(platform: str, year: int):

    df = pd.read_csv("movies_db.csv")

    # Filter DataFrame by platform and year
    filtered_df = df[(df["platform"] == platform) & (df["year"] == year)]

    # Split actors string into a list and convert to DataFrame
    actors_df = filtered_df["cast"].str.split(", ", expand=True)

    # Obtain most repeated actor
    actor_count = actors_df.stack().value_counts().reset_index()
    actor_repeated = actor_count.iloc[0]["index"]

    return actor_repeated