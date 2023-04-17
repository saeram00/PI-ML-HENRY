import numpy as np
import pandas as pd

from pathlib import Path
import sys
import gc

DATA_DIR = Path(__file__).parents[1].joinpath("data").resolve()


def check_platform(id_string: str) -> str | float:
    """Figure out platform based on substring in id"""

    if id_string.startswith("a"):
        return "amazon"
    elif id_string.startswith("d"):
        return "disney"
    elif id_string.startswith("h"):
        return "hulu"
    elif id_string.startswith("n"):
        return "netflix"
    else:
        return np.NaN


def main():
    # Load all platform db csv files.
    amaz_df = pd.read_csv(DATA_DIR.joinpath("amazon_prime_titles.csv"))
    disn_df = pd.read_csv(DATA_DIR.joinpath("disney_plus_titles.csv"))
    hulu_df = pd.read_csv(DATA_DIR.joinpath("hulu_titles.csv"))
    netf_df = pd.read_csv(DATA_DIR.joinpath("netflix_titles.csv"))

    # Add first letter of platform name to id column respectively
    amaz_df["show_id"] = "a" + amaz_df["show_id"]
    disn_df["show_id"] = "d" + disn_df["show_id"]
    hulu_df["show_id"] = "h" + hulu_df["show_id"]
    netf_df["show_id"] = "n" + netf_df["show_id"]

    # Concat all DataFrames into a single DataFrame object
    # and delete previous dfs to conserve resources
    cleaned_datafs = pd.concat(
        (amaz_df, disn_df, hulu_df, netf_df),
        ignore_index=True
    )
    del amaz_df, disn_df, hulu_df, netf_df
    gc.collect()

    # 1. Create id column by simply renaming show_id column
    cleaned_datafs.rename({"show_id": "id"}, axis=1, inplace=True)

    # 2. Fill NaN values in rating columns with "G" rating,
    # for "General for All Audiences"
    cleaned_datafs["rating"].fillna(value="G", inplace=True)

    # 3. Convert any dates to "YYYY-mm-dd" format
    cleaned_datafs["date_added"] = pd.to_datetime(cleaned_datafs["date_added"])

    # 4. Turn all text fields to lowercase
    cleaned_datafs.applymap(lambda s: s.lower() if isinstance(s, str) else s)

    # 5. duration columns should be turned to 2 separate columns:
    # duration_int & duration_type. The first must be of type int and be
    # equal to the numeric part of the previous duration column, while the
    # second must be a string equal to the non-numeric part of the
    # duration column, indicating min or seasons, respectively
    cleaned_datafs[
        ["duration_int", "duration_type"]
    ] = cleaned_datafs["duration"].str.split(" ", expand=True)
    cleaned_datafs["duration_int"] = cleaned_datafs["duration_int"].astype(pd.Int16Dtype())
    cleaned_datafs.drop(["duration"], axis=1, inplace=True)

    # Insert platform column based on id first character if not exists,
    # else drop column to avoid errors
    if "platform" in cleaned_datafs.columns:
        cleaned_datafs.drop(["platform"], axis=1, inplace=True)
    else:
        cleaned_datafs.insert(
            loc=1,
            column="platform",
            value=cleaned_datafs["id"].map(check_platform)
        )
    
    # Merge all ratings csv files into one dataframe
    globbed_path = DATA_DIR.joinpath("ratings").glob("*.csv")
    ratings_df = pd.concat(map(pd.read_csv, globbed_path), ignore_index=True)
    # Add platform column if not exists already
    if "platform" in ratings_df.columns:
        ratings_df.drop("platform", axis=1, inplace=True)
    else:
        ratings_df.insert(
            loc=len(ratings_df.columns),
            column="platform",
            value=ratings_df["movieId"].map(check_platform)
        )

    # Obtain the mean value of all ratings per movie
    mean_score = ratings_df.groupby("movieId")["rating"].mean().round(1)
    processed_ratings = ratings_df.join(
        mean_score,
        on="movieId",
        rsuffix="_avg"
    )
    processed_ratings = processed_ratings.drop_duplicates(
        subset="movieId",
        keep="first"
    )

    del globbed_path, mean_score, ratings_df
    gc.collect()

    processed_ratings.rename(
        columns={
            "rating": "score",
            "rating_avg": "score_avg"
        },
        inplace=True
    )

    processed_ratings.to_csv(
        DATA_DIR.joinpath("processed_ratings.csv"),
        index=False
    )

    # Merge both cleaned_datafs and processed_ratings into a
    # single DataFrame and csv file
    processed_ratings.drop(
        ["platform", "score", "userId", "timestamp"],
        axis=1,
        inplace=True
    )

    complete_db = cleaned_datafs.merge(
        processed_ratings,
        how="inner",
        left_on="id",
        right_on="movieId",
        suffixes=(None, "_y"),
        validate="1:1"
    )

    if "movieId" in complete_db.columns:
        complete_db.drop(["movieId"], axis=1, inplace=True)
    
    complete_db.to_csv(
        DATA_DIR.joinpath("movies_db.csv"),
        index=False
    )


if __name__ == "__main__":
    rc: int = 1
    try:
        main()
        rc = 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        sys.exit(rc)