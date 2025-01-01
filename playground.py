import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell
def _():
    import zipfile
    import requests
    import os
    import json
    import polars as pl
    from dotenv import load_dotenv
    from collections import Counter
    from datetime import datetime

    load_dotenv()

    zip_file_path = "./export/Account Settings Dec 18 2024.zip"

    extract_to_path = "./extracted_files"

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_path)
    return (
        Counter,
        datetime,
        extract_to_path,
        json,
        load_dotenv,
        os,
        pl,
        requests,
        zip_file_path,
        zip_ref,
        zipfile,
    )


@app.cell
def _(extract_to_path, pl):
    # read csv from extracted files diary with polars

    df = (
        pl.read_csv(f"{extract_to_path}/diary.csv")
        .filter(pl.col("Watched Date") > pl.lit("2024-01-01"))
    )
    return (df,)


@app.cell
def _(df, pl):
    df_fmt = df.with_columns(
        pl.col('Date').str.to_datetime('%Y-%m-%d'), 
        pl.col('Watched Date').str.to_datetime('%Y-%m-%d'),
        pl.concat_str(
            [pl.col('Name'), pl.col('Year')], separator='_').alias('title_year')
    )
    return (df_fmt,)


@app.cell
def _():
    CACHE_FILE = "movie_cache.json"
    return (CACHE_FILE,)


@app.cell
def _(CACHE_FILE, json, os):
    def load_cache():
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        return {}

    def save_cache(cache):
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=4)
    return load_cache, save_cache


@app.cell
def _(load_cache, requests, save_cache):
    def get_movie_data(title, year, api_key):
        parsed_title = title.replace(" ", "+")

        cache = load_cache()
        key = f"{title}_{year}"

        if key in cache:
            print(f"Cache hit per '{title}' ({year})")
            return cache[key]

        url = f"http://www.omdbapi.com/?&t={parsed_title}&y={year}&apikey={api_key}"

        response = requests.get(url)
        data = response.json()

        if response.status_code == 200 and data.get("Response") == "True":
            cache[key] = data
            save_cache(cache)
            return data
        else:
            print(f"Errore: {data.get('Error')}")
            return None
    return (get_movie_data,)


@app.cell
def _(os):
    API_KEY = os.getenv("API_KEY")

    # get_movie_data("Submarine", 2010, API_KEY)
    return (API_KEY,)


@app.cell
def _(get_movie_data):
    def get_metatadat(diary_df, api_key):
        for t, y in diary_df[['Name', 'Year']].rows():
            print(f"Getting data for {t} ({y})")
            get_movie_data(t, y, api_key)
    return (get_metatadat,)


@app.cell
def _(mo):
    mo.md(
        r"""
        # **Movie Data Processing and Insights**

        This application processes movie diary data exported from **Letterboxd** and enriches it with detailed metadata to analyze and uncover interesting insights. The exported data serves as a starting point, containing a basic list of movies watched, and is transformed into a rich dataset with comprehensive information about genres, runtime, directors, writers, and more.

        ---

        ## **The Final Data Structure**
        The full dataframe is a comprehensive table where each row represents a movie and includes the following columns:

        ### **Basic Information**
        - **Name**: The title of the movie.
        - **Year**: The year the movie was released.
        - **Watched Date**: The date the movie was watched.

        ### **Metadata**
        - **Genre**: The genres associated with the movie (e.g., Drama, Comedy).  
        - **Runtime**: The duration of the movie in minutes.  
        - **Director**: The director(s) of the movie.  
        - **Writer**: The writer(s) of the movie.  
        - **Actors**: The cast of the movie.  
        - **Country**: The country where the movie was produced.  
        - **Language**: The languages spoken in the movie.  
        - **Metascore**: The score of the movie based on critics.  
        - **IMDB Rating**: The rating of the movie based on audience reviews.
        - **Rating Differences**: Differences between user ratings and the normalized scores to highlight alignment or discrepancy with critics and the user.
        """
    )
    return


@app.cell
def _(df_fmt, load_cache, pl):
    def extract_metadata(cache, title_year, param):
        temp_metadata = []
        for ty in title_year:
            temp_data = cache.get(ty)
            if temp_data is not None:
                temp_metadata.append(cache.get(ty)[param])
            else:
                temp_metadata.append(None)
        return pl.Series("metadata", temp_metadata)


    cache = load_cache()
    df_full = df_fmt.with_columns(
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Genre"
            )
        )
        .str.split(", ")
        .alias("Genre"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Runtime"
            )
        )
        .alias("Runtime"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Director"
            )
        )
        .alias("Director"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Writer"
            )
        )
        .str.split(", ")
        .alias("Writer"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Actors"
            )
        )
        .str.split(", ")
        .alias("Actors"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Country"
            )
        )
        .str.split(", ")
        .alias("Country"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Language"
            )
        )
        .str.split(", ")
        .alias("Language"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Metascore"
            )
        )
        .str.to_integer(strict=False)
        .alias("Metascore"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "imdbRating"
            )
        )
        .str.to_decimal().cast(pl.Float64)
        .alias("imdbRating"),
    )

    df_full = df_full.with_columns(
        pl.col("Runtime")
        .str.replace(" min", "")
        .str.to_integer()
        .alias("Runtime_normalized"),
        (pl.col("Metascore") * 5 / 100).alias("Normalized_Metascore"),
        (pl.col("imdbRating") * 5 / 10).alias("Normalized_IMDB"),
        (pl.col("Rating") - pl.col("Metascore") * 5 / 100)
        .abs()
        .alias("Rating_Difference_Metascore"),
        (pl.col("Rating") - pl.col("imdbRating") * 5 / 10)
        .abs()
        .alias("Rating_Difference_IMDB"),
    )

    df_full
    return cache, df_full, extract_metadata


@app.cell
def _(Counter):
    def get_top_from_list(df, column, top=5):
        items = []
        for item_list in df.select(column).rows():
            if item_list[0]:
                for i in item_list[0]:
                    items.append(i)

        item_counts = Counter(items)

        for item, count in item_counts.most_common(top):
            print(f"{item}: {count}")
    return (get_top_from_list,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## **Insights and Results**
        The application generates the following analyses and results from the enriched dataframe:
        """
    )
    return


@app.cell
def _(mo, number_unique_directors, total_movies_watched):
    mo.md(
        f"""
        ### **General Movie Statistics**
        - **Total Movies Watched**: The total number of movies logged -> **{total_movies_watched}**
        - **Unique Directors**: The number of unique directors in the dataset -> **{number_unique_directors}**
        - **Top Directors**: The directors with the most movies in the dataset.
        """
    )
    return


@app.cell
def _(df_full):
    total_movies_watched = df_full.select('Name').count().item()
    return (total_movies_watched,)


@app.cell
def _(df_full):
    number_unique_directors = df_full.select('Director').n_unique()
    return (number_unique_directors,)


@app.cell
def _(df_full, pl):
    df_full.group_by('Director').agg(pl.col('Name').count().alias('count')).sort('count', descending=True)
    return


@app.cell
def _(mo, top_3_longest_movies_fmt, total_runtime):
    mo.md(
        f"""
        ### **Time Spent Watching Movies**

        This analysis focuses on the total runtime of movies, identifying the longest movies watched, and examining monthly trends based on runtime.

        1. **Total Runtime -> {total_runtime}h**
           - Calculates the total time spent watching movies in hours by summing up the normalized runtime of all entries. This gives a clear picture of the overall time investment in movie-watching.

        2. **Longest Movies**  
            Identifies the longest movies watched by sorting the dataset by runtime in descending order. This highlights which movies required the most time to watch.
            
            **Top 3**: 
                {top_3_longest_movies_fmt}
        """
    )
    return


@app.cell
def _(df_full):
    total_runtime = int(df_full.select('Runtime_normalized').sum().item() / 60)
    return (total_runtime,)


@app.cell
def _(df_full):
    top_3_longest_movies = df_full.select('Name', 'Runtime_normalized').drop_nulls().sort('Runtime_normalized', descending=True).select('Name').head(3).to_series().to_list()
    return (top_3_longest_movies,)


@app.cell
def _(top_3_longest_movies):
    top_3_longest_movies_fmt = ""
    for m in top_3_longest_movies:
        top_3_longest_movies_fmt += f"""
        - {m}
        
        """
    return m, top_3_longest_movies_fmt


@app.cell
def _(df_full):
    df_full.select('Name', 'Runtime_normalized').drop_nulls().sort('Runtime_normalized', descending=True)
    return


@app.cell
def _(mo, top_month):
    mo.md(
        f"""
            3. **Monthly Trends**  
               - Aggregates the runtime of movies watched each month and converts it into hours. By grouping the data dynamically by month, this analysis reveals how much time was spent watching movies in different months, highlighting trends or patterns in viewing habits.

               Busiest month: **{top_month.strftime("%B")}**

        """
    )
    return


@app.cell
def _(df_full, pl):
    top_month = df_full.group_by_dynamic('Watched Date', every='1mo').agg(pl.col('Runtime_normalized').sum() / 60).sort('Runtime_normalized', descending=True).select('Watched Date').head(1).item()
    return (top_month,)


@app.cell
def _(df_full, pl):
    df_full.group_by_dynamic('Watched Date', every='1mo').agg(pl.col('Runtime_normalized').sum() / 60).sort('Runtime_normalized', descending=True).with_columns(
        pl.col('Watched Date').dt.strftime("%B").alias('Month'),
        pl.col('Runtime_normalized').cast(pl.Int32).alias('Hours')
    ).select('Month', 'Hours')
    return


@app.cell
def _(mo):
    mo.md(
        """
        ''### **Top Categories**
        - **Top Genres**: The most frequently watched genres.  
        - **Top Actors**: The actors appearing most often in the movies.  
        - **Top Writers**: The most recurring writers.  
        - **Top Countries and Languages**: The most common production countries and spoken languages.
        """
    )
    return


@app.cell
def _(df_full, get_top_from_list):
    get_top_from_list(df_full, 'Genre', 10)
    return


@app.cell
def _(df_full, get_top_from_list):
    get_top_from_list(df_full, 'Actors', 10)
    return


@app.cell
def _(df_full, get_top_from_list):
    get_top_from_list(df_full, 'Writer', 10)
    return


@app.cell
def _(df_full, get_top_from_list):
    get_top_from_list(df_full, 'Country', 10)
    return


@app.cell
def _(df_full, get_top_from_list):
    get_top_from_list(df_full, 'Language', 10)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ### **Highest Rated Movies**
        - **Top by Rating**: Movies sorted by user ratings.  
        - **Top by Metascore**: Movies sorted by critical scores.
        """
    )
    return


@app.cell
def _(df_full):
    df_full.select('Name', 'Rating').drop_nulls().sort('Rating', descending=True)
    return


@app.cell
def _(df_full):
    df_full.select('Name', 'Metascore').drop_nulls().sort('Metascore', descending=True)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ### **Interesting Comparisons**
        - **Critics vs. Audience**: Identifies movies with the largest discrepancies between user ratings, Metascore, and IMDB ratings.  
        - **Rating Consistency**: Highlights how user preferences align with audience or critic opinions.
        """
    )
    return


@app.cell
def _(df_full):
    df_full.select('Name', 'Rating_Difference_Metascore', 'Rating', 'Normalized_Metascore').drop_nulls().sort('Rating_Difference_Metascore', descending=True)
    return


@app.cell
def _(df_full):
    df_full.select('Name', 'Rating_Difference_IMDB', 'Rating', 'Normalized_IMDB').drop_nulls().sort('Rating_Difference_IMDB', descending=True)
    return


@app.cell
def _(df_full, pl):
    df_full.group_by('Year').agg(pl.col('Name').count().alias('count')).sort('count', descending=True)
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
