import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell
def _():
    import zipfile
    import requests
    import os
    import io
    import json
    import polars as pl
    import seaborn as sns
    import matplotlib.pyplot as plt
    from dotenv import load_dotenv
    from collections import Counter
    from datetime import datetime


    load_dotenv()

    is_local = True
    return (
        Counter,
        datetime,
        io,
        is_local,
        json,
        load_dotenv,
        os,
        pl,
        plt,
        requests,
        sns,
        zipfile,
    )


@app.cell
def _(mo):
    file = mo.ui.file(kind="button")
    file
    return (file,)


@app.cell
def _(io, pl, zipfile):
    def process_zip_and_load_csv(file_contents):
        with zipfile.ZipFile(io.BytesIO(file_contents)) as zf:
            if "diary.csv" in zf.namelist():
                with zf.open("diary.csv") as csv_file:
                    df = pl.read_csv(csv_file).filter((pl.col("Watched Date") > pl.lit("2024-01-01")) & (pl.col("Watched Date") < pl.lit("2025-01-01")))
                    return df
            else:
                raise FileNotFoundError(
                    "'diary.csv' not found"
                )
    return (process_zip_and_load_csv,)


@app.cell
def _(file, pl, process_zip_and_load_csv):
    df = pl.DataFrame()

    if file.name():
        df = process_zip_and_load_csv(file.contents())
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
def _(is_local):
    if is_local:
        CACHE_FILE = "/Users/filippomameli/Projects/letterboxd_wrapped/movie_cache.json"
    else:
        CACHE_FILE = "https://raw.githubusercontent.com/mameli/letterboxd_wrapped/refs/heads/main/movie_cache.json"
    return (CACHE_FILE,)


@app.cell
def _(CACHE_FILE, is_local, json, os, requests):
    def load_cache():
        if is_local:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, 'r') as f:
                    return json.load(f)
        else:
            return requests.get(CACHE_FILE).json()
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
    def get_metatadata(diary_df, api_key):
        for t, y in diary_df[['Name', 'Year']].rows():
            print(f"Getting data for {t} ({y})")
            get_movie_data(t, y, api_key)

    # get_metatadata(df_fmt, API_KEY)
    return (get_metatadata,)


@app.cell
def _(mo):
    mo.center(mo.md("# **Letterbox Diary Insights**"))
    return


@app.cell
def _(mo):
    mo.md(
        """
            This application processes movie diary data exported from **Letterboxd** and enriches it with detailed metadata to analyze and uncover interesting insights. 

            The exported data serves as a starting point, containing a basic list of movies watched, and is transformed into a rich dataset with comprehensive information about genres, runtime, directors, writers, and more."""
    ).style({"text-align": "center"})
    return


@app.cell
def _(mo):
    mo.md(
        f"""
        {mo.center(mo.image(         
            src="https://raw.githubusercontent.com/mameli/letterboxd_wrapped/refs/heads/main/imgs/letterboxd.png",
            alt="Letterboxd logo",
            width=500,
            caption="By Mameli with Letterboxd",
        ))}

        ---
        """
    ).style({"text-align": "center"})
    return


@app.cell
def _(mo):
    mo.center(
        mo.accordion(
            {
                "Data structure detail": mo.md(
                    """
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
            - **Box Office**: Total gross revenue from a film’s theatrical release.
            - **Content rating**: Age-appropriate classification (e.g., PG, R, etc.).
        """
                ),
            }
        )
    )
    return


@app.cell
def _(df_full, mo):
    mo.center(
        mo.accordion(
            {
                "Dataframe example": df_full,
            }
        )
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
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Rated"
            )
        )
        .alias("Rated"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "BoxOffice"
            )
        )
        .alias("BoxOffice"),
        pl.struct(["title_year"])
        .map_batches(
            lambda movie: extract_metadata(
                cache, movie.struct.field("title_year"), "Poster"
            )
        )
        .alias("Poster"),
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
        pl.col("BoxOffice")
        .str.replace_all(r"[\$,]", "")
        .str.to_integer(strict=False)
        .alias("BoxOffice"),
    )

    df_full = df_full.with_columns(
        (pl.col("Rating") / pl.col("BoxOffice")).alias("RatingPerEarning"),
    )
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

        return item_counts.most_common(top)
    return (get_top_from_list,)


@app.cell
def _(mo):
    mo.center(
        mo.md(
            r"""
        ## **Insights and Results**
        The application generates the following analyses and results from the enriched dataframe:
        """
        ).style({"text-align": "center"})
    )
    return


@app.cell
def _(mo):
    mo.center(
        mo.md("### **General Movie Statistics**")
    )
    return


@app.cell
def _(df_full, mo, number_unique_directors, pl, total_movies_watched):
    mo.center(
        mo.md(
            f"""
        - **Total Movies Watched**: The total number of movies logged -> **{total_movies_watched}**
        - **Unique Directors**: The number of unique directors in the dataset -> **{number_unique_directors}**
        - **Top Directors**: The directors with the most movies in the dataset. -> **{df_full.group_by('Director').agg(pl.col('Name').count().alias('count')).sort('count', descending=True).select('Director').head(1).item()}**
        """
        )
    )
    return


@app.cell
def _(df_full, mo, pl, plt, sns):
    sns.set(style="whitegrid", font="serif")

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=df_full.group_by("Director")
        .agg(pl.col("Name").count().alias("count"))
        .sort("count", descending=True)
        .head(12)
        .to_pandas(),
        y="Director",  # Horizontal orientation: Directors on the y-axis
        x="count",  # Movie counts on the x-axis
        hue="Director",
        palette="pastel",  # Reverse Blues color palette
    )


    # Customize the plot
    plt.title("Number of Movies by Director", fontsize=16)
    plt.xlabel("Number of Movies", fontsize=12)
    plt.ylabel(" ", fontsize=12)

    # Set integer ticks for the x-axis
    max_count = (
        df_full.group_by("Director")
        .agg(pl.col("Name").count().alias("count"))
        .to_pandas()["count"]
        .max()
    )
    plt.xticks(
        ticks=range(0, int(max_count) + 1), fontsize=12
    )

    plt.yticks(fontsize=12)
    plt.tight_layout()

    mo.center(plt.gca())
    return (max_count,)


@app.cell
def _(df_full):
    total_movies_watched = df_full.select('Name').count().item()
    return (total_movies_watched,)


@app.cell
def _(df_full):
    number_unique_directors = df_full.select('Director').n_unique()
    return (number_unique_directors,)


@app.cell
def _(df_full, mo, pl):
    mo.center(mo.accordion(
        {
            "Full director list": df_full.group_by("Director")
            .agg(pl.col("Name").count().alias("count"))
            .sort("count", descending=True),
        }
    ))
    return


@app.cell
def _(mo):
    mo.center(mo.md("### **Time Spent Watching Movies**"))
    return


@app.cell
def _(mo, top_3_longest_movies_stack, total_runtime):
    mo.center(
        mo.md(
            f"""


        This analysis focuses on the total runtime of movies, identifying the longest movies watched, and examining monthly trends based on runtime.

        1. **Total Runtime -> {total_runtime}h**
           - Calculates the total time spent watching movies in hours by summing up the normalized runtime of all entries. This gives a clear picture of the overall time investment in movie-watching.

        2. **Longest Movies**  
            Identifies the longest movies watched by sorting the dataset by runtime in descending order. This highlights which movies required the most time to watch.

            **Top 3**: 
                {mo.hstack(
                    top_3_longest_movies_stack,
                    align="center",
                    justify="center",
                    gap="1",
                )}
        """
        )
    )
    return


@app.cell
def _(df_full):
    total_runtime = int(df_full.select('Runtime_normalized').sum().item() / 60)
    return (total_runtime,)


@app.cell
def _(df_full):
    top_3_longest_movies = df_full.select('Name', 'Runtime_normalized', 'Poster').drop_nulls().sort('Runtime_normalized', descending=True).select('Name', 'Poster').head(3).to_dicts()
    return (top_3_longest_movies,)


@app.cell
def _(mo, top_3_longest_movies):
    top_3_longest_movies_stack = []
    for m in top_3_longest_movies:
        top_3_longest_movies_stack.append(
            mo.md(
                f"""
            {m['Name']}
            ![{m['Name']} poster]({m['Poster']})
            """
            )
        )
    return m, top_3_longest_movies_stack


@app.cell
def _(mo, top_month):
    mo.center(
        mo.md(
            f"""
            3. **Monthly Trends**  
               - Aggregates the runtime of movies watched each month and converts it into hours. 

                   By grouping the data dynamically by month, this analysis reveals how much time was spent watching movies in different months, highlighting trends or patterns in viewing habits.

                   Busiest month: **{top_month.strftime("%B")}**

        """
        )
    )
    return


@app.cell
def _(df_full, pl):
    month_hour_list = (
       df_full.group_by_dynamic("Watched Date", every="1mo")
        .agg(pl.col("Runtime_normalized").sum() / 60)
        .with_columns(
            pl.col("Watched Date").dt.strftime("%B").alias("Month"),
            pl.col("Runtime_normalized").cast(pl.Int32).alias("Hours"),
        )
        .select("Month", "Hours")
    )
    return (month_hour_list,)


@app.cell
def _(mo, month_hour_list, plt, sns):
    sns.set(style="whitegrid", font="serif")
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=month_hour_list,
        x="Month",
        y="Hours",
        linewidth=2.5,  # Adjust the line thickness
        marker="o",  # Use a circular marker
        markersize=10,  # Increase the size of the markers
        color=sns.color_palette("pastel")[0],  # Use a pastel color
    )

    # Graph customization
    plt.title("Hours per Month", fontsize=14)
    plt.xlabel("Months", fontsize=12)
    plt.ylabel("Hours", fontsize=12)
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.tight_layout()

    # Display the graph
    mo.center(plt.gca())
    return


@app.cell
def _(df_full, pl):
    top_month = (
        df_full.group_by_dynamic("Watched Date", every="1mo")
        .agg(pl.col("Runtime_normalized").sum() / 60)
        .sort("Runtime_normalized", descending=True)
        .select("Watched Date")
        .head(1)
        .item()
    )
    return (top_month,)


@app.cell
def _(df_full, mo, pl):
    mo.center(
        mo.accordion(
            {
                "Month list": df_full.group_by_dynamic("Watched Date", every="1mo")
                .agg(pl.col("Runtime_normalized").sum() / 60)
                .sort("Runtime_normalized", descending=True)
                .with_columns(
                    pl.col("Watched Date").dt.strftime("%B").alias("Month"),
                    pl.col("Runtime_normalized").cast(pl.Int32).alias("Hours"),
                )
                .select("Month", "Hours")
            }
        )
    )
    return


@app.cell
def _(df_full, pl):
    top_rated = df_full.group_by('Rated').agg(pl.col('Name').count().alias('count')).sort('count', descending=True).head(1).to_dict()
    return (top_rated,)


@app.cell
def _(df_full, pl):
    top_boxoffice = df_full.select('Name','BoxOffice', 'Poster').drop_nulls().sort(pl.col('BoxOffice'), descending=True).select('Name', 'Poster').head(1).to_dicts()[0]
    return (top_boxoffice,)


@app.cell
def _(mo):
    mo.center(mo.md("### **Top Categories**"))
    return


@app.cell
def _(df_full, get_top_from_list, mo, top_boxoffice):
    mo.center(
        mo.md(
            f"""
        - **Top Genres**: The most frequently watched genres.  **{get_top_from_list(df_full, 'Genre', 1)[0][0]}**
        - **Top Actors**: The actors appearing most often in the movies. **{get_top_from_list(df_full, 'Actors', 1)[0][0]}**
        - **Top Writers**: The most recurring writers.  **{get_top_from_list(df_full, 'Writer', 1)[0][0]}**
        - **Top Countries and Languages**: The most common production countries and spoken languages. **{get_top_from_list(df_full, 'Country', 1)[0][0]}** , **{get_top_from_list(df_full, 'Language', 1)[0][0]}**
        - **BoxOffice sensation**: The highest-grossing movie I watched this year is **{top_boxoffice['Name']}**
        """
        )
    )
    return


@app.cell
def _(mo, top_boxoffice):
    mo.center(mo.md(f"""![{top_boxoffice['Name']} poster]({top_boxoffice['Poster']})"""))
    return


@app.cell
def _(mo, top_rated):
    mo.center(
        mo.md(f""" **Fun fact**: [The motion picture content rating system]('https://en.wikipedia.org/wiki/Motion_picture_content_rating_system#United_States') is really strict. Even Inside Out and Shrek are not rated “All ages admitted.”

        - I watched **{top_rated['count'].item()}** **{top_rated['Rated'].item()}** rated movies
        """)
    )
    return


@app.cell
def _():
    # get_top_from_list(df_full, 'Genre', 10)
    # get_top_from_list(df_full, 'Actors', 10)
    # get_top_from_list(df_full, 'Writer', 10)
    # get_top_from_list(df_full, 'Country', 10)
    # get_top_from_list(df_full, 'Language', 10)
    return


@app.cell
def _(mo):
    mo.center(mo.md("### **Highest Rated Movies**"))
    return


@app.cell
def _(mo, top_3_user_rating_stack):
    mo.center(
        mo.md(
            f"""
        - **Top by Rating**: Movies sorted by user ratings.

        **Top 3**: 
        {mo.hstack(
                    top_3_user_rating_stack,
                    align="center",
                    justify="center",
                    gap="1",
                )}
        """
        )
    )
    return


@app.cell
def _(df_full):
    top_user_rating = df_full.select('Name', 'Rating', 'Poster').drop_nulls().sort('Rating', descending=True).head(3).to_dicts()
    return (top_user_rating,)


@app.cell
def _(mo, top_user_rating):
    top_3_user_rating_stack = []
    for mr in top_user_rating:
        top_3_user_rating_stack.append(mo.md(
            f"""
            {mr['Name']}
            ![{mr['Name']} poster]({mr['Poster']})
            """
        ))
    return mr, top_3_user_rating_stack


@app.cell
def _(mo):
    mo.center(
        mo.md("""- **Top by Metascore**: Movies sorted by critical scores.""")
    )
    return


@app.cell
def _(mo, plt, sns):
    def plot_top_scores(df, column_name):
        """
        Plots a barplot for the top 10 movies based on a given column (e.g., 'Metascore' or 'imdbRating').

        Args:
        - df: DataFrame containing movie data.
        - column_name: Column name to be used for the plot (e.g., 'Metascore' or 'imdbRating').
        """
        # Set the style
        sns.set(style="whitegrid", font="serif")

        # Create the barplot
        plt.figure(figsize=(12, 6))
        plot_data = (
            df
            .select("Name", column_name)
            .drop_nulls()
            .sort(column_name, descending=True)
            .head(10)
            .to_pandas()
        )

        barplot = sns.barplot(
            data=plot_data,
            x="Name",
            y=column_name,
            hue=column_name,
            palette="pastel",
            legend=False,
        )

        # Add values on top of the bars
        for bar, score in zip(barplot.patches, plot_data[column_name].iloc[::-1]):
            barplot.text(
                bar.get_x() + bar.get_width() / 2,  # Center text on the bar
                bar.get_height() + .1,              # Place text slightly above the bar
                f"{score}",                   # Text content (integer score)
                ha="center", fontsize=10
            )

        # Customize the plot
        plt.title(f"Top {column_name.capitalize()}", fontsize=16, weight="bold")
        plt.xlabel("Movie", fontsize=14)
        plt.ylabel(column_name.capitalize(), fontsize=14)
        plt.xticks(rotation=80, fontsize=10, weight="bold")
        plt.yticks(fontsize=12)
        plt.tight_layout()
        plt.tick_params(labelsize=12)

        return mo.center(plt.gca())
    return (plot_top_scores,)


@app.cell
def _(df_full, mo, plot_top_scores):
    mo.center(mo.ui.tabs(
        {
            "Metacritic": plot_top_scores(df_full, "Metascore"),
            "IMDB": plot_top_scores(df_full, "imdbRating"),
        }
    ))
    return


@app.cell
def _(df_full, mo):
    mo.center(
        mo.accordion(
            {
                "Metascore full list": df_full.select("Name", "Metascore")
                .drop_nulls()
                .sort("Metascore", descending=True),
                "IMDB full list": df_full.select("Name", "imdbRating")
                .drop_nulls()
                .sort("imdbRating", descending=True),
            }
        )
    )
    return


@app.cell
def _(mo):
    mo.center(mo.md("### **Interesting Comparisons**"))
    return


@app.cell
def _(mo):
    mo.center(
        mo.md(
            """
        - **Critics vs. Audience**: Identifies movies with the largest discrepancies between user ratings, Metascore, and IMDB ratings.
        """
        )
    )
    return


@app.cell
def _(mo, plt, sns):
    def plot_rating_differences(df, rating_diff_col, normalized_col, title_suffix=""):
        """
        Plots a bar plot showing your ratings vs critic ratings for the top 10 movies based on a rating difference column.

        Args:
            df: The Polars DataFrame containing the data.
            rating_diff_col: The column representing the difference between your rating and the critic's.
            normalized_col: The column representing the normalized critic rating.
            title_suffix: A string to append to the plot title (e.g., "Metascore" or "IMDB").
        """
        # Set the style
        sns.set(style="whitegrid", font="serif")

        # Prepare the data
        plot_data = (
            df.select("Name", rating_diff_col, "Rating", normalized_col)
            .drop_nulls()
            .sort(rating_diff_col, descending=True)
            .head(10)
            .to_pandas()
        )

        # Melt the data for side-by-side bar plotting
        plot_data_melted = plot_data.melt(
            id_vars=["Name"],
            value_vars=["Rating", normalized_col],
            var_name="Rating Type",
            value_name="Score",
        )

        # Create the barplot
        plt.figure(figsize=(12, 6))
        barplot_difference = sns.barplot(
            data=plot_data_melted,
            x="Name",
            y="Score",
            hue="Rating Type",
            palette="coolwarm",
        )

        # Move the legend
        sns.move_legend(barplot_difference, "lower right")

        # Customize the plot
        plt.title(f"Your Rating vs Critic Rating ({title_suffix})", fontsize=16)
        plt.xlabel("Movie", fontsize=14)
        plt.ylabel("Ratings", fontsize=14)
        plt.xticks(fontsize=12, rotation=80, weight="bold")
        plt.yticks(fontsize=12)
        plt.tick_params(labelsize=12)

        # Display the plot
        return mo.center(plt.gca())
    return (plot_rating_differences,)


@app.cell
def _(df_full, mo, plot_rating_differences):
    mo.center(mo.ui.tabs(
        {
            "Metacritic": plot_rating_differences(
                df=df_full,
                rating_diff_col="Rating_Difference_Metascore",
                normalized_col="Normalized_Metascore",
                title_suffix="Metascore",
            ),
            "IMDB": plot_rating_differences(
                df=df_full,
                rating_diff_col="Rating_Difference_IMDB",
                normalized_col="Normalized_IMDB",
                title_suffix="IMDB",
            ),
        }
    ))
    return


@app.cell
def _(df_full, mo):
    mo.center(
        mo.accordion(
            {
                "Metascore difference full": df_full.select(
                    "Name",
                    "Rating_Difference_Metascore",
                    "Rating",
                    "Normalized_Metascore",
                )
                .drop_nulls()
                .sort("Rating_Difference_Metascore", descending=True),
                "IMDB difference full": df_full.select(
                    "Name", "Rating_Difference_IMDB", "Rating", "Normalized_IMDB"
                )
                .drop_nulls()
                .sort("Rating_Difference_IMDB", descending=True),
            }
        )
    )
    return


@app.cell
def _():
    # df_full.select("Name", "BoxOffice", "Rating", "RatingPerEarning").drop_nulls()
    return


@app.cell
def _():
    # df_full.group_by('Year').agg(pl.col('Name').count().alias('count')).sort('count', descending=True)
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
