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
    import plotly.express as px
    from dotenv import load_dotenv
    from collections import Counter
    from datetime import datetime


    load_dotenv()

    is_local = False

    if is_local:
        zip_file_path = "/Users/filippomameli/Projects/letterboxd_wrapped/export/Account Settings Jan 6 2025.zip"

        extract_to_path = "/Users/filippomameli/Projects/letterboxd_wrapped/extracted_files"

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)

        diary_path = f"{extract_to_path}/diary.csv"
    else:
        diary_path = f"https://raw.githubusercontent.com/mameli/letterboxd_wrapped/refs/heads/main/extracted_files/diary.csv"
    return (
        Counter,
        datetime,
        diary_path,
        extract_to_path,
        io,
        is_local,
        json,
        load_dotenv,
        os,
        pl,
        px,
        requests,
        zip_file_path,
        zip_ref,
        zipfile,
    )


@app.cell
def _(diary_path, pl):
    # read csv from extracted files diary with polars

    df = (
        pl.read_csv(diary_path)
        .filter((pl.col("Watched Date") > pl.lit("2024-01-01")) & (pl.col("Watched Date") < pl.lit("2025-01-01")))
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
    mo.center(
        mo.md("""# **Letterboxd Movie Analysis**""")
    )
    return


@app.cell
def _(mo):
    mo.center(
        mo.md("""## Uncovering My Watching Habits""")
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
            This application processes movie diary data exported from **Letterboxd** and enriches it with detailed metadata to analyze and uncover interesting insights. 

            [Letterboxd](https://letterboxd.com/) is a social platform designed for movie enthusiasts. It allows users to log, rate, and review the movies they watch while keeping track of their viewing habits.

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
            width=400,
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
                "Dataframe example": df_full.select(
                    "title_year",
                    "Genre",
                    "Runtime_normalized",
                    "Director",
                    "Actors",
                    "Language",
                    "Metascore",
                )
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
def _(df_full, mo, pl, px):
    directors_data = (
        df_full.group_by("Director")
        .agg(pl.col("Name").count().alias("count"))
        .sort("count", descending=True)
        .head(13)
    )

    directors = directors_data["Director"].to_list()
    counts = directors_data["count"].to_list()

    fig = px.bar(
        x=counts,  
        y=directors,  
        labels={"x": "Number of Movies", "y": "Director"},
        title="Number of Movies by Director",
        color=directors, 
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )

    # Personalizza il grafico
    fig.update_layout(
        xaxis_title="Number of Movies",
        yaxis_title="",
        title_font_size=16,
        title_font_weight="bold",
        xaxis_tickfont_size=12,
        yaxis_tickfont_size=12,
        margin=dict(l=0, r=0, t=40, b=40),
        yaxis=dict(autorange="reversed"),
        showlegend=False,

    )

    mo.center(mo.ui.plotly(fig)).style({"overflow": "auto", "width": "100%"})
    return counts, directors, directors_data, fig


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
           - The Time spent watching movies in hours by summing up the normalized runtime of all entries. This gives a clear picture of the overall time investment in movie-watching.

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
def _(mo, px):
    def plot_hours_per_month_plotly(month_hour_list):
        """
        Creates an interactive line chart for hours worked per month.

        Args:
            month_hour_list: Polars DataFrame containing "Month" and "Hours" columns.
        """
        # Convert Polars DataFrame to Pandas for Plotly
        plot_data = month_hour_list

        # Create the Plotly line chart
        fig = px.line(
            plot_data,
            x="Month",
            y="Hours",
            markers=True,  # Add circular markers
            title="Hours per Month",
            labels={"Month": "Months", "Hours": "Hours"},  # Axis labels
            line_shape="linear",  # Line interpolation
        )

        # Customize the chart
        fig.update_traces(
            line=dict(width=2.5, color=px.colors.qualitative.Pastel[0]),  # Line color and thickness
            marker=dict(size=10, symbol="circle", color=px.colors.qualitative.Pastel[0]),  # Marker size and color
        )
        fig.update_layout(
            title_font_size=14,
            title_font_weight="bold",
            xaxis=dict(
                title_font=dict(size=12),
                tickangle=45,  # Rotate x-axis labels
                tickfont=dict(size=12),
            ),
            yaxis=dict(
                title_font=dict(size=12),
                tickfont=dict(size=12),
            ),
            margin=dict(l=20, r=20, t=50, b=40),  # Adjust margins
            plot_bgcolor="white",  # Set background color
        )
        fig.update_xaxes(showgrid=True, gridcolor="lightgrey")
        fig.update_yaxes(showgrid=True, gridcolor="lightgrey")

        return mo.center(mo.ui.plotly(fig)).style({"overflow": "auto", "width": "100%"})
    return (plot_hours_per_month_plotly,)


@app.cell
def _(month_hour_list, plot_hours_per_month_plotly):
    plot_hours_per_month_plotly(month_hour_list)
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
        mo.md("""- **Top by Critics**: Movies sorted by critical scores (Metacritic and IMDB).""")
    )
    return


@app.cell
def _(mo, px):
    def plot_top_scores(df, column_name):
        """
        Creates an interactive bar chart for the top 10 movies based on a given column (e.g., 'Metascore' or 'imdbRating').

        Args:
        - df: Polars DataFrame containing movie data.
        - column_name: Column name to be used for the plot (e.g., 'Metascore' or 'imdbRating').
        """
        plot_data = (
            df.select("Name", column_name)
            .drop_nulls()
            .sort(column_name, descending=True)
            .head(10)
        )

        movie_titles = plot_data["Name"].to_list()
        scores = plot_data[column_name].to_list()

        fig = px.bar(
            x=movie_titles,
            y=scores,
            text=scores,
            labels={"x": "Movie", "y": column_name.capitalize()},
            title=f"Top 10 {column_name.capitalize()} Scores",
            color=scores,
            color_continuous_scale=px.colors.sequential.Teal,
        )

        fig.update_traces(
            texttemplate="%{text}",  # Format text as the score
            textposition="outside",  # Display text above the bars
        )
        fig.update_layout(
            xaxis_title="Movie",
            yaxis_title=column_name.capitalize(),
            title_font_size=16,
            title_font_weight="bold",
            xaxis_tickangle=45,  
            xaxis_tickfont=dict(size=12, family="Serif"),
            yaxis_tickfont=dict(size=12, family="Serif"),
            margin=dict(l=10, r=10, t=50, b=40),
        )

        return mo.center(mo.ui.plotly(fig)).style({"overflow": "auto", "width": "100%"})
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
def _(mo, pl, px):
    def plot_rating_differences(df, rating_diff_col, normalized_col, title_suffix=""):
        """
        Creates an interactive bar chart comparing your ratings vs critic ratings for the top 10 movies based on a rating difference column.

        Args:
            df: The Polars DataFrame containing the data.
            rating_diff_col: The column representing the difference between your rating and the critic's.
            normalized_col: The column representing the normalized critic rating.
            title_suffix: A string to append to the plot title (e.g., "Metascore" or "IMDB").
        """
        # Prepare the data
        plot_data = (
            df.select(["Name", rating_diff_col, "Rating", normalized_col])
            .drop_nulls()
            .sort(rating_diff_col, descending=True)
            .head(10)
        )

        # Extract data into lists for plotting
        names = plot_data["Name"].to_list()
        ratings = plot_data["Rating"].to_list()
        normalized_ratings = plot_data[normalized_col].to_list()

        # Prepare data for side-by-side plotting
        categories = ["Your Rating"] * len(ratings) + ["Critic Rating"] * len(normalized_ratings)
        scores = ratings + normalized_ratings
        movie_labels = names * 2  # Repeat names for both groups

        # Create a DataFrame for Plotly
        plotly_data = pl.DataFrame({
            "Movie": movie_labels,
            "Rating Type": categories,
            "Score": scores,
        })

        # Create the Plotly bar chart
        fig = px.bar(
            plotly_data,
            x="Movie",
            y="Score",
            color="Rating Type",
            barmode="group",  # Group bars side by side
            text="Score",  # Display score values on the bars
            labels={"Score": "Rating", "Movie": "Movie"},
            title=f"Your Rating vs Critic Rating ({title_suffix})",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )

        # Customize the chart
        fig.update_traces(
            texttemplate="%{text:.1f}",  # Format text values
            textposition="outside",  # Place text outside the bars
        )
        fig.update_layout(
            title_font_size=16,
            title_font_weight="bold",
            xaxis_tickangle=45,  # Rotate x-axis labels
            xaxis_tickfont=dict(size=12, family="Serif"),
            yaxis_tickfont=dict(size=12, family="Serif"),
            legend=dict(
                title="Rating Type",
                orientation="h",
                yanchor="bottom",
                y=1,
                xanchor="center",
                x=0.5,
            ),
            margin=dict(l=10, r=10, t=50, b=40),
        )

        return mo.center(mo.ui.plotly(fig)).style({"overflow": "auto", "width": "100%"})
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
