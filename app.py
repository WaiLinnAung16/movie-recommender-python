# imdb_etl_app.py

import os
import csv
from abc import ABC, abstractmethod
from typing import List, Dict
from flask import Flask, request, render_template_string


# -------------------- Domain Model --------------------
class Movie:
    def __init__(self, title: str, genres: List[str], year: int, rating: float, director: str):
        self.title = title
        self.genres = genres
        self.year = year
        self.rating = rating
        self.director = director

    def __str__(self):
        return f"{self.title} ({self.year}) - {self.rating}"


# -------------------- ETL Pipeline --------------------
class Extractor:
    def extract(self, filepath: str) -> List[Dict]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        with open(filepath, encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return list(reader)


class Transformer:
    def transform(self, raw_data: List[Dict]) -> List[Dict]:
        cleaned = []

        for row in raw_data:
            if not row.get('IMDB_Rating'):
                continue

            try:
                cleaned.append({
                    "title": row['Series_Title'],
                    "genres": row['Genre'].split(', '),
                    "year": int(row['Released_Year']),
                    "rating": float(row['IMDB_Rating']),
                    "director": row['Director']
                })
            except (ValueError, KeyError):
                continue

        return cleaned


class Loader:
    def load(self, transformed_data: List[Dict]) -> List[Movie]:
        return [
            Movie(
                title=data["title"],
                genres=data["genres"],
                year=data["year"],
                rating=data["rating"],
                director=data["director"]
            )
            for data in transformed_data
        ]


# -------------------- Data Source --------------------
class DataSource(ABC):
    @abstractmethod
    def load_movies(self) -> List[Movie]:
        pass


class ETLDataSource(DataSource):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()

    def load_movies(self) -> List[Movie]:
        raw_data = self.extractor.extract(self.filepath)
        transformed_data = self.transformer.transform(raw_data)
        return self.loader.load(transformed_data)


# -------------------- Repository --------------------
class MovieRepository:
    def __init__(self, data_source: DataSource):
        self._movies = data_source.load_movies()

    def get_all(self) -> List[Movie]:
        return self._movies


# -------------------- Services --------------------
class MovieFilterService:
    def filter_by_genre_and_year(
        self,
        movies: List[Movie],
        genre: str,
        start_year: int,
        end_year: int
    ) -> List[Movie]:
        genre = genre.lower()

        return [
            movie for movie in movies
            if genre in [g.lower() for g in movie.genres]
            and start_year <= movie.year <= end_year
        ]


class RecommendationService:
    def get_top_rated(self, movies: List[Movie], limit: int = 5) -> List[Movie]:
        return sorted(movies, key=lambda m: m.rating, reverse=True)[:limit]


# -------------------- Flask App --------------------
def create_app(repo: MovieRepository,
               filter_service: MovieFilterService,
               recommendation_service: RecommendationService) -> Flask:

    app = Flask(__name__)

    HTML_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>IMDb Movie Recommender</title>
    </head>
    <body>
        <h2>IMDb Movie Recommendation System</h2>
        <form method="POST">
            Genre: <input type="text" name="genre" required><br><br>
            Start Year: <input type="number" name="start_year" required><br><br>
            End Year: <input type="number" name="end_year" required><br><br>
            <input type="submit" value="Recommend">
        </form>

        {% if error %}
            <p style="color:red;">{{ error }}</p>
        {% endif %}

        {% if no_results %}
            <p style="color:orange;">No movies found for your criteria.</p>
        {% endif %}

        {% if recommendations %}
            <h3>Top Movies:</h3>
            <ul>
            {% for movie in recommendations %}
                <li>{{ movie }}</li>
            {% endfor %}
            </ul>
        {% endif %}
    </body>
    </html>
    """

    @app.route('/', methods=['GET', 'POST'])
    def home():
        recommendations = []
        error = None
        no_results = False

        if request.method == 'POST':
            try:
                genre = request.form['genre']
                start_year = int(request.form['start_year'])
                end_year = int(request.form['end_year'])

                if start_year > end_year:
                    raise ValueError("Start year must be <= end year.")

                movies = repo.get_all()
                filtered = filter_service.filter_by_genre_and_year(
                    movies, genre, start_year, end_year
                )

                if not filtered:
                    no_results = True
                else:
                    recommendations = recommendation_service.get_top_rated(filtered)

            except Exception as e:
                error = str(e)

        return render_template_string(
            HTML_TEMPLATE,
            recommendations=recommendations,
            error=error,
            no_results=no_results
        )

    return app


# -------------------- Main --------------------
def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(BASE_DIR, "data", "imdb-top-1000.csv")

    data_source = ETLDataSource(csv_path)
    repo = MovieRepository(data_source)

    filter_service = MovieFilterService()
    recommendation_service = RecommendationService()

    app = create_app(repo, filter_service, recommendation_service)
    app.run(debug=True)


if __name__ == '__main__':
    main()