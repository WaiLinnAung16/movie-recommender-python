# imdb_etl_mysql_admin.py

import os
import csv
from typing import List, Dict
from flask import Flask, request, render_template_string, redirect, url_for, session
from flask_sqlalchemy import SQLAlchemy

# -------------------- Config --------------------
DB_URI = "mysql+pymysql://root@localhost/imdb_db"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "1234"

db = SQLAlchemy()


# -------------------- Model --------------------
class Movie(db.Model):
    __tablename__ = "movies"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255))
    genre = db.Column(db.String(255))
    year = db.Column(db.Integer)
    rating = db.Column(db.Float)
    director = db.Column(db.String(255))


# -------------------- ETL Pipeline --------------------
class ETLService:

    def extract(self, filepath: str) -> List[Dict]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(filepath)

        with open(filepath, encoding='utf-8') as f:
            return list(csv.DictReader(f))

    def transform(self, raw_data: List[Dict]) -> List[Dict]:
        cleaned = []

        for row in raw_data:
            if not row.get('IMDB_Rating'):
                continue

            try:
                cleaned.append({
                    "title": row['Series_Title'],
                    "genre": row['Genre'],
                    "year": int(row['Released_Year']),
                    "rating": float(row['IMDB_Rating']),
                    "director": row['Director']
                })
            except:
                continue

        return cleaned

    def load(self, data: List[Dict]):
        for d in data:
            exists = Movie.query.filter_by(title=d["title"], year=d["year"]).first()
            if not exists:
                db.session.add(Movie(**d))

        db.session.commit()

    def run(self, filepath: str):
        raw = self.extract(filepath)
        transformed = self.transform(raw)
        self.load(transformed)


# -------------------- Services --------------------
class MovieService:

    def filter_movies(self, genre, start_year, end_year):
        query = Movie.query

        if genre:
            query = query.filter(Movie.genre.ilike(f"%{genre}%"))

        query = query.filter(Movie.year.between(start_year, end_year))

        return query.all()

    def top_movies(self, movies, limit=5):
        return sorted(movies, key=lambda m: m.rating, reverse=True)[:limit]


class AuthService:
    def login(self, username, password):
        return username == ADMIN_USERNAME and password == ADMIN_PASSWORD


# -------------------- App Factory --------------------
def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = DB_URI
    app.config["SECRET_KEY"] = "secret"
    db.init_app(app)

    movie_service = MovieService()
    auth_service = AuthService()

    # -------------------- Templates --------------------
    LOGIN_HTML = """
    <h2>Login</h2>
    <form method="POST">
        UserName<input name="username"><br><br>
        Password<input type="password" name="password"><br><br>
        <button>Login</button>
    </form>
    {% if error %}
    <p style="color:red;">{{ error }}</p>
    {% endif %}
    """

    DASHBOARD_HTML = """
    <h2>Dashboard</h2>
    <a href="/admin/add">Add</a> | <a href="/admin/logout">Logout</a>

    <form method="GET">
        <input name="search" placeholder="Search title">
        <button>Search</button>
    </form>

    <table border="1">
        <tr>
            <th>Title</th><th>Genre</th><th>Year</th><th>Rating</th><th>Action</th>
        </tr>
        {% for m in movies %}
        <tr>
            <td>{{ m.title }}</td>
            <td>{{ m.genre }}</td>
            <td>{{ m.year }}</td>
            <td>{{ m.rating }}</td>
            <td>
                <a href="/admin/edit/{{m.id}}">Edit</a>
                <a href="/admin/delete/{{m.id}}">Delete</a>
            </td>
        </tr>
        {% endfor %}
    </table>
    """

    FORM_HTML = """
    <h2>{{ title }}</h2>
    <form method="POST">
        <input name="title" value="{{ m.title if m else '' }}"><br>
        <input name="genre" value="{{ m.genre if m else '' }}"><br>
        <input name="year" type="number" value="{{ m.year if m else '' }}"><br>
        <input name="rating" type="number" step="0.1" value="{{ m.rating if m else '' }}"><br>
        <input name="director" value="{{ m.director if m else '' }}"><br>
        <button>Save</button>
    </form>
    <p>{{ msg }}</p>
    """

    # -------------------- Auth --------------------
    def is_admin():
        return "admin" in session

    @app.route("/admin/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            if auth_service.login(request.form["username"], request.form["password"]):
                session["admin"] = True
                return redirect("/admin/dashboard")
            else:
                error = "Invalid login"
        return render_template_string(LOGIN_HTML, error=error)

    @app.route("/admin/logout")
    def logout():
        session.clear()
        return redirect("/admin/login")

    # -------------------- Dashboard --------------------
    @app.route("/admin/dashboard")
    def dashboard():
        if not is_admin():
            return redirect("/admin/login")

        search = request.args.get("search")
        if search:
            movies = Movie.query.filter(Movie.title.ilike(f"%{search}%")).all()
        else:
            movies = Movie.query.all()

        return render_template_string(DASHBOARD_HTML, movies=movies)

    # -------------------- Create --------------------
    @app.route("/admin/add", methods=["GET", "POST"])
    def add():
        if not is_admin():
            return redirect("/admin/login")

        msg = None
        if request.method == "POST":
            db.session.add(Movie(
                title=request.form["title"],
                genre=request.form["genre"],
                year=int(request.form["year"]),
                rating=float(request.form["rating"]),
                director=request.form["director"]
            ))
            db.session.commit()
            msg = "Added!"

        return render_template_string(FORM_HTML, title="Add Movie", m=None, msg=msg)

    # -------------------- Update --------------------
    @app.route("/admin/edit/<int:id>", methods=["GET", "POST"])
    def edit(id):
        if not is_admin():
            return redirect("/admin/login")

        m = Movie.query.get_or_404(id)
        msg = None

        if request.method == "POST":
            m.title = request.form["title"]
            m.genre = request.form["genre"]
            m.year = int(request.form["year"])
            m.rating = float(request.form["rating"])
            m.director = request.form["director"]

            db.session.commit()
            msg = "Updated!"

        return render_template_string(FORM_HTML, title="Edit Movie", m=m, msg=msg)

    # -------------------- Delete --------------------
    @app.route("/admin/delete/<int:id>")
    def delete(id):
        if not is_admin():
            return redirect("/admin/login")

        m = Movie.query.get_or_404(id)
        db.session.delete(m)
        db.session.commit()

        return redirect("/admin/dashboard")

    # -------------------- Public Recommendation --------------------
    @app.route("/", methods=["GET", "POST"])
    def home():
        movies = []
        if request.method == "POST":
            genre = request.form["genre"]
            start = int(request.form["start"])
            end = int(request.form["end"])

            filtered = movie_service.filter_movies(genre, start, end)
            movies = movie_service.top_movies(filtered)

            
           
        return render_template_string("""
        <h2>Recommend</h2>
        <form method="POST">
            Genre: <input name="genre"><br><br>
            Start date: <input name="start" type="number"><br><br>
            End date: <input name="end" type="number"><br><br>
            <button>Search</button>
        </form>
                

        {% if movies %}
            <ul>
            {% for m in movies %}
                <li>{{m.title}} ({{m.year}}) - {{m.rating}}</li>
            {% endfor %}
            </ul>
        {% else %}
            <p>No movies found</p>
        {% endif %}
        """, movies=movies)

    return app


# -------------------- Main --------------------
def main():
    app = create_app()

    with app.app_context():
        db.create_all()

        # Run ETL once
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(BASE_DIR, "data", "imdb-top-1000.csv")

        etl = ETLService()
        etl.run(csv_path)

    app.run(debug=True)

if __name__ == "__main__":
    main()
