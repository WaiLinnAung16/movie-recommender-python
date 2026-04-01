import os 
import tempfile 
import pytest 
import sys 
 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))) 
from imdb_etl_mysql_admin_secure import ( 
    create_app, db, ETLService, 
    validate_movie_form, AuthService, MovieService, Movie 
) 
 
# -------------------- Test App Fixture -------------------- 
@pytest.fixture 
def app(): 
    app = create_app() 
    app.config.update({ 
        "TESTING": True, 
        "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:" 
    }) 
 
    with app.app_context(): 
        db.create_all() 
        yield app 
        db.drop_all() 
 
@pytest.fixture 
def client(app): 
    return app.test_client()
# -------------------- ETL Tests -------------------- 
def test_transform_valid_data(): 
    etl = ETLService() 
    raw = [{ 
        "Series_Title": "Inception", 
        "Genre": "Sci-Fi", 
        "Released_Year": "2010", 
        "IMDB_Rating": "8.8", 
        "Director": "Nolan" 
    }] 
 
    result = etl.transform(raw) 
 
    assert len(result) == 1 
    assert result[0]["title"] == "Inception" 
    assert result[0]["rating"] == 8.8 
 
def test_transform_invalid_data(): 
    etl = ETLService() 
    raw = [{"Series_Title": "Bad Data"}]  # missing fields 
 
    result = etl.transform(raw) 
 
    assert result == [] 
 
def test_extract_invalid_path(): 
    etl = ETLService() 
 
    with pytest.raises(PermissionError): 
        etl.extract("/etc/passwd") 
 
# -------------------- Validation Tests -------------------- 
def test_validate_movie_form_success(): 
    form = { 
        "title": "Test", 
        "genre": "Drama", 
        "year": "2020", 
        "rating": "7.5", 
        "director": "Me" 
    } 
 
    data, error = validate_movie_form(form) 
 
    assert error is None 
    assert data["title"] == "Test" 
 
def test_validate_movie_form_failure(): 
    form = { 
        "title": "", 
        "genre": "",
        "year": "abc", 
        "rating": "bad" 
    } 
 
    data, error = validate_movie_form(form) 
 
    assert data is None 
    assert error is not None 
 
# -------------------- Auth Tests -------------------- 
def test_auth_success(monkeypatch): 
    auth = AuthService() 
 
    # override env password hash 
    from werkzeug.security import generate_password_hash 
    monkeypatch.setenv("ADMIN_PASSWORD_HASH", 
generate_password_hash("1234")) 
 
    assert auth.login("admin", "1234") is True 
 
def test_auth_failure(): 
    auth = AuthService() 
    assert auth.login("admin", "wrong") is False 
 
# -------------------- MovieService Tests -------------------- 
def test_movie_service_filter(app): 
    with app.app_context(): 
        db.session.add(Movie(title="A", genre="Action", year=2020, rating=8.0)) 
        db.session.add(Movie(title="B", genre="Drama", year=2021, rating=7.0)) 
        db.session.commit() 
 
        service = MovieService() 
        results = service.filter_movies("Action", 2019, 2021) 
 
        assert len(results) == 1 
        assert results[0].title == "A" 
 
def test_top_movies(): 
    service = MovieService() 
 
    class Dummy: 
        def __init__(self, rating): 
            self.rating = rating 
 
    movies = [Dummy(5), Dummy(9), Dummy(7)] 
    top = service.top_movies(movies, limit=2) 
 
    assert len(top) == 2 
    assert top[0].rating == 9
# -------------------- Flask Route Tests -------------------- 
def test_home_page(client): 
    res = client.get("/") 
    assert res.status_code == 200 
 
def test_login_fail(client): 
    res = client.post("/admin/login", data={ 
        "username": "admin", 
        "password": "wrong" 
    }) 
    assert b"Invalid login" in res.data 
 
def test_protected_route_redirect(client): 
    res = client.get("/admin/dashboard") 
    assert res.status_code == 302  # redirect to login 
 
# -------------------- DB Insert Test -------------------- 
def test_add_movie(app): 
    with app.app_context(): 
        movie = Movie(title="Test", genre="Drama", year=2022, rating=8.0) 
        db.session.add(movie) 
        db.session.commit() 
 
        assert Movie.query.count() == 1