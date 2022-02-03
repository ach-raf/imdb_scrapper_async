from abc import ABC
from dataclasses import dataclass


@dataclass
class Imdb(ABC):
    imdb_id: str
    title: str
    original_title: str
    score: int
    voters: int
    plot: str
    poster: str
    rated: str
    genre: str
    media_type: str
    release_date: str
    countries: str
    actors: str

    def insertion_command(self) -> str:
        ...


@dataclass
class ImdbSerie(Imdb):
    creator: str
    runtime: str
    years: str
    seasons: str

    def insertion_command(self):
        _insert_command = f"""INSERT INTO serie_details VALUES 
                            ("{self.imdb_id}", "{self.title}", "{self.original_title}", "{self.score}", 
                            "{self.voters}", "{self.plot}", "{self.poster}", "{self.rated}", 
                            "{self.genre}", "{self.media_type}", "{self.release_date}", "{self.countries}", "{self.actors}", 
                            "{self.creator}", "{self.runtime}", "{self.years}", "{self.seasons}")"""
        return _insert_command


@dataclass
class ImdbMovie(Imdb):
    director: str
    runtime: str

    def insertion_command(self):
        _insert_command = f"""INSERT INTO movie_details VALUES 
                            ("{self.imdb_id}", "{self.title}", "{self.original_title}", "{self.score}", 
                            "{self.voters}", "{self.plot}", "{self.poster}", "{self.rated}", 
                            "{self.genre}", "{self.media_type}", "{self.release_date}", "{self.countries}", 
                            "{self.actors}", "{self.director}", "{self.runtime}")"""
        return _insert_command
