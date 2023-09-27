from dataclasses import dataclass

@dataclass
class Postgres:
    host: str
    port: str
    database: str
    user: str
    password: str


