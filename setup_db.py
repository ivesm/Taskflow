import sqlite3
import os

DB_NAME = "pokemon_assessment.db"


def setup_database():
    """Creates and populates the SQLite database with dirty data."""
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)  # Start fresh each time

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # --- Create Tables ---
    cursor.execute("""
    CREATE TABLE types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL COLLATE NOCASE
    );
    """)
    cursor.execute("""
    CREATE TABLE abilities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL COLLATE NOCASE
    );
    """)
    cursor.execute("""
    CREATE TABLE trainers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE pokemon (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        type1_id INTEGER,
        type2_id INTEGER,
        FOREIGN KEY (type1_id) REFERENCES types(id),
        FOREIGN KEY (type2_id) REFERENCES types(id)
    );
    """)
    cursor.execute("""
    CREATE TABLE trainer_pokemon_abilities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pokemon_id INTEGER NOT NULL,
        trainer_id INTEGER NOT NULL,
        ability_id INTEGER NOT NULL,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon(id),
        FOREIGN KEY (trainer_id) REFERENCES trainers(id),
        FOREIGN KEY (ability_id) REFERENCES abilities(id)
    );
    """)

    # --- Populate with Dirty Data ---
    # Types (duplicates, inconsistent casing, misspellings)
    types_data = [
        ('Normal',),
        ('Grass',),
        ('Electric',),
        ('Ice',),
        ('Fighting',),
        ('Poison',),
        ('Ground',),
        ('Flying',),
        ('Psychic',),
        ('Bug',),
        ('Rock',),
        ('Ghost',),
        ('Dragon',),
        ('Dark',),
        ('Steel',),
        ('Fairy',),
        # Duplicates and misspellings
        ('???',),
        ('fire',),
        ('WATER',),
        ('---',),
        ('',),
        ('gras',),
        ('Poision',),
        ('Normal',)
    ]
    cursor.executemany("INSERT INTO types (name) VALUES (?)", types_data)
    cursor.execute("SELECT id, name FROM types")
    type_map = {name.lower(): id for id, name in cursor.fetchall()}

    # Abilities (duplicates, inconsistent casing)
    abilities_data = [
        ('Overgrow',),
        ('Blaze',),
        ('Chlorophyll',),
        ('Intimidate',),
        ('Keen Eye',),
        ('Run Away',),
        ('Guts',),
        ('Rock Head',),
        ('Sturdy',),
        ('Tangled Feet',),
        # Duplicates and misspellings
        ('static',),
        ('overgrow',),
        ('Torrent',),
        ('Remove this ability',)
    ]
    cursor.executemany("INSERT INTO abilities (name) VALUES (?)", abilities_data)
    cursor.execute("SELECT id, name FROM abilities")
    ability_map = {name.lower(): id for id, name in cursor.fetchall()}

    # Trainers (duplicates)
    trainers_data = [('Ash Ketchum',),
                     ('Brock',),
                     ('Gary Oak',),
                     ('Professor Oak',),
                     # Duplicates and misspellings
                     ('Ash Ketchum',),
                     ('misty',)]
    cursor.executemany("INSERT INTO trainers (name) VALUES (?)", trainers_data)
    cursor.execute("SELECT id, name FROM trainers")
    trainer_map = {name.lower(): id for id, name in cursor.fetchall()}

    # Pokemon (duplicates, misspellings)
    pokemon_data = [
        ('Bulbasaur', 'Grass', 'Poison'),
        ('Ivysaur', 'Grass', 'Poison'),
        ('Venusaur', 'Grass', 'Poison'),
        ('Charmeleon', 'Fire', None),
        ('Charizard', 'Fire', 'Flying'),
        ('Squirtle', 'Water', None),
        ('Wartortle', 'Water', None),
        ('Blastoise', 'Water', None),
        ('Pikachu', 'Electric', None),
        ('Raichu', 'Electric', None),
        ('Geodude', 'Rock', 'Ground'),
        ('Graveler', 'Rock', 'Ground'),
        ('Golem', 'Rock', 'Ground'),
        ('Pidgey', 'Normal', 'Flying'),
        ('Pidgeotto', 'Normal', 'Flying'),
        ('Pidgeot', 'Normal', 'Flying'),
        # Duplicates and misspellings
        ('Pikachu', 'Electric', None),
        ('Pikuchu', 'Electric', None),
        ('Charmanderr', 'Fire', None),
        ('Bulbasuar', 'Grass', 'Poison'),
        ('Geodude', 'Rock', 'Ground'),
        ('RATtata', 'Normal', None),
    ]
    for name, t1, t2 in pokemon_data:
        t1_id = type_map.get(t1.lower()) if t1 else None
        t2_id = type_map.get(t2.lower()) if t2 else None
        cursor.execute("INSERT INTO pokemon (name, type1_id, type2_id) VALUES (?, ?, ?)", (name, t1_id, t2_id))
    cursor.execute("SELECT id, name FROM pokemon")
    pokemon_map = {name.lower(): id for id, name in cursor.fetchall()}

    # Trainers Pokemon and Abilities
    pokemon_abilities_data = [
        ('Bulbasaur', 'Ash Ketchum', 'Overgrow'),
        ('Raichu', 'Ash Ketchum', 'Run Away'),
        ('Squirtle', 'Ash Ketchum', 'Torrent'),
        ('Pikachu', 'Ash Ketchum', 'Static'),
        ('Geodude', 'Brock', 'Rock Head'),
        ('Geodude', 'Brock', 'Sturdy'),
        ('Rattata', 'Brock', 'Guts'),
        ('Pidgey', 'Gary Oak', 'Keen Eye'),
        ('Pidgeotto', 'Gary Oak', 'Tangled Feet'),
        ('Rattata', 'Misty', 'Run Away'),
        ('Geodude', 'Misty', 'Sturdy'),
        ('Wartortle', 'Misty', 'Intimidate'),
        ('Pidgey', 'Misty', 'Intimidate'),
    ]
    for p_name, t_name, a_name in pokemon_abilities_data:
        p_id = pokemon_map.get(p_name.lower())
        t_id = trainer_map.get(t_name.lower())
        a_id = ability_map.get(a_name.lower())
        cursor.execute("INSERT INTO trainer_pokemon_abilities (pokemon_id, trainer_id, ability_id) VALUES (?, ?, ?)",(p_id, t_id, a_id))

    # --- Commit and Close ---
    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' created and populated with dirty data.")


if __name__ == "__main__":
    setup_database()
