# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import uvicorn
import httpx
import asyncio

# --- Constants ---
DB_NAME = "pokemon_assessment.db"
POKEMON_BASE_URL = "https://pokeapi.co/api/v2/"


def pokemon_api_connect(api_endpoint):
    name = ""


    

    return name

# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    connection = None
    try:
        # --- Implement Here ---
        
        connection = sqlite3.connect(DB_NAME) # My SQLLite Connection
        
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection


# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    return None
    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---

        # --- Removing Duplicates ---
        # Deleting Duplicate pokemon
        try: 
            cursor.execute("DELETE FROM pokemon "
                           "WHERE id NOT IN "
                           "( SELECT MIN(id) "
                           "FROM pokemon GROUP BY LOWER(name) ) "
                        )
        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning pokemon: {e}")
            conn.rollback()  # Roll back changes on error
            return

        # Deleting Duplicate  types 
        try: 
            cursor.execute("DELETE FROM types "
                           "WHERE id NOT IN "
                           "( SELECT MIN(id) "
                           "FROM types GROUP BY LOWER(name) ) "
                        )
        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning types: {e}")
            conn.rollback()  # Roll back changes on error
            return

        # Deleting Duplicate abilities
        try:
            cursor.execute("DELETE FROM abilities "
                           "WHERE id NOT IN "
                           "( SELECT MIN(id) "
                           "FROM abilities GROUP BY LOWER(name) ) "
                        )

        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning abilities: {e}")
            conn.rollback()  # Roll back changes on error
            return

        # Deleting Duplicate trainers
        try: 
            cursor.execute("DELETE FROM trainers "
                           "WHERE id NOT IN "
                           "( SELECT MIN(id) "
                           "FROM trainers GROUP BY LOWER(name) ) "
                        )

        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning trainers: {e}")
            conn.rollback()  # Roll back changes on error
            return
        # --- End Removing Duplicates ---

        # --- Correct Misspellings ---

        try: 
            cursor.execute("")

        except sqlite3.Error as e:
            print(f"An error occurred during MissSpelling Updates: {e}")
            conn.rollback()  # Roll back changes on error
            return
        # --- end Correct Misspellings ---

        # --- Standardize case ----

        #pokemon
        try: 

            #select pokemon
            cursor.execute("SELECT id , name FROM pokemon")
            pokemons = cursor.fetchall()
            for pokemon in pokemons:
                id_value = pokemon[0]    # id is the first column
                name_value = pokemon[1] 
                titlecase = name_value.title()
                try: 
                    #update pokemon
                    cursor.execute(
                    "UPDATE pokemon SET name = ? WHERE id = ?",
                    (titlecase, id_value)
                    )

                except sqlite3.Error as e:
                    print(f"An error occurred Updating Case pokemons: {e}")
                    conn.rollback()  # Roll back changes on error
        except sqlite3.Error as e:
            print(f"An error occurred table pokemon Selection : {e}")
            conn.rollback()  # Roll back changes on error
            return

        #types
        try: 
            #Select types
            cursor.execute("SELECT id , name FROM types")
            types = cursor.fetchall()
            for typep in types:
                id_value = typep[0]    # id is the first column
                name_value = typep[1] 
                titlecase = name_value.title()
                try: 
                    #update types
                    cursor.execute(
                    "UPDATE types SET name = ? WHERE id = ?",
                    (titlecase, id_value)
                    )

                except sqlite3.Error as e:
                    print(f"An error occurred Updating Case types: {e}")
                    conn.rollback()  # Roll back changes on error
                    return
        except sqlite3.Error as e:
            print(f"An error occurred table types Selection : {e}")
            conn.rollback()  # Roll back changes on error
            return

        #abilities
        try: 

            #select abilities
            cursor.execute("SELECT id , name FROM abilities")
            abilities = cursor.fetchall()
            for ability in abilities:
                id_value = ability[0]    # id is the first column
                name_value = ability[1] 
                titlecase = name_value.title()
                try: 
                    #update A
                    cursor.execute(
                    "UPDATE abilities SET name = ? WHERE id = ?",
                    (titlecase, id_value)
                    )

                except sqlite3.Error as e:
                    print(f"An error occurred Updating Case abilities: {e}")
                    conn.rollback()  # Roll back changes on error
                    return

        except sqlite3.Error as e:
            print(f"An error occurred table Abilities Selection : {e}")
            conn.rollback()  # Roll back changes on error
            return

        #trainers
        try: 

            #Update trainers
            cursor.execute("SELECT id , name FROM trainers")
            abilities = cursor.fetchall()
            for trainer in trainers:
                id_value = trainer[0]    # id is the first column
                name_value = trainer[1] 
                titlecase = name_value.title()
                try: 
                    cursor.execute(
                    "UPDATE trainers SET name = ? WHERE id = ?",
                    (titlecase, id_value)
                    )

                except sqlite3.Error as e:
                    print(f"An error occurred Updating Case trainers: {e}")
                    conn.rollback()  # Roll back changes on error
                    return

        except sqlite3.Error as e:
            print(f"An error occurred table trainers Selection : {e}")
            conn.rollback()  # Roll back changes on error
            return

        # --- End Standardize case ----
        # --- Remove Redundant data ---
        #pokemon
        try: 
            cursor.execute(
                "DELETE FROM pokemon "
                "WHERE TRIM(name) = '' "
                "OR TRIM(name) = '---' "
                "OR TRIM(name) = '???'"
            )
        except sqlite3.Error as e:
            print(f"An error occurred during remove Redundant data pokemon: {e}")
            conn.rollback()  # Roll back changes on error
            return


        #types
        try: 
            cursor.execute(
                "DELETE FROM types "
                "WHERE TRIM(name) = '' "
                "OR TRIM(name) = '---' "
                "OR TRIM(name) = '???'"
            )
        except sqlite3.Error as e:
            print(f"An error occurred during remove Redundant data types: {e}")
            conn.rollback()  # Roll back changes on error
            return


        #abilities
        try: 
            cursor.execute(
                "DELETE FROM abilities "
                "WHERE TRIM(name) = '' "
                "OR TRIM(name) = '---' "
                "OR TRIM(name) = '???'"
            )
        except sqlite3.Error as e:
            print(f"An error occurred during remove Redundant data abilities: {e}")
            conn.rollback()  # Roll back changes on error
            return

        #trainers
        try: 
            cursor.execute(
                "DELETE FROM trainers "
                "WHERE TRIM(name) = '' "
                "OR TRIM(name) = '---' "
                "OR TRIM(name) = '???'"
            )
        except sqlite3.Error as e:
            print(f"An error occurred during remove Redundant data trainers: {e}")
            conn.rollback()  # Roll back changes on error
            return


        # --- End Redundant data ---


        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error
        return

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---

        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---

        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---

        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---

        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---

        # --- End Implementation ---

    # --- Implement Task 8 here ---

    # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)

        temp_conn.close()
        print("DB Connection Closed")
    else :
        print("DB Does Not Exist")
        
   # app_instance = create_fastapi_app()
   # uvicorn.run(app_instance, host="127.0.0.1", port=8000)
