# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException, Path
from typing import List, Optional
import uvicorn
import httpx
import asyncio
from difflib import get_close_matches

# --- Constants ---
DB_NAME = "pokemon_assessment.db"
pokemon_pokemon     = [] 
pokemon_types       = []
pokemon_abilities   = []



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

# getting all Pokemon names 
def get_pokemon_names():
    global pokemon_pokemon

    url = f"https://pokeapi.co/api/v2/pokemon?limit=2000"
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_pokemon = [p["name"] for p in data["results"]]

    return
 
# getting all pokemon types
def get_pokemon_types():
    global pokemon_types

    url = f"https://pokeapi.co/api/v2/type/"
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_types = [t["name"] for t in data["results"]]

    return 

# getting all pokemon abilities 
def get_pokemon_abilities():
    global pokemon_abilities

    url = f"https://pokeapi.co/api/v2/ability?limit=500"
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_abilities = [a["name"] for a in data["results"]]

    return 

# Delete Duplicates
def delete_dublicates(cursor,table_name ,conn: sqlite3.Connection):
    
    print("Start Deleting Duplicates ", table_name)
    allowed_tables = {"pokemon", "abilities", "types","trainers"}
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name")
        return False

    if table_name == "pokemon" :
        # if  pokemon table whe need to  Delete  the  record   from 
        # trainer_pokemon_abilities 
        sql = """DELETE FROM trainer_pokemon_abilities
                WHERE pokemon_id NOT IN (
                    SELECT MIN(id)
                        FROM pokemon
                    GROUP BY LOWER(name)
            )"""
        
        try:
            cursor.execute(sql)
            conn.commit() 
        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning {table_name}: {e}")
            conn.rollback()
            return False

    sql = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM {table_name}
            GROUP BY LOWER(name)
        )
        """
    try:
        cursor.execute(sql)
        conn.commit() 
    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning {table_name}: {e}")
        conn.rollback()
        return False

    print("End Deleting Duplicates")
    return True 
    
# get Spelling Suggestion
def get_spelling_suggestion(name: str , pokemon_list):
    
    return get_close_matches(
        name.lower(),
        pokemon_list,
        n=1,          # max suggestions
        cutoff=0.6    # similarity threshold
    )

# Correct Spelling 
def correct_spelling(cursor ,table_name ,conn: sqlite3.Connection):

    print("Start  correct_spelling ", table_name )
   #  there is no official  list   for the Correct spelling of trainers 
   #  so  we cannot fix  trainer names 
    allowed_tables = {"pokemon", "abilities", "types"}
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name")
        return False
   
    sql = f"""
        SELECT id , name  FROM {table_name}
        """
    try:
        cursor.execute(sql)
        names_ids = cursor.fetchall()
        for name_id in names_ids:
            
            id_value = name_id[0]  
            name_value = name_id[1]

            list_name = "pokemon_"+table_name    
            pokemon_name = get_spelling_suggestion(name_value , globals()[list_name])
            
            if pokemon_name:
                pokemon_name = pokemon_name[0]
            else : 
                pokemon_name = name_value

            # update the  Spelling of the names 
            if pokemon_name !=  name_value :
                sql_update =f"UPDATE {table_name} SET name = ? WHERE id = ?"
                cursor.execute(sql_update, (pokemon_name, id_value))
                conn.commit()

    except sqlite3.Error as e:
        print(f"An error occurred during Spelling Fix cleaning {table_name}: {e}")
        return False

    print("End  correct_misspellings ")
    return True   

def standardise_case(cursor,table_name ,conn: sqlite3.Connection): 
    print("Start Standardise Case  ",table_name )

    allowed_tables = {"pokemon", "abilities", "types","trainers"}
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name")
        return False
    
    sql_select = f"""
        SELECT id , name  FROM {table_name}
        """
    
    try: 
        cursor.execute(sql_select)
        names_ids = cursor.fetchall()
        for name_id in names_ids:
            id_value = name_id[0]    # id is the first column
            name_value = name_id[1] 
            titlecase = name_value.title()

            try: 
                #update table
                sql_update =f"UPDATE {table_name} SET name = ? WHERE id = ?"
                cursor.execute(sql_update, (titlecase, id_value))
                conn.commit()
            except sqlite3.Error as e:
                print(f"An error occurred Updating Case pokemons: {e}")
                return False
    except sqlite3.Error as e:
        print(f"An error occurred table pokemon Selection : {e}")
        return False

    print("End Standardise Case")
    return True   

# Remove Redundant Data 
def remove_redundant_data(cursor,table_name ,conn: sqlite3.Connection): 

    print("Start remove_redundant_data ",table_name )

    allowed_tables = {"pokemon", "abilities", "types","trainers"}
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name")
        return False
    

    sql = f"""
        DELETE FROM {table_name}
        WHERE TRIM(name) = ''
        OR TRIM(name) = '---'
        OR TRIM(name) = '???'
        OR name like '%Remove%'
        """
    
    try: 
        cursor.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred during remove Redundant data pokemon: {e}")
        conn.rollback()
        return False

    print("end remove_redundant_data ")
    return True

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

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---
        db_tables = ["pokemon","types","abilities","trainers"]
        for db_table in db_tables:
            
             # --- Correct Misspellings ---
             # need to Correct  Spelling before we standardise case and  remove
             # duplicates , but only  for Pokemons , types and Abilities 

            # --- Remove Redundant data ---    
            if not remove_redundant_data(cursor, db_table,conn):    
                return
            
            # --- Correct Spelling 
            if db_table != "trainers" :
                if not correct_spelling(cursor, db_table,conn) :
                    return
        
            # --- Standardize case ----

            if not standardise_case(cursor, db_table, conn):
                return
            
            # --- Removing Duplicates ---
            if not delete_dublicates(cursor, db_table, conn):
                return
        # --- End Implementation ---
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
        return {"message": "Pokemon Assessment API"}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        regex="^[a-z-]+$",
        description="Pokemon ability name (lowercase, hyphen-separated)"
    )):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
    
        conn = connect_db()
        if conn:
            cursor = conn.cursor()
            try: 
                
                ability_name = ability_name.title()

                sql = """
                SELECT  pk.name  FROM pokemon pk 
                    inner join trainer_pokemon_abilities tpa on pk.id = tpa.pokemon_id 
                    inner join abilities ab on tpa.ability_id = ab.id
                    where ab.name =  ? """
            
                cursor.execute(sql, (ability_name,))
                rows = cursor.fetchall()

                if not rows:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Pokémon found with ability '{ability_name}'"
                    )
                
                return [row[0] for row in rows]
            
            except sqlite3.Error as e:
                raise HTTPException(
                        status_code=404,
                        detail=f"No Pokémon found with ability '{ability_name}'"
                    )
                print(f"An error occurred during remove Redundant data pokemon: {e}")
            
            conn.close()
            
        else :
            print("DB Does Not Exist")

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
        get_pokemon_names()  
        get_pokemon_types()
        get_pokemon_abilities()

        clean_database(temp_conn)
        temp_conn.close()
        print("DB Connection Closed")
    else :
        print("DB Does Not Exist")
        
    app_instance = create_fastapi_app()
   # uvicorn.run(app_instance, host="127.0.0.1", port=8000)
