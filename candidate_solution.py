# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException, Path
from typing import List, Optional
import uvicorn
import httpx
import asyncio
from difflib import get_close_matches


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="app.log",   # Writes to file
    filemode="a"          # Append mode
)

logger = logging.getLogger("pokemon_api")

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

# This function Retrieves all of the  data for any  pokemon that  Exists 
def get_pokemon_data(pokemon_name:str):
    
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}"
   
    try:
        r = httpx.get(url, timeout=10)
        r.raise_for_status()
    except httpx.HTTPStatusError:
        raise ValueError(f"Pokemon '{pokemon_name}' not found")
    except httpx.RequestError as e:
        raise ConnectionError(f"Network error: {e}")

    data = r.json()

    pokemon_data = {
        "name": data["name"],
        # types in order (primary, secondary)
        "types": [
            t["type"]["name"]
            for t in sorted(data["types"], key=lambda x: x["slot"])
        ],

        # abilities
        "abilities": [
            {
                "name": a["ability"]["name"],
                "is_hidden": a["is_hidden"]
            }
            for a in data["abilities"]
        ]
    }

    return pokemon_data


# getting all Pokemon names 
def get_pokemon_names():
    global pokemon_pokemon

    url = f"https://pokeapi.co/api/v2/pokemon?limit=500"
    
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_pokemon = [p["name"].title() for p in data["results"]]

    return
 
# getting all pokemon types
def get_pokemon_types():
    global pokemon_types

    url = f"https://pokeapi.co/api/v2/type?limit=500"
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_types = [t["name"].title() for t in data["results"]]

    return 

# getting all pokemon abilities 
def get_pokemon_abilities():
    global pokemon_abilities

    url = f"https://pokeapi.co/api/v2/ability?limit=1000"
    r = httpx.get(url)
    r.raise_for_status()

    data = r.json()
    pokemon_abilities = [a["name"].title() for a in data["results"]]

    return 

# Delete Duplicates
def delete_duplicates(cursor,table_name ,conn: sqlite3.Connection):
    
    print("Start Deleting Duplicates ", table_name)
    allowed_tables = {"pokemon", "abilities", "types","trainers"}
    
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name")
        return False

    # Pokemon Deletes
    if table_name == "pokemon" :
        sql = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MIN(id)
			FROM {table_name}
			WHERE id in (
				SELECT pokemon_id FROM 
				trainer_pokemon_abilities
			)
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
        
    # Abilities deletes
    if table_name == "abilities" :
       sql = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MIN(id)
			FROM {table_name}
			WHERE id in (
				SELECT ability_id FROM 
				trainer_pokemon_abilities
			)
		GROUP BY LOWER(name)
        )
        """

    # Trainers deletion
    if table_name == "trainers" :
       sql = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MIN(id)
			FROM {table_name}
			WHERE id in (
				SELECT trainer_id FROM 
				trainer_pokemon_abilities
			)
		GROUP BY LOWER(name)
        )
        """
        
    # Fixing types mapping    
    if table_name == "types" :
        
         #We need to  first  fix the  mapping  for types 
        #before we can delete Duplicates
        sql = f"""
            SELECT d.id AS duplicate_id, m.min_id
                FROM {table_name} d
                JOIN (
                    SELECT LOWER(name) AS lname, MIN(id) AS min_id
                    FROM {table_name}
                    GROUP BY LOWER(name)
                ) m
                ON LOWER(d.name) = m.lname
                WHERE d.id != m.min_id;
            """
        
        try:
            cursor.execute(sql)
            type_ids = cursor.fetchall()
            for duplicate_id, min_id in type_ids:    
                sql = """
                UPDATE pokemon
                SET
                    type1_id = CASE WHEN type1_id = ? THEN ? ELSE type1_id END,
                    type2_id = CASE WHEN type2_id = ? THEN ? ELSE type2_id END
                """
                cursor.execute(sql, (duplicate_id, min_id, duplicate_id, min_id))
                conn.commit()

        except sqlite3.Error as e:
            print(f"An error occurred during database cleaning {table_name}: {e}")
            conn.rollback()
            return False
        
        #now  we Can Delete Duplicates
        sql = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM {table_name}
            GROUP BY LOWER(name)
        )
        """
    # And  now we can Delete the Dullicates from the tables 
    
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
        name.title(),
        pokemon_list,
        n=1,          # max suggestions
        cutoff=0.6    # similarity threshold
    )

# Correct Spelling 
#  there is no official  list   for the Correct spelling of trainers 
#  so  we cannot fix  trainer names 
def correct_spelling(cursor ,table_name ,conn: sqlite3.Connection):

    print("Start  correct_spelling ", table_name )
  
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

            #retrieving  Supposed  Correct  Spelling 
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

# Standardise the case   for  the names in the tables 
# All names Should  be Title case 
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
    # OR name like '%Remove%'   only because  Abilites has a Remove this ability Record
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
        # Retrieving pokemon data used  for Cleaning data 

        get_pokemon_names()  
        get_pokemon_types()
        get_pokemon_abilities()
    
        # --- Implement Here ---
        db_tables = ["pokemon","types","abilities","trainers"]
        for db_table in db_tables:
            
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
            if not delete_duplicates(cursor, db_table, conn):
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
        logger.info(f"ROOT")
        return {"message": "Pokemon Assessment API - Basic"}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon ability name (Titlecase, hyphen-separated)"
    )):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---

        try: 
            
            ability_name = ability_name.title()
            conn = connect_db()
            if conn:
                cursor = conn.cursor()
                sql = """
                SELECT  pk.name  FROM pokemon pk 
                    inner join trainer_pokemon_abilities tpa on pk.id = tpa.pokemon_id 
                    inner join abilities ab on tpa.ability_id = ab.id
                    where ab.name =  ? """
            
                cursor.execute(sql, (ability_name,))
                rows = cursor.fetchall()

                if not rows:
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Pokémon found with ability '{ability_name}' found "
                    )
            else : 
                raise HTTPException(status_code=500, detail="Database connection failed")  
                
            conn.close()
            return [row[0] for row in rows]
        
        except sqlite3.Error as e:
            raise HTTPException(
                    status_code=500,
                    detail=f"Some Unforseen  Error occured please Contact your administrator"
                )
        
        
        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon Type name (Titlecase, hyphen-separated)"
    )):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        try: 
            
            type_name = type_name.title()
            conn = connect_db()

            if conn:
                cursor = conn.cursor()

                sql = """
                    SELECT pk.name
                    FROM pokemon pk
                    LEFT JOIN types t1 ON pk.type1_id = t1.id
                    LEFT JOIN types t2 ON pk.type2_id = t2.id
                    WHERE t1.name = ? OR t2.name = ?
                    """
                cursor.execute(sql, (type_name, type_name))
           
                rows = cursor.fetchall()

                if not rows:
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Pokémon found with type '{type_name}' found "
                    )
            else : 
                raise HTTPException(status_code=500, detail="Database connection failed") 
             
            conn.close()
            return [row[0] for row in rows]
        
        except sqlite3.Error as e:
            raise HTTPException(
                    status_code=500,
                    detail=f"Some Unforseen Error occured please contact your administrator"
                )
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon name (Titlecase, hyphen-separated)"
    )):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---

        try: 
            
            pokemon_name = pokemon_name.title()
            conn = connect_db()
            
            if conn:
                cursor = conn.cursor()

                sql = """
                SELECT  tr.name  FROM trainers tr 
                    inner join trainer_pokemon_abilities tpa on tr.id = tpa.trainer_id
                    inner join pokemon pk on pk.id = tpa.pokemon_id  
                    where pk.name =  ?
                     limit 1
                       """
            
                cursor.execute(sql, (pokemon_name,))

                rows = cursor.fetchall()

                if not rows:
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Trainer names found for pokemon named '{pokemon_name}' found "
                    )
            else : 
                raise HTTPException(status_code=500, detail="Database connection failed")  
                
            conn.close()
            return [row[0] for row in rows]
        
        except sqlite3.Error as e:
            raise HTTPException(
                    status_code=500,
                    detail=f"Some Unforseen Error occured please contact your administrator"
                )
        # --- End Implementation ---

  
        # --- Implement here ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon name (Titlecase, hyphen-separated)"
    )):
        
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        try: 
            
            pokemon_name = pokemon_name.title()
            conn = connect_db()

            if conn:
                cursor = conn.cursor()

                sql = """
                SELECT  ab.name  FROM abilities ab 
                    inner join trainer_pokemon_abilities tpa on ab.id = tpa.ability_id
                    inner join pokemon pk on pk.id = tpa.pokemon_id  
                    where pk.name =  ? """
            
                cursor.execute(sql, (pokemon_name,))

                rows = cursor.fetchall()

                if not rows:
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Abilities found for pokemon named '{pokemon_name}' found "
                    )
                
            conn.close()
            return [row[0] for row in rows]
        
        except sqlite3.Error as e:
            raise HTTPException(
                    status_code=500,
                    detail=f"Some Unforseen  Error occured please Contact your administrator"
                )
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    
    @app.post("/pokemon/{pokemon_name}/trainer/{trainer_name}")
    def add_pokemon(pokemon_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon name (Titlecase, hyphen-separated)"
    ), trainer_name: str = Path(
        ...,
        min_length=1,
        max_length=30,
        pattern="^[A-Za-z-]+$",
        description="Pokemon name (Titlecase, hyphen-separated)"
    )):
        
        try:     
            pokemon_name = pokemon_name.title()
            trainer_name = trainer_name.title() 
            
            conn = connect_db()

            if conn:
                logger.info(f"Adding POKKEMON ")
                cursor = conn.cursor()
                conn.execute("BEGIN")
                sql = """ SELECT id FROM pokemon WHERE name = ?"""
                cursor.execute(sql, (pokemon_name,))

                existing = cursor.fetchone()

                if existing:
                    raise HTTPException(
                        status_code=409,
                        detail="Pokemon already exists"
                    )
               
                try:
                    pokemon_data = get_pokemon_data(pokemon_name.lower())
                except ValueError:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No Pokémon named '{pokemon_name}' found"
                    )
            
                typelist=[]
                abilitylist=[]
                trainer_id = 0 
                pokemon_id = 0 

                #Checking  if types  exist if not  add types
                for  pokemon_types in pokemon_data["types"]:
                    sql = """
                        SELECT  tp.id , tp.name  FROM types tp
                        where tp.name =  ? """
                    cursor.execute(sql, (pokemon_types.title(),))
                    pokemon_type = cursor.fetchone()

                    if pokemon_type:
                        typelist.append(pokemon_type[0])
                    else : 
                        sql = """
                        INSERT INTO types (name) VALUES (?)
                        """
                        cursor.execute(sql, (pokemon_types.title(),))
                        typelist.append(cursor.lastrowid) 
            
                #Checking if abilities  exist if not  add ability
                for ability in pokemon_data["abilities"]:
                    
                    sql = """
                        SELECT  ab.id , ab.name  FROM abilities ab
                        where ab.name =  ? """
                    cursor.execute(sql, (ability['name'].title(),))
                    pokemon_ability = cursor.fetchone()

                    if pokemon_ability:
                        abilitylist.append(pokemon_ability[0])
                    else : 
                        sql = """
                        INSERT INTO abilities (name) VALUES (?)
                        """
                        cursor.execute(sql, (ability['name'].title(),))
                        abilitylist.append(cursor.lastrowid) 
                
                #check  if trainer exist if not add trainer
                sql = """
                        SELECT  tr.id , tr.name  FROM trainers tr
                        where tr.name =  ? """
                cursor.execute(sql, (trainer_name,))
                trainer = cursor.fetchone()

                if trainer:
                    trainer_id = trainer[0]
                else : 
                    sql = """
                    INSERT INTO trainers (name) VALUES (?)
                    """
                    cursor.execute(sql, (trainer_name,))
                    trainer_id = cursor.lastrowid
                
                # now we can Add the pokemon 
                type1_id = typelist[0] if len(typelist) > 0 else None
                type2_id = typelist[1] if len(typelist) > 1 else None
                
                sql = """
                    INSERT INTO pokemon (name , type1_id , type2_id ) VALUES (?,?,?)
                    """
                cursor.execute(sql, (pokemon_name,type1_id,type2_id ))
                pokemon_id = cursor.lastrowid
            
                #inserting trainer_pokemon_abilities
                for abilityID in abilitylist:
                    sql = """
                    INSERT OR IGNORE INTO trainer_pokemon_abilities 
                    (pokemon_id , trainer_id , ability_id ) VALUES (?,?,?)
                    """
                    cursor.execute(sql, (pokemon_id,trainer_id,abilityID))

            
            conn.commit()          
            return {"message": "Successfully added"}
        except HTTPException:
            conn.rollback()
            raise
        except sqlite3.Error as e:
            conn.rollback()
            raise HTTPException(
                status_code=500,
                detail=f"Some Unforseen  Error occured please Contact your administrator"
            )
        finally:
            if conn:
                conn.close()


        
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
        
    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
