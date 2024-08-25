from mysql.connector import connect as mysql_connect, Error as MySQLError
from neo4j import GraphDatabase
import sys

# MySQL setup  - this is where all the details are for connecting to the MySQL database
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'appdbproj'
}

# Neo4j setup - similiar to MySQL, but for Neo4j
NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'Parachromis2193'
}

# Connect to MySQL - Ensuring we're connected, error if not
def connect_mysql():
    try:
        connection = mysql_connect(**MYSQL_CONFIG)
        print("Woohoo! Connected to MySQL database.")  # yay, it worked!
        return connection
    except MySQLError as e:
        print(f"Oops! Failed to connect to MySQL: {e}")
        sys.exit(1)

# Same as above, but for neo
def connect_neo4j():
    try:
        driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
        )
        print("Neo4j says hello! Connection successful.")  # Neo4j is ready to go
        return driver
    except Exception as e:
        print(f"Oh no! Couldn't connect to Neo4j: {e}")
        sys.exit(1)

# MySQL: See Cities by Country
def view_cities_by_country():
    country_name = input("What's the country name (or part of it)? ").strip()
    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        query = """
        SELECT c.Name, city.Name, city.District, city.Population
        FROM city
        JOIN country c ON city.CountryCode = c.Code
        WHERE c.Name LIKE %s
        """
        cursor.execute(query, (f"%{country_name}%",))
        cities = cursor.fetchall()
        if not cities:
            print("No cities here... did you spell it right?")
            return
        for i, city in enumerate(cities, start=1):
            print(f"{i}. Country: {city[0]}, City: {city[1]}, District: {city[2]}, Population: {city[3]}")
            if i % 2 == 0:
                user_input = input("Press any key to see more cities, or 'q' to quit... ")
                if user_input.lower() == 'q':
                    break
    except MySQLError as e:
        print(f"Error fetching cities: {e}")
    finally:
        cursor.close()
        connection.close()

# MySQL: Change City Population - update a city's pop
def update_city_population():
    city_id = input("City ID, please: ").strip()
    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT Name, Population FROM city WHERE ID = %s", (city_id,))
        city = cursor.fetchone()
        if not city:
            print("Hmm, couldn't find that City ID.")
            return

        print(f"Current Population of {city[0]}: {city[1]}")
        action = input("Increase (I) or Decrease (D) the population? ").strip().lower()
        if action not in ['i', 'd']:
            print("Invalid choice, sorry.")
            return

        change = int(input("How much should we change it by? "))
        new_population = city[1] + change if action == 'i' else city[1] - change
        cursor.execute("UPDATE city SET Population = %s WHERE ID = %s", (new_population, city_id))
        connection.commit()
        print("Population updated. Easy peasy.")
    except MySQLError as e:
        print(f"Error updating population: {e}")
    finally:
        cursor.close()
        connection.close()

# MySQL: List Countries by Population - see countries based on how many people live there
def view_countries_by_population():
    operator = input("Comparison operator (<, >, =): ").strip()
    if operator not in ['<', '>', '=']:
        print("That's not a valid operator!")
        return
    try:
        population = int(input("Population to compare with: "))
    except ValueError:
        print("Oops, that's not a number.")
        return

    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        query = f"""
        SELECT Code, Name, Continent, Population
        FROM country
        WHERE Population {operator} %s
        """
        cursor.execute(query, (population,))
        countries = cursor.fetchall()
        if not countries:
            print(f"No countries with population {operator} {population}. Try again maybe?")
            return
        for i, country in enumerate(countries, start=1):
            print(f"{i}. Code: {country[0]}, Name: {country[1]}, Continent: {country[2]}, Population: {country[3]}")
    except MySQLError as e:
        print(f"Error fetching countries: {e}")
    finally:
        cursor.close()
        connection.close()

# MySQL: Add New Country and City - add brand new country and its cities
def add_new_country():
    connection = connect_mysql()
    cursor = connection.cursor()

    try:
        country_code = input("New country code (3 letters): ").strip().upper()
        country_name = input("New country name: ").strip()
        continent = input("Which continent? ").strip()
        population = int(input("Population of this country: "))

        query = """
        INSERT INTO country (Code, Name, Continent, Population)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (country_code, country_name, continent, population))
        connection.commit()
        print(f"Country '{country_name}' added successfully!")

        add_new_city(country_code)

    except MySQLError as e:
        print(f"Error adding new country: {e}")
    finally:
        cursor.close()
        connection.close()

def add_new_city(country_code):
    connection = connect_mysql()
    cursor = connection.cursor()

    try:
        while True:
            city_name = input("New city name: ").strip()
            district = input("District: ").strip()
            population = int(input("Population of the city: "))

            query = """
            INSERT INTO city (Name, CountryCode, District, Population)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (city_name, country_code, district, population))
            connection.commit()
            print(f"City '{city_name}' added successfully!")

            more_cities = input("Add another city for this country? (y/n): ").strip().lower()
            if more_cities != 'y':
                break

    except MySQLError as e:
        print(f"Error adding new city: {e}")
    finally:
        cursor.close()
        connection.close()

# MySQL: Add New Person - add a person and make sure ther city exists
def add_new_person():
    connection = connect_mysql()
    cursor = connection.cursor()

    try:
        person_id = input("New person ID: ").strip()
        person_name = input("Person's name: ").strip()
        person_age = int(input("Person's age: ").strip())
        person_salary = float(input("Person's salary: ").strip())
        city_id = input("City ID where the person lives: ").strip()

        # Check if the city exists else error message
        cursor.execute("SELECT Name FROM city WHERE ID = %s", (city_id,))
        city = cursor.fetchone()
        if not city:
            print("City ID does not exist. Can't add the person.")
            return

        query = """
        INSERT INTO person (ID, Name, Age, Salary, CityID)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (person_id, person_name, person_age, person_salary, city_id))
        connection.commit()
        print(f"Person '{person_name}' added successfully to the city '{city[0]}'.")
    except MySQLError as e:
        print(f"Error adding person: {e}")
    finally:
        cursor.close()
        connection.close()

# MySQL: Delete Person - remove a person, but only if they haven't been visiting cities
def delete_person():
    connection = connect_mysql()
    cursor = connection.cursor()

    try:
        person_id = input("ID of the person to delete: ").strip()

        cursor.execute("SELECT COUNT(*) FROM visits WHERE PersonID = %s", (person_id,))
        visits_count = cursor.fetchone()[0]
        if visits_count > 0:
            print("This person has visited cities, can't delete them!")
            return

        cursor.execute("DELETE FROM person WHERE ID = %s", (person_id,))
        connection.commit()
        print(f"Person with ID '{person_id}' deleted successfully.")
    except MySQLError as e:
        print(f"Error deleting person: {e}")
    finally:
        cursor.close()
        connection.close()

# Neo4j: Show Twinned Cities - see which cities are twinned with each other
def show_twinned_cities():
    driver = connect_neo4j()
    with driver.session() as session:
        result = session.run(
            "MATCH (c:City)-[:TWINNED_WITH]->(t:City) RETURN c.name AS City1, t.name AS City2 ORDER BY c.name")
        print("Twinned Cities:")
        for record in result:
            print(f"{record['City1']} <-> {record['City2']}")
    driver.close()

# Neo4j: Display Twin Count for Cities - see how many cities each city is twinned with
def display_twin_counts():
    driver = connect_neo4j()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:City)-[:TWINNED_WITH]->()
            RETURN c.name AS City, COUNT(*) AS TwinCount
            ORDER BY TwinCount DESC, City ASC
        """)
        print("City Twin Counts:")
        for record in result:
            print(f"{record['City']}: {record['TwinCount']} twins")
    driver.close()

def get_city_name_from_mysql(city_id):
    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT Name FROM city WHERE ID = %s", (city_id,))
        city = cursor.fetchone()
        if city:
            return city[0]
        else:
            return None
    except MySQLError as e:
        print(f"Error getting city name: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

# Neo4j: Twin with Dublin - make a city twinned with Dublin
def twin_with_dublin():
    city_id = input("City ID to twin with Dublin: ").strip()
    driver = connect_neo4j()
    with driver.session() as session:
        city_name = get_city_name_from_mysql(city_id)

        if not city_name:
            print("City doesn't exist in MySQL, so we can't twin it.")
            return

        session.run("""
            MERGE (c:City {id: $city_id, name: $city_name})
            MERGE (d:Dublin {name: 'Dublin'})
            MERGE (c)-[:TWINNED_WITH]->(d)
        """, city_id=city_id, city_name=city_name)

        print(f"City '{city_name}' with ID {city_id} twinned with Dublin.")
    driver.close()

# MySQL: Search for Cities by Population Range - filter cities based on population
def search_cities_by_population_range():
    min_population = int(input("Enter the minimum population: ").strip())
    max_population = int(input("Enter the maximum population: ").strip())

    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        query = """
        SELECT Name, CountryCode, District, Population
        FROM city
        WHERE Population BETWEEN %s AND %s
        ORDER BY Population ASC
        """
        cursor.execute(query, (min_population, max_population))
        cities = cursor.fetchall()
        if not cities:
            print(f"No cities found with population between {min_population} and {max_population}.")
            return
        for i, city in enumerate(cities, start=1):
            print(f"{i}. City: {city[0]}, Country: {city[1]}, District: {city[2]}, Population: {city[3]}")
    except MySQLError as e:
        print(f"Error retrieving cities by population range: {e}")
    finally:
        cursor.close()
        connection.close()

# Main Menu nav
def main():
    while True:
        print("\n1. View Cities by Country (MySQL)")
        print("2. Update City Population (MySQL)")
        print("3. View Countries by Population (MySQL)")
        print("4. Add New Country and City (MySQL)")
        print("5. Add New Person (MySQL)")
        print("6. Delete Person (MySQL)")
        print("7. Show Twinned Cities (Neo4j)")
        print("8. Twin with Dublin (Neo4j)")
        print("9. Display City Twin Counts (Neo4j)")
        print("10. Search Cities by Population Range (MySQL)")
        print("x. Exit Application")
        choice = input("Choose an option: ").strip()
        if choice == '1':
            view_cities_by_country()
        elif choice == '2':
            update_city_population()
        elif choice == '3':
            view_countries_by_population()
        elif choice == '4':
            add_new_country()
        elif choice == '5':
            add_new_person()
        elif choice == '6':
            delete_person()
        elif choice == '7':
            show_twinned_cities()
        elif choice == '8':
            twin_with_dublin()
        elif choice == '9':
            display_twin_counts()
        elif choice == '10':
            search_cities_by_population_range()
        elif choice.lower() == 'x':
            print("Exiting application...")
            break
        else:
            print("Invalid option, please try again.")

if __name__ == "__main__":
    main()
