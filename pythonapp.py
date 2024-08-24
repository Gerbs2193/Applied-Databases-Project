from mysql.connector import connect, Error
import sys

# MySQL configuration
MYSQL_CONFIG = {
    'host': '10.0.0.60',
    'user': 'root',
    'password': 'root',
    'database': 'appdbproj'
}

def connect_mysql():
    """ Establish connection to MySQL database. """
    try:
        return connect(**MYSQL_CONFIG)
    except Error as e:
        print(f"Failed to connect to MySQL: {e}")
        sys.exit(1)

def view_cities_by_country():
    """ View cities by country with pagination. """
    country = input("Enter country code (e.g., 'USA', 'GBR'): ").strip()
    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CountryCode, Name, District, Population FROM city WHERE CountryCode = %s", (country,))
        cities = cursor.fetchall()
        if not cities:
            print("No cities found for this country code.")
            return
        for i, city in enumerate(cities, start=1):
            print(f"{i}. City: {city[1]}, District: {city[2]}, Population: {city[3]}")
            if i % 2 == 0:  # Display 2 records at a time
                input("Press any key to see more cities...")
    except Error as e:
        print(f"Error retrieving cities: {e}")
    finally:
        cursor.close()
        connection.close()

def update_city_population():
    """ Update the population of a specific city. """
    city_id = input("Enter City ID: ").strip()
    connection = connect_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT Name, Population FROM city WHERE ID = %s", (city_id,))
        city = cursor.fetchone()
        if not city:
            print("City ID not found.")
            return
        print(f"Current Population of {city[0]}: {city[1]}")
        new_population = int(input("Enter the new population: "))
        cursor.execute("UPDATE city SET Population = %s WHERE ID = %s", (new_population, city_id))
        connection.commit()
        print("Population updated successfully.")
    except Error as e:
        print(f"Error updating city population: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    while True:
        print("\n1. View Cities by Country")
        print("2. Update City Population")
        print("x. Exit Application")
        choice = input("Choose an option: ").strip()
        if choice == '1':
            view_cities_by_country()
        elif choice == '2':
            update_city_population()
        elif choice.lower() == 'x':
            print("Exiting application...")
            break
        else:
            print("Invalid option, please try again.")

if __name__ == "__main__":
    main()

