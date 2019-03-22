import ijson
import os
import mysql.connector
import zipfile

def add_data_from_json_file(jsonfile):

    host_env_name = "{}_MYSQL_SERVICE_HOST".format(os.environ["MYSQL_SERVICE_PREFIX"].upper().replace("-", "_"))
    db_conn_data = {
      "user": os.environ["MYSQL_SERVICE_USER"],
      "password": os.environ["MYSQL_SERVICE_PASSWORD"],
      "host": os.environ[host_env_name],
      "database": os.environ["MYSQL_SERVICE_PERSONA_DB"]
    }

    print(db_conn_data)
  
    db = mysql.connector.connect(**db_conn_data)
    cursor = db.cursor()
    

    cursor.execute("""
      CREATE TABLE users(id INTEGER PRIMARY KEY, job TEXT, company TEXT, ssn TEXT,
                         residence TEXT, longditude REAL, latitude REAL, 
                         blood_group TEXT, username TEXT, name TEXT, sex TEXT,
                         address TEXT, mail TEXT, birthdate TEXT)
    """)

    cursor.execute("""
      CREATE TABLE websites(id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER, website TEXT)
    """)
    
    db.commit()

    print("Importing JSON data.")
    
    with open(jsonfile) as jf:
      counter = 0
      """
        Use ijson to iterate through the json data - the file is fairly big and using the
        default json module could cause memory issues. ijson is an iterative parser that
        allows us to stream in the file.
      """
      for item in ijson.items(jf, 'item'):
        """
          Explicitly set the id so we can add items to the websites database with the
          same counter as the user_id.
        """
        # print("Inserting item {}".format(counter))
        counter += 1
        if counter > 1000:
          break
        cursor.execute("""
          INSERT INTO users(id, job, company, ssn, residence, longditude, latitude, 
                            blood_group, username, name, sex, address, mail, birthdate)

                          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

        """, (counter, item["job"], item["company"], item["ssn"], item["residence"], 
              float(item["current_location"][0]), float(item["current_location"][1]), 
              item["blood_group"], item["username"], item["name"], 
              item["sex"], item["address"], item["mail"],item["birthdate"]))

        for website in item["website"]:
          cursor.execute("""
            INSERT INTO websites(user_id, website) 
            VALUES(%s, %s)
          """, (counter, website))
    
    db.commit()

    print("Finished importing JSON.")

def unzip_json_file(filepath):
	zipper = zipfile.ZipFile(filepath)
	json_path = os.path.dirname(os.path.realpath(__file__))
	extracted_filename = zipper.namelist()[0]
	zipper.extractall(json_path)
	zipper.close()
	return os.path.join(json_path, extracted_filename)

add_data_from_json_file(unzip_json_file("./fake_profiles.zip"))