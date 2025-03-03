import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL Database configuration
DATABASE_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Headers to mimic a browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

# URC Teams & URLs
URC_TEAMS = {
    # "Cardiff Rugby": "https://www.rugbypass.com/teams/cardiff-blues/",
    # "Zebre": "https://www.rugbypass.com/teams/zebre/",
    # "Edinburgh": "https://www.rugbypass.com/teams/edinburgh/",
    # "Leinster": "https://www.rugbypass.com/teams/leinster/",
    # "Dragons RFC": "https://www.rugbypass.com/teams/gwent-dragons/",
    # "Ospreys": "https://www.rugbypass.com/teams/ospreys/",
    "Munster": "https://www.rugbypass.com/teams/munster/"#,
#     "Connacht": "https://www.rugbypass.com/teams/connacht/",
#     "Benetton": "https://www.rugbypass.com/teams/benetton/",
#     "Scarlets": "https://www.rugbypass.com/teams/scarlets/",
#     "Ulster": "https://www.rugbypass.com/teams/ulster/",
#     "Glasgow Warriors": "https://www.rugbypass.com/teams/glasgow/",
#     "Lions": "https://www.rugbypass.com/teams/lions/",
#     "Bulls": "https://www.rugbypass.com/teams/bulls/",
#     "Sharks": "https://www.rugbypass.com/teams/sharks/",
#     "Stormers": "https://www.rugbypass.com/teams/stormers/",
 }

# Scrape Squad Details
def get_squad_details(team_url, team_name):
    """Scrapes squad details (name, position, image, profile link) from RugbyPass."""
    response = requests.get(team_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch page: {team_url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    
    players_data = []
    player_cards = soup.select(".player-item")

    for player in player_cards:
        name_tag = player.select_one(".title")
        position_tag = player.select_one(".position")
        img_tag = player.select_one(".photo img")
        profile_link_tag = player.select_one("a.link-box")

        name = name_tag.text.strip() if name_tag else "Unknown"
        position = position_tag.text.strip() if position_tag else "Unknown"
        image_url = img_tag["src"] if img_tag else "No Image"
        profile_url = profile_link_tag["href"] if profile_link_tag else "No Link"

        players_data.append({
            "Name": name,
            "Position": position,
            "Image URL": image_url,
            "Profile URL": profile_url,
            "Team": team_name  # ✅ Includes team name
        })

    return players_data

# Get Player Details
def get_player_details(profile_url):
    """Scrapes player profile details."""
    if profile_url == "No Link":
        return {"Nationality": "Unknown", "Age": "Unknown", "Height": "Unknown", "Weight": "Unknown"}

    response = requests.get(profile_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch player page: {profile_url}")
        return {"Nationality": "Unknown", "Age": "Unknown", "Height": "Unknown", "Weight": "Unknown"}

    soup = BeautifulSoup(response.text, "html.parser")
    
    details = {
        "Nationality": "Unknown",
        "Age": "Unknown",
        "Height": "Unknown",
        "Weight": "Unknown"
    }

    # Extract Nationality from flag alt tag
    nationality_img = soup.select_one(".player-details .detail img.flag")
    if nationality_img and nationality_img.has_attr("alt"):
        details["Nationality"] = nationality_img["alt"].strip()

    # Extract other details
    details_sections = soup.select(".player-details .detail")
    
    for section in details_sections:
        label = section.text.strip()

        if "Age" in label:
            age_tag = section.select_one("div:last-child")
            details["Age"] = age_tag.text.strip() if age_tag else "Unknown"

        if "Height" in label:
            height_tag = section.select_one("div:last-child")
            details["Height"] = height_tag.text.strip() if height_tag else "Unknown"

        if "Weight" in label:
            weight_tag = section.select_one("div:last-child")
            details["Weight"] = weight_tag.text.strip() if weight_tag else "Unknown"

    return details

# Insert into PostgreSQL
def insert_rugby_stats(df):
    """Insert rugby player stats into PostgreSQL"""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur = conn.cursor()

        insert_query = """
        INSERT INTO ingestion.landing_rugby_urc_player_details (
            player_name, position, team, nationality, age, height, weight, image_url, profile_url
        ) VALUES %s
        """

        # Convert DataFrame to a list of tuples in the correct order
        values = [
            (
                row["Name"],         # player_name
                row["Position"],     # position
                row["Team"],         # team
                row["Nationality"],  # nationality
                row["Age"],          # age
                row["Height"],       # height
                row["Weight"],       # weight
                row["Image URL"],    # image_url
                row["Profile URL"]  # profile_url
            )
            for _, row in df.iterrows()
        ]

        execute_values(cur, insert_query, values)
        conn.commit()

        execute_values(cur, insert_query, values)
        conn.commit()

        print(f"Inserted {len(df)} records successfully.")

    except Exception as e:
        print(f"Error inserting data: {e}")

    finally:
        cur.close()
        conn.close()

# Main Function - Scrape URC Teams & Insert Data
def scrape_urc_teams():
    """Loops through all URC teams and scrapes their squads."""
    all_squad_data = []

    for team_name, team_url in URC_TEAMS.items():
        print(f"Scraping squad for {team_name}...")
        squad_data = get_squad_details(team_url, team_name)

        for player in squad_data:
            if player["Profile URL"] != "No Link":
                print(f"Fetching details for {player['Name']}...")
                details = get_player_details(player["Profile URL"])
                player.update(details)
                time.sleep(1)  # Avoids overloading the site

        all_squad_data.extend(squad_data)

    # Convert to DataFrame & Insert into PostgreSQL
    df_final = pd.DataFrame(all_squad_data)
    insert_rugby_stats(df_final)

# Run the scraper
scrape_urc_teams()
