import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

#base_dir bring the current path for the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#raw_dir bring the current path of raw_data folder
RAW_DIR = os.path.join(BASE_DIR,"data","raw_data")
#output_dir bring the current path of output folder 
OUTPUT_DIR = os.path.join(BASE_DIR,"data","output")
#raw_data bring the path of the raw data file to use it easily with pandas
raw_data = os.path.join(RAW_DIR,"fifa_world_cup_2026_player_performance.csv") 
#log_file bring the path of the log file 
log_file = os.path.join(BASE_DIR,"log_file.txt")
#cleaned_data bring the path of the cleaned data file to use it easily with pandas
cleaned_data = os.path.join(OUTPUT_DIR,"cleaned_data.csv")
#driven_data bring the path of the driven data file to use it easily with pandas
driven_data = os.path.join(OUTPUT_DIR,"driven_data.csv")
#team_summary_data bring the path of the team summary data file to use it easily with pandas
team_summary_data = os.path.join(OUTPUT_DIR,"team_summary.csv")
#player_summary_data bring the path of the player summary data file to use it easily with pandas
player_summary_data = os.path.join(OUTPUT_DIR,"player_summary.csv")
#top_players_data bring the path of the top players data file to use it easily with pandas
top_players_data = os.path.join(OUTPUT_DIR,"top_players.csv")
# if the RAW_DIR not exist it will make it
os.makedirs(RAW_DIR,exist_ok = True)
# if the OUTPUT_DIR not exist it will make it
os.makedirs(OUTPUT_DIR , exist_ok= True)
#i defined the logger for my project
logger = logging.getLogger("fifa_etl")
# set the level of logger start from info
logger.setLevel("INFO")
#i used it to avoid duplicate handlers on the rerun
logger.handlers = []

file_handler = logging.FileHandler(log_file ,mode='w', encoding = 'utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

#i make range_rules to use it for checking validation data for range numbers for important columns
range_rules = {
    "age":(15,45),
    "height_cm":(150,220),
    "weight_kg":(50,110),
    "minutes_played":(0,120),
    "pass_accuracy":(0.0,1.0),
    "player_rating":(0.0,10.0),
    "tournament_rating": (0.0, 10.0),
    "save_percentage": (0.0, 1.0),
}
#i make it for checking the schema validation
required_columns=[
      'player_id', 'player_name', 'age', 'nationality', 'team',
       'jersey_number', 'position', 'height_cm', 'weight_kg', 'preferred_foot',
       'club_name', 'market_value_eur', 'match_id', 'match_date', 'stadium',
       'city', 'opponent_team', 'tournament_stage', 'match_result',
       'goals_team', 'goals_opponent', 'minutes_played', 'goals', 'assists',
       'shots', 'shots_on_target', 'expected_goals_xg', 'expected_assists_xa',
       'key_passes', 'successful_passes', 'total_passes', 'pass_accuracy',
       'dribbles_attempted', 'successful_dribbles', 'crosses',
       'successful_crosses', 'tackles', 'interceptions', 'clearances',
       'blocks', 'aerial_duels_won', 'aerial_duels_lost', 'recoveries',
       'defensive_actions', 'fouls_committed', 'fouls_suffered',
       'yellow_cards', 'red_cards', 'offsides', 'saves', 'save_percentage',
       'punches', 'clean_sheet', 'goals_conceded', 'penalty_saves',
       'distance_covered_km', 'sprint_distance_km', 'top_speed_kmh',
       'accelerations', 'decelerations', 'stamina_score', 'player_rating',
       'performance_score', 'offensive_contribution', 'defensive_contribution',
       'possession_impact', 'pressure_resistance', 'creativity_score',
       'consistency_score', 'clutch_performance_score',
       'total_goals_tournament', 'total_assists_tournament',
       'total_minutes_tournament', 'player_of_match_awards',
       'tournament_rating'

]
#i make Non_negative_columns to use it for checking validation data to ensure that the values of columns not in negative in number
NON_NEGATIVE_COLUMNS = [    
    "goals", "assists", "shots", "shots_on_target", "key_passes",
    "successful_passes", "total_passes", "dribbles_attempted",
    "successful_dribbles", "crosses", "successful_crosses", "tackles",
    "interceptions", "clearances", "blocks", "aerial_duels_won",
    "aerial_duels_lost", "recoveries", "defensive_actions",
    "fouls_committed", "fouls_suffered", "yellow_cards", "red_cards",
    "offsides", "saves", "punches", "goals_conceded", "penalty_saves",
    "total_goals_tournament", "total_assists_tournament",
    "total_minutes_tournament", "player_of_match_awards",
]

REQUIRED_NUMRIC_COLUMNS = ['age','height_cm', 'weight_kg','market_value_eur',
                            "goals", "assists", "shots", "shots_on_target", "key_passes",
                            "successful_passes", "total_passes", "dribbles_attempted",
                            "successful_dribbles", "crosses", "successful_crosses", "tackles",
                            "interceptions", "clearances", "blocks", "aerial_duels_won",
                            "aerial_duels_lost", "recoveries", "defensive_actions",
                            "fouls_committed", "fouls_suffered", "yellow_cards", "red_cards",
                            "offsides", "saves", "punches", "goals_conceded", "penalty_saves",
                            "total_goals_tournament", "total_assists_tournament",
                            "total_minutes_tournament", "player_of_match_awards",

                        ]

#function for making logging
def log(message,level="info"):
    getattr(logger,level)(message)

#function for extracting raw_data from csv_file
def extract(file_path=raw_data):
    log("statring the pipline","info")
    log(f"read the csv file from {file_path}")
    #read csv file or raw data with using error handling to check for error
    try:
       df = pd.read_csv(file_path)
    except FileNotFoundError as ex:
        log(f"file not found at {file_path} : {ex}","error")
        raise
    except pd.errors.ParserError as ex:
        log(f"can't parse csv : {ex}","error")
        raise
    except Exception as e:
        log(f"an error has occured : {e}", "error")
        raise
    else:
        log(f"the raw data loaded successfully {df.shape[0]} rows , {df.shape[1]} columns")
    return df

def check_schema(df):
    log("start checking the schema of data")
    validation_schema = set(required_columns) - set(df.columns)
    if len(validation_schema) > 0 :
        log(f"there is an error in the schema validation in {validation_schema}","error")
        raise ValueError(f"there is error in the schema of data in missing column{validation_schema}")
    else:
        log("the schema of data is valid")
    return df


#function for checking the validation of data    
def transformation(df):
    log("statr  transformation ")
#remove duplicated rows
    log("start checking duplicated rows")
    initial_duplicated = df.duplicated().sum()
    log(f"found duplicated {initial_duplicated} rows")
    if initial_duplicated > 0:
        df=df.drop_duplicates()
        log(f"removed  duplicated {initial_duplicated} rows successfully ")
#Clean column names
    log("start cleaning columns name")
    df.columns = df.columns.str.strip().str.lower()
    log(f"print name of columns after cleaning the name of columns {df.columns}")
#Convert required numeric columns
    for col in REQUIRED_NUMRIC_COLUMNS:
            df[col] = pd.to_numeric(df[col],errors="coerce")
    log("transformation required numric columns to numric successfully")

#handling missing values
    log("starting search for missing values")
    for col in df.columns:
            col_missing = df[col].isnull().sum()
            if col_missing > 0 :
                log(f"column {col} has found {col_missing} missing values")
                if col in NON_NEGATIVE_COLUMNS:
                    df[col]=df[col].fillna(0)
                    log(f"filled missing values in {col} with zero")
                elif df[col].dtype=='object':
                     df[col]= df[col].fillna("unknown")
                     log(f"filled missing values in {col} with unknow")
                
                else:
                    log(f"missing value in {col} will be kept as Nan")

    log(f"filled missing values in {col} successfully")
#transform the colmn match_date to datetime data type
    df['match_date'] = pd.to_datetime(df['match_date'])
    df['year_of_match'] = df['match_date'].dt.year
    df['month_of_match'] = df['match_date'].dt.month
    df['day_of_match'] = df['match_date'].dt.day
    df['name_of_day_match'] = df['match_date'].dt.day_name()
    return df

def validation(df):
#range validation
    log("start checking the validation of data")
    for column,(minimum,maximum) in range_rules.items():
        invalid = (df[column] < minimum) | (df[column] > maximum)
        if invalid.any():
            log(f"there is problem in {column} range and has {invalid.sum()} values","error")
# Non-negative validation
    for column in NON_NEGATIVE_COLUMNS:
        invalid = df[column] < 0
        if invalid.any():
            log(f"ther is column {column} has {invalid.sum()} negative values","error")

    df.to_csv(cleaned_data,index=False)
    log(f"the cleaned data saved successfully in {cleaned_data} with {df.shape[0]} rows and {df.shape[1]} columns")
    return df


def business_validation(df):
    log("start checking the business validation of data")
#shot
    invalid_shots = df['shots_on_target'] > df['shots']
    if invalid_shots.any():
        df.loc[invalid_shots, 'shots_on_target'] = df.loc[invalid_shots, 'shots']
        log(f"there is problem in shots_on_target and has {invalid_shots.sum()} values","error")
#pass
    invalid_passes = df['successful_passes'] > df['total_passes']
    if invalid_passes.any():
        df.loc[invalid_passes, 'successful_passes'] = df.loc[invalid_passes, 'total_passes']
        log(f"there is problem in successful_passes and has {invalid_passes.sum()} values","error")
#dribbles
    invalid_dribbles = df['successful_dribbles'] > df['dribbles_attempted']
    if invalid_dribbles.any():
        df.loc[invalid_dribbles, 'successful_dribbles'] = df.loc[invalid_dribbles, 'dribbles_attempted']
        log(f"there is problem in successful_dribbles and has {invalid_dribbles.sum()} values","error")
#crosses
    invalid_crosses = df['successful_crosses'] > df['crosses']
    if invalid_crosses.any():
        df.loc[invalid_crosses, 'successful_crosses'] = df.loc[invalid_crosses, 'crosses']
        log(f"there is problem in successful_crosses and has {invalid_crosses.sum()} values","error")
#goals
    invalid_goals = df['goals'] > df['shots_on_target']
    if invalid_goals.any():
        df.loc[invalid_goals, 'shots_on_target'] = df.loc[invalid_goals, 'goals']
        log(f"there is problem in goals > shots_on_target and corrected {invalid_goals.sum()} values", "warning")
    return df

def driven_columns(df):
    log("start checking the driven columns of data")
#calculate the goals_per_90
    minutes=df['minutes_played'].replace(0, pd.NA)  # Replace 0 with NaN to avoid division by zero
    df['goals_per_90'] = ((df['goals']/minutes)*90).fillna(0)
    log("calculated the goals_per_90 successfully") 
#calculate the assists_per_90
    df['assists_per_90'] = ((df['assists']/minutes)*90).fillna(0)
    log("calculated the assists_per_90 successfully")   
#calculate the goal_contribution_per_90
    df['goal_contribution_per_90'] = (((df['goals'] + df ['assists'])/minutes)*90).fillna(0)
    log("calculated the goal_contribution_per_90 successfully")         
#calculate the pass_accuracy_percentage
    df['pass_accuracy_percentage'] = df['pass_accuracy'] * 100      
    log("calculated the pass_accuracy_percentage successfully") 
#calculate the save_percentage_percentage
    df['save_percentage_percentage'] = df['save_percentage'] * 100      
    log("calculated the save_percentage_percentage successfully")   
#calculate the defensive_actions_per_90
    df['defensive_actions_per_90'] = (df['defensive_actions']/minutes)*90
    log("calculated the defensive_actions_per_90 successfully")
    df.to_csv(driven_data,index=False)
    log(f"the driven data saved successfully in {driven_data} with {df.shape[0]} rows and {df.shape[1]} columns")
    return df

def aggregate_data(df):
    log("start aggregating the data")
#aggregate the team summary statistics
    team_summary = df.groupby('team').agg(

        total_goals=('goals', 'sum'),
        total_assists=('assists', 'sum'),
        total_shots=('shots', 'sum'),
        total_shots_on_target=('shots_on_target', 'sum'),
        total_minutes_played=('minutes_played', 'sum'),
        average_rating=('player_rating', 'mean')   
    ).reset_index()
    team_summary.to_csv(team_summary_data, index=False)
    log(f"the team summary data saved successfully in {team_summary_data} with {team_summary.shape[0]} rows and {team_summary.shape[1]} columns")
#aggregate the player summary statistics   
    player_summary = df.groupby('player_name').agg(
        total_goals=('goals', 'sum'),
        total_assists=('assists', 'sum'),
        total_shots=('shots', 'sum'),
        total_shots_on_target=('shots_on_target', 'sum'),
        total_minutes_played=('minutes_played', 'sum'),
        average_rating=('player_rating', 'mean')
    ).reset_index()
    player_summary.to_csv(player_summary_data, index=False)
    log(f"the player summary data saved successfully in {player_summary_data} with {player_summary.shape[0]} rows and {player_summary.shape[1]} columns") 
    
#top players based on goals scored
    log("start calculating the top players based on goals scored")
    top_players = df.groupby(['player_name','player_id']).agg(
        total_goals=('goals', 'sum'),
        total_assists=('assists', 'sum'),
        total_minutes_played=('minutes_played', 'sum')).reset_index()
#calculate the goals per 90 minutes for top players based on goals scored
    top_players['goals_per_90']=(
        top_players['total_goals']/top_players['total_minutes_played']*90
    ).fillna(0)
#sort the top players based on goals per 90 minutes in descending order
    top_players=top_players.sort_values(by='goals_per_90', ascending=False)
    top_players.to_csv(top_players_data, index=False)
    log(f"the top players data saved successfully in {top_players_data} with {top_players.shape[0]} rows and {top_players.shape[1]} columns")
    return df


def connect_to_database():
    try:
        connection_url = URL.create(
            
        "mssql+pyodbc",
        username = "sa",
        password  = "AsA12345@@",
        host = "MOMEN",
        database = "fifa_etl_pipline",
            query={
            "driver": "ODBC Driver 17 for SQL Server",
            "TrustServerCertificate": "yes"
        }
        )

        engine = create_engine(connection_url)

        with engine.connect() as connection:
            log("Connected to the database successfully", "info")
        return engine
    except Exception as e:
        log(f"Failed to connect to the database: {e}", "error")
        raise
       
def load_data_to_database(df, table_name, engine,schema=None):
    try:
        log(f"Starting loading data into {schema}.{table_name}")
        log(f"Data shape: {df.shape}")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False,chunksize=1000,schema=schema)
        log(f"Data loaded into table '{schema}.{table_name}' successfully", "info")
    except Exception as e:
        log(f"Failed to load data into table '{schema}.{table_name}': {e}", "error")
        raise

df=extract(raw_data)
df=check_schema(df)
df=transformation(df)
df=validation(df)
df=business_validation(df)
df=driven_columns(df)
df=aggregate_data(df)
engine = connect_to_database()
load_data_to_database(df, "fifa_world_cup_2026_player_performance", engine,schema = "bronze")
load_data_to_database(pd.read_csv(cleaned_data), "cleaned_data", engine,schema = "silver")
load_data_to_database(pd.read_csv(driven_data), "driven_data", engine,schema = "silver")
load_data_to_database(pd.read_csv(team_summary_data), "team_summary", engine,schema = "silver")
load_data_to_database(pd.read_csv(player_summary_data), "player_summary", engine,schema = "silver")
load_data_to_database(pd.read_csv(top_players_data), "top_players", engine,schema = "silver")
