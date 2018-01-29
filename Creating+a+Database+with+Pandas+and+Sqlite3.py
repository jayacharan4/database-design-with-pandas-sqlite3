
# coding: utf-8

# In[2]:


import pandas as pd
import sqlite3
import csv

pd.set_option("max_columns",180)
pd.set_option('max_rows', 200000)
pd.set_option('max_colwidth', 5000)


# In[5]:


game_log = pd.read_csv("game_log.csv",low_memory = False)
print(game_log.shape)
game_log.head()


# In[6]:


person_codes = pd.read_csv('person_codes.csv')
print(person_codes.shape)
person_codes.head()


# In[7]:


park_codes = pd.read_csv('park_codes.csv')
print(park_codes.shape)
park_codes.head()


# In[8]:


team_codes = pd.read_csv('team_codes.csv')
print(team_codes.shape)
team_codes.head()


# In[12]:


game_log["h_league"].value_counts().head()


# In[14]:


def league_desc(league):
    league_games = game_log[game_log["h_league"]==league]
    start = league_games["date"].min()
    end = league_games["date"].max()
    print("{} happened between {} and {}".format(league,start,end))
for league in game_log["h_league"].unique():
    league_desc(league)


# In[21]:


database = "mlb.db"

def run_query(q):
    with sqlite3.connect(database) as conn:
        return pd.read_sql_query(q,conn)
def run_command(cmd):
    with sqlite3.connect(database) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        conn.isolation_level = None
        conn.execute(cmd)
def show_tables():
    q = """
    SELECT 
        name,
        type 
    FROM sqlite_master
    WHERE type IN ('table','view');
    """
    return run_query(q)
show_tables()


# In[28]:


tables = {
    "game_log": game_log,
    "person_codes": person_codes,
    "team_codes": team_codes,
    "park_codes": park_codes
}
with sqlite3.connect(DB) as conn:    
    for name, data in tables.items():
        conn.execute("DROP TABLE IF EXISTS {};".format(name))
        data.to_sql(name,conn,index=False)


# In[29]:


show_tables()


# In[38]:


cmd1 = """
ALTER TABLE game_log
ADD COLUMN game_id TEXT;
"""
try:
    run_command(cmd1)
except:
    pass
cmd2 = """
UPDATE game_log 
SET game_id = date || h_name || number_of_game
WHERE game_id IS NULL;
""" 
run_command(cmd2)

query = """
SELECT
    game_id,
    date,
    h_name,
    number_of_game
FROM game_log
LIMIT 5;
"""

run_query(query)


# In[39]:


cmd1 = """
CREATE TABLE IF NOT EXISTS person (
    person_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT
);
"""

cmd2 = """
INSERT OR IGNORE INTO person
SELECT
    id,
    first,
    last
FROM person_codes;
"""

query = """
SELECT * FROM person
LIMIT 5;
"""

run_command(cmd1)
run_command(cmd2)
run_query(query)


# In[40]:


c1 = """
CREATE TABLE IF NOT EXISTS park (
    park_id TEXT PRIMARY KEY,
    name TEXT,
    nickname TEXT,
    city TEXT,
    state TEXT,
    notes TEXT
);
"""

c2 = """
INSERT OR IGNORE INTO park
SELECT
    park_id,
    name,
    aka,
    city,
    state,
    notes
FROM park_codes;
"""

q = """
SELECT * FROM park
LIMIT 5;
"""

run_command(c1)
run_command(c2)
run_query(q)


# In[42]:


c1 = "DROP TABLE IF EXISTS appearance_type;"

run_command(c1)

c2 = """
CREATE TABLE appearance_type (
    appearance_type_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT
);
"""
run_command(c2)

appearance_type = pd.read_csv('appearance_type.csv')

with sqlite3.connect('mlb.db') as conn:
    appearance_type.to_sql('appearance_type',
                           conn,
                           index=False,
                           if_exists='append')

q = """
SELECT * FROM appearance_type;
"""

run_query(q).head()

