import db

# maak connectie met de database
database = ''
dbConnection = db.connect(database)

# zet de query
query = ""
dbConnection.setQuery(query)

# voer query uit
dbConnection.execute()

# haal data op die bij de query hoort
data = dbConnection.fetch()

# laat een stuk van de data zien
dbConnection.printSnippet(data)

# optionele view functie
# dbConnection.showInPopup(data)