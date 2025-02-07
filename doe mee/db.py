from datetime import datetime
import sqlite3, json, os, time, pip
import tkinter as tk
from tkinter import ttk

class connect:

    def __init__(self, dbname, followProgress=True):
        self._basepath = os.path.dirname(__file__) + '//'
        self._dbname = dbname
        self._followProgress = followProgress
        self._timestamp = datetime.now().strftime("%H:%M:%S")
        self._result = []
        self._query = ''
        self._error = None
        self._progress = []
        self._columns = []

        try:
            __import__('pandas')
        except ImportError:
            pip.main(['install', 'pandas'])

        try:
            __import__('termcolor')
        except ImportError:
            pip.main(['install', 'termcolor'])

        self.print_progress('\nDatabase request started', "magenta")
        
        if not os.path.isfile(self._basepath+self._dbname):
            self.print_progress(f'Creating database "{self._dbname}"', 'yellow')
        
        self._conn = sqlite3.connect(self._basepath+self._dbname)
        self.print_progress(f'Connecting to database "{self._dbname}"')


    def __del__(self):
        self._conn.close()
        self.print_progress('Database request ended\n', "magenta")

    def setQuery(self, query, log=True):
        self._query = query.strip()

        if not self._validate_query():
            self._error = f'"{self._query}" is not a query'
            self.print_progress(self._error, "red")
            
        if log:
            self._logQuery()

    def _validate_query(self):
        return len(self._query) >= 8
    
    def execute(self):
        try:
            if not self._validate_query():
                self._error = f'Cannot execute "{self._query}"'
                self.print_progress(self._error, "red")
            else:
                self._cursor = self._conn.cursor()
                self.print_progress(f'Executing query "{self._query}"')
                self._cursor.execute(self._query)
                self._conn.commit()

                if (self._cursor.description == None):
                    self.print_progress("Query Excecuted", "green")
                                
        except sqlite3.OperationalError as dberror:
            self._error = str(dberror)
            self.print_progress(self._error, "red")
        except Exception as exceptionError:
            self._error = repr(exceptionError)
            self.print_progress(self._error, "red")

    def fetch(self, save=True):
        try:
            if self._validate_query():
                self.print_progress('Fetching data')
                self._result = self._cursor.fetchall()

                if len(self._result) > 0:
                    self._columns = [description[0] for description in self._cursor.description]
                    total_rows = len(self._result)
                    total_columns = len(self._columns)

                    result = []
                    for i in range(total_rows):
                        entry = {}
                        for j in range(total_columns):
                            entry[self._columns[j]] = self._result[i][j]
                        result.append(entry)

                    self._result = result
                    
                    if len(result) > 1:
                        self.print_progress(f'{len(self._result)} results found')
                        
        except sqlite3.OperationalError as dberror:
            self._error = str(dberror)
            self.print_progress(self._error, "red")
        except Exception as exceptionError:
            self._error = repr(exceptionError)
            self.print_progress(self._error, "red")
        
        if save:
            self._saveResult()

        return self._result

    # def fetchTables(self):
    #     return self.fetch("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    
    def printSnippet(self, data:list):
        if not isinstance(data, list):
            __import__('termcolor').cprint('Data is not a list', color="red")
            print('Data given:', data)
        elif len(data) == 0:
            __import__('termcolor').cprint('No data to show', color="yellow")
        else:
            indexes = [''] * len(data)
            dataFrame = __import__('pandas').DataFrame(data, index=indexes)
            print('')
            print(dataFrame)
            print('')
    
    def showInPopup(self, data:list, title='Data table'):
        if not isinstance(data, list):
            __import__('termcolor').cprint('Data is not a list', color="red")
            print('Data given:', data)
        elif len(data) == 0:
            __import__('termcolor').cprint('No data to show', color="yellow")
        else:
            self.print_progress('Opening popup')

            root = tk.Tk()
            root.title(title)
            root.geometry(f'820x410+60+60')

            tree = ttk.Treeview(root, columns=list(data[0].keys()), show='headings')

            for col in data[0].keys():
                tree.heading(col, text=col, anchor=tk.W)
                tree.column(col, anchor=tk.W, stretch=False)

            for item in data:
                tree.insert('', tk.END, values=list(item.values()))

            tree.pack(expand=True, fill='both', anchor=tk.CENTER)

            root.mainloop()

    def _logQuery(self):
        if not os.path.exists(self._basepath+'querylog'):
            os.makedirs(self._basepath+'querylog')

        filename = datetime.now().strftime("%d_%m_%Y")
        filename = "querylog/"+filename+".log"
        self.print_progress(f'Logging query in: {filename}')
        
        f = open(self._basepath+filename, "a")
        f.write(self._query+'; #'+self._timestamp+'@'+self._dbname+'\n')
        f.close()

    def _saveResult(self):
        if not os.path.exists(self._basepath+'datalog'):
            os.makedirs(self._basepath+'datalog')

        dirname = datetime.now().strftime("%d_%m_%Y")
        if not os.path.exists(self._basepath+'datalog/'+dirname):
            os.makedirs(self._basepath+'datalog/'+dirname)

        json_object = json.dumps({
            'database' : self._dbname,
            'query' : self._query,
            'error' : self._error,
            'resultcount' : len(self._result) if isinstance(self._result, list) else None,
            'result' : self._result
        }, indent=4)
        
        filename = "datalog/"+dirname+"/"+self._timestamp.replace(':','')+".json"
        self.print_progress(f'Saving result in: {filename}')
        
        f = open(self._basepath+filename, "w")
        f.write(json_object)
        f.close()
    
    def print_progress(self, msg, color=None):
        self._progress.append(msg)
        if self._followProgress:
            __import__('termcolor').cprint(msg, color=color)
            time.sleep(0.3)
