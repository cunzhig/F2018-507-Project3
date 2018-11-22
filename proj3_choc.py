import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)

# Create Table Bars, Countries
statement = '''
        CREATE TABLE `Bars` (
        `Id`	INTEGER PRIMARY KEY AUTOINCREMENT,
        `Company`	TEXT,
        `SpecificBeanBarName`	TEXT,
        `REF`	TEXT,
        `ReviewDate`	TEXT,
        `CocoaPercent`	REAL,
        `CompanyLocationId`	INTEGER,
        `Rating`	REAL,
        `BeanType`	TEXT,
        `BroadBeanOriginId`	INTEGER
);
'''
cur.execute(statement)

statement = '''
        CREATE TABLE `Countries` (
	`Id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`Alpha2`	TEXT,
	`Alpha3`	TEXT,
	`EnglishName`	TEXT,
	`Region`	TEXT,
	`Subregion`	TEXT,
	`Population`	INTEGER,
	`Area`	REAL
);
'''
cur.execute(statement)
conn.commit()

# Read data from files
with open(BARSCSV, 'r') as f:
        reader = csv.reader(f)
        cacao = list(reader)
        f.close()

f = open(COUNTRIESJSON, 'r', encoding='utf-8')
reader = f.read()
countries = json.loads(reader)

# Insert data
# Table Contry
for row in countries:
        insertion = (None, row['alpha2Code'], row['alpha3Code'], row['name'],
                     row['region'], row['subregion'], row['population'], row['area'])
        statement = 'INSERT INTO "Countries" '
        statement += 'VALUES (?,?,?,?,?,?,?,?)'
        cur.execute(statement, insertion)
# Table Bars
flag = 0
for row in cacao:
        if flag != 0:
                statement = 'SELECT ID FROM Countries WHERE EnglishName = ?'
                cur.execute(statement, (row[5],))
                try:
                        cId = cur.fetchone()[0]
                except:
                        cId = row[5]
                cur.execute(statement, (row[8],))
                try:
                        bId = cur.fetchone()[0]
                except:
                        bId = row[8]
                percent = row[4].split('%')[0]
                insertion = (None, row[0], row[1], row[2],
                             row[3], percent, cId, row[6], row[7], bId)
                statement = 'INSERT INTO "Bars" '
                statement += 'VALUES (?,?,?,?,?,?,?,?,?,?)'
                cur.execute(statement, insertion)
        else:
                flag = 1
conn.commit()

# Part 2: Implement logic to process user commands


def process_command(command):
        parameters = command.split()
        if command == '':
                return []
        if parameters[0] == 'bars':
                statement = '''
                SELECT Bars.SpecificBeanBarName, Bars.Company, c1.EnglishName,Bars.Rating,Bars.CocoaPercent,c2.EnglishName
                FROM Bars 
                LEFT JOIN Countries AS c1 ON Bars.CompanyLocationId = c1.Id
                LEFT JOIN Countries AS c2 ON Bars.BroadBeanOriginId = c2.Id
                '''
                flag_limit = False
                flag_order = False
                state_limit =''
                state_order =''
                for i in range(1,len(parameters)):
                        line = parameters[i].split('=')
                        # sellcountry=<alpha2>
                        if line[0] == 'sellcountry':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c1.Alpha2 = "{}"\n'.format(line[1])
                        # sourcecountry=<alpha2>
                        elif line[0] == 'sourcecountry':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c2.Alpha2 = "{}"\n'.format(line[1])
                        # sellregion=<name>
                        elif line[0] == 'sellregion':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c1.Region = "{}"\n'.format(line[1])
                        # sourceregion=<name>
                        elif line[0] == 'sourceregion':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c2.Region = "{}"\n'.format(line[1])
                        # ratings|cocoa
                        elif line[0] == 'cocoa':
                                state_order = 'ORDER BY CocoaPercent '
                                flag_order = True
                        elif line[0] == 'ratings':
                                state_order = 'ORDER BY Rating '
                                flag_order = True
                        # top|bottom
                        elif line[0] == 'bottom':
                                state_limit= '\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        elif line[0] == 'top':
                                state_limit = 'DESC\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        else:
                                print('Command not recognized: {}'.format(command))
                                return []

                if flag_order == False:
                        statement +=  'ORDER BY Rating '
                else:
                        statement += state_order
                if flag_limit == False:
                        statement += 'DESC\nLIMIT 10'
                else:
                        statement += state_limit
        elif parameters[0] == 'companies':
                state_select = 'SELECT Company, c.EnglishName, '
                statement = '''
                FROM Bars
                LEFT JOIN Countries AS c ON Bars.CompanyLocationId = c.Id
                '''
                state_group ='''
                GROUP BY Company
                HAVING COUNT(*) > 4
                '''
                flag_limit = False
                flag_order = False
                state_limit =''
                state_order =''
                for i in range(1,len(parameters)):
                        line = parameters[i].split('=')
                        # country=<alpha2>
                        if line[0] == 'country':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c.Alpha2 = "{}"\n'.format(line[1])
                        # region=<name>
                        elif line[0] == 'region':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        statement += 'WHERE c.Region = "{}"\n'.format(line[1])
                        # ratings|cocoa|bars_sold
                        elif line[0] == 'ratings':
                                state_order = 'ORDER BY AVG(Rating) '
                                flag_order = True
                                state_select += 'AVG(Rating) '
                        elif line[0] == 'cocoa':
                                state_order = 'ORDER BY AVG(CocoaPercent) '
                                flag_order = True
                                state_select += 'AVG(CocoaPercent) '
                        elif line[0] == 'bars_sold':
                                state_order = 'ORDER BY COUNT(*)'
                                flag_order = True
                                state_select += 'COUNT(*) '
                        # top|bottom
                        elif line[0] == 'bottom':
                                state_limit= '\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        elif line[0] == 'top':
                                state_limit = 'DESC\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        else:
                                print('Command not recognized: {}'.format(command))
                                return []
                        
                if flag_order == False:
                        state_group +=  'ORDER BY AVG(Rating) '
                        state_select += 'AVG(Rating) '
                else:
                        state_group += state_order
                if flag_limit == False:
                        state_group += 'DESC\nLIMIT 10'
                else:
                        state_group += state_limit
                
                statement = state_select + statement + state_group
        elif parameters[0] == 'countries':
                state_select = 'SELECT c.EnglishName, c.Region, '
                statement = '''
                FROM Bars
                '''
                state_group ='''
                GROUP BY c.EnglishName
                HAVING COUNT(*) > 4
                '''
                flag_limit = False
                flag_order = False
                flag_country = False
                state_limit =''
                state_order =''
                for i in range(1,len(parameters)):
                        line = parameters[i].split('=')
                        # region=<name>
                        if line[0] == 'region':
                                if line[1] == '':
                                        print('Command not recognized: {}'.format(command))
                                        return []
                                else:
                                        state_group = '\nWHERE c.Region = "{}"\n'.format(line[1]) + state_group
                        # sellers|sources:
                        elif line[0] == 'sellers':
                                statement += 'LEFT JOIN Countries AS c ON Bars.CompanyLocationId = c.Id'
                                flag_country = True
                        elif line[0] == 'sources':
                                statement += 'LEFT JOIN Countries AS c ON Bars.BroadBeanOriginId = c.Id'
                                flag_country = True
                        # ratings|cocoa|bars_sold
                        elif line[0] == 'ratings':
                                state_order = 'ORDER BY AVG(Rating) '
                                flag_order = True
                                state_select += 'AVG(Rating) '
                        elif line[0] == 'cocoa':
                                state_order = 'ORDER BY AVG(CocoaPercent) '
                                flag_order = True
                                state_select += 'AVG(CocoaPercent) '
                        elif line[0] == 'bars_sold':
                                state_order = 'ORDER BY COUNT(*)'
                                flag_order = True
                                state_select += 'COUNT(*) '
                        # top|bottom
                        elif line[0] == 'bottom':
                                state_limit= '\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        elif line[0] == 'top':
                                state_limit = 'DESC\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        else:
                                print('Command not recognized: {}'.format(command))
                                return []
                if flag_country == False:
                        statement += 'LEFT JOIN Countries AS c ON Bars.CompanyLocationId = c.Id'
                if flag_order == False:
                                state_group +=  'ORDER BY AVG(Rating) '
                                state_select += 'AVG(Rating) '
                else:
                        state_group += state_order
                if flag_limit == False:
                        state_group += 'DESC\nLIMIT 10'
                else:
                        state_group += state_limit
                statement = state_select + statement + state_group
        elif parameters[0] == 'regions':
                state_select = 'SELECT c.Region, '
                statement = '''
                FROM Bars
                '''
                state_group ='''
                GROUP BY c.Region
                HAVING COUNT(*) > 4
                '''
                flag_limit = False
                flag_order = False
                flag_country = False
                state_limit =''
                state_order =''
                for i in range(1,len(parameters)):
                        line = parameters[i].split('=')
                        # sellers|sources:
                        if line[0] == 'sellers':
                                statement += 'JOIN Countries AS c ON Bars.CompanyLocationId = c.Id'
                                flag_country = True
                        elif line[0] == 'sources':
                                statement += 'JOIN Countries AS c ON Bars.BroadBeanOriginId = c.Id'
                                flag_country = True
                        # ratings|cocoa|bars_sold
                        elif line[0] == 'ratings':
                                state_order = 'ORDER BY AVG(Rating) '
                                flag_order = True
                                state_select += 'AVG(Rating) '
                        elif line[0] == 'cocoa':
                                state_order = 'ORDER BY AVG(CocoaPercent) '
                                flag_order = True
                                state_select += 'AVG(CocoaPercent) '
                        elif line[0] == 'bars_sold':
                                state_order = 'ORDER BY COUNT(*)'
                                flag_order = True
                                state_select += 'COUNT(*) '
                        # top|bottom
                        elif line[0] == 'bottom':
                                state_limit= '\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        elif line[0] == 'top':
                                state_limit = 'DESC\nLIMIT {}'.format(line[1])
                                flag_limit = True
                        else:
                                print('Command not recognized: {}'.format(command))
                                return []
                if flag_country == False:
                        statement += 'JOIN Countries AS c ON Bars.CompanyLocationId = c.Id'
                if flag_order == False:
                                state_group +=  'ORDER BY AVG(Rating) '
                                state_select += 'AVG(Rating) '
                else:
                        state_group += state_order
                if flag_limit == False:
                        state_group += 'DESC\nLIMIT 10'
                else:
                        state_group += state_limit
                statement = state_select + statement + state_group
        else:
                print('Command not recognized: {}'.format(command))
                return []   
        # print(statement)
        # execute
        try:
                cur.execute(statement)
                results = cur.fetchall()
                return results
        except:
                print('Command not recognized: {}'.format(command))
                return []




# result=process_command("regions top=5 ratings sources")
# print('')

 

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!

def print_nice(row,number):
        result = row.ljust(number)
        result = result[:number-1]
        if result[-4:] != '    ':
              result = result[:-4]+'... '
        return result

def print_agg(command,row):
        if 'cocoa' in command:
                result = str(row).split('.')[0]+'%' 
                result = result.ljust(10)
        elif 'bars_sold' in command:
                result = str(row).ljust(10)
        else:  
                result = str(round(row,1)).ljust(10)
        return result

def interactive_prompt():
        help_text = load_help_text()
        response = ''
        while response != 'exit':
                response = input('\nEnter a command: ')

                if response == 'help':
                        print(help_text)
                        continue
                elif response == 'exit':
                        print('bye')
                        continue
                elif response == '':
                        continue
                else:
                        result = process_command(response)
                        command = response.split()
                        if command[0] == 'bars':
                                for row in result:
                                        bar = print_nice(row[0],20)
                                        company = print_nice(row[1],20)
                                        location = print_nice(row[2],20)
                                        rating = str(round(row[3],1)).ljust(10)
                                        cocoa = str(row[4]).split('.')[0]+'%'
                                        cocoa = cocoa.ljust(10)
                                        if row[5] == None:
                                                origin = 'Unknown'
                                        else:
                                                origin = print_nice(row[5],20)
                                        print(bar+company+location+rating+cocoa+origin)
                        elif command[0] == 'companies':
                                for row in result:
                                        company = print_nice(row[0],20)
                                        location = print_nice(row[1],20)
                                        agg = print_agg(command,row[2])
                                        print(company+location+agg)
                        elif command[0] == 'countries':
                                for row in result:
                                        country = print_nice(row[0],20)
                                        region = print_nice(row[1],20)
                                        agg = print_agg(command,row[2])
                                        print(country+region+agg)
                        elif command[0] == 'regions':
                                for row in result:
                                        region = print_nice(row[0],20)
                                        agg = print_agg(command,row[1])
                                        print(region+agg)



# Make sure nothing runs or prints out when this file is run as a module
if __name__ == "__main__":
    interactive_prompt()
