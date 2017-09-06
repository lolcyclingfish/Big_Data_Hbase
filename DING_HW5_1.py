"""
Spyder Editor

Name: Meghan Ding
Task: MSiA 2017 Spring Big Data HW5
"""

import os
from starbase import Connection

# The restful interface on wolf for hbase runs on port 20550
con = Connection(port = 20550)

#1. Create an hbase database model:
#build the table schema
t = con.table('enron_meghan')
t.drop()
t.create('user', 'address', 'datetime', 'body')

#2. Import all emails into hbase:
row_ind = 0
folder_path = '/home/public/course/enron/'
user_names = os.listdir(folder_path) #extract subfolder names as list of user_names
for user in user_names:
    user_path = os.path.join(folder_path, user) 
    emails = os.listdir(user_path) #extract subfile names as list of email names within each users' folders
    
    for email in emails:
        email_path = os.path.join(user_path, email)
        with open(email_path) as f:
            lines = f.readlines()

        #all the [:-1] is used for removing the next line just in case there no whitespace seperating lines for current line
        date = lines[1][:-1].split(' ')
        week_day = date[1]
        day = date[2]
        month = date[3]
        year = date[4]
        time = date[5]

        address_from = lines[2][:-1].split(' ')[1]
        address_to = lines[3][:-1].split(' ')[1]

        #retrieve the email content:
        #enumerate lines till reach 'X-FileName', after this line will be content of the email
        body = False
        for ind, line in enumerate(lines):
            if line[:10] == 'X-FileName':
                body = True
                starting_line= ind
                break

        content = ''.join(lines[starting_line+1:])

        data = {
            'user': {'name': user},
            'address': {'from': address_from, 'to': address_to},
            'datetime': {'week_day': week_day, 'day': day, 'month': month, 'year': year, 'time': time},
            'body': {'body': content}
        }
        t.insert('row' + str(row_ind), data)
        row_ind += 1

#3. return the bodies of all emails for a user of your choice (as a single text file)
#extract emails of mKelly: only one email will be returned 
query_user = 'mKelly'
content = ''
for row in range(row_ind):
    record = t.fetch('row' + str(row), ['user', 'body'])
    if record['user']['name'] == query_user:
        content += record['body']['body']

with open('DING_1.3.txt', 'w') as f:
    f.write(content)


#4. Return the bodies of all emails written during a particular month of your choice (as a single text file).
#extract emails from Feb: only one email rewritten by mCarson will be returned
query_month = 'Feb'
content = ''
for row in range(row_ind):
    record = t.fetch('row' + str(row), ['body', 'datetime'])
    if record['datetime']['month'] == query_month:
        content += record['body']['body']

with open('DING_1.4.txt', 'w') as f:
    f.write(content)


#5. Return the bodies of all emails of a given user during a particular month both of your choice (as a single text file).
#extract emails of mKelly in Feb: only one email will be returned 
query_user = 'mKelly'
query_month = 'Feb'
content = ''
for row in range(row_ind):
    record = t.fetch('row' + str(row), ['user', 'body', 'datetime'])
    if record['user']['name'] == query_user and record['datetime']['month'] == query_month:
        content += record['body']['body']

with open('DING_1.5.txt', 'w') as f:
    f.write(content)