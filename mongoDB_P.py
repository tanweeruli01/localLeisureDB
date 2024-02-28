from pymongo import MongoClient
from pprint import pprint
import os

password = os.environ.get("MONGODB_PW")
if password is None:
    print("MongoDB Password has not been set in environment variables!")
else:
    print("MongoDB password has been imported Successfully!")

client = MongoClient(f"mongodb+srv://tanweeruli01:{password}@cluster0.pgw2o4p.mongodb.net/")
 

mongoDB = client['local_leisure']

course = mongoDB['Course']
members = mongoDB['Members']
lessons = mongoDB['Lessons']

#A.	Use the SQL AND, OR and NOT Operators in your query (The WHERE clause can be combined with AND, OR, and NOT operators)
#A1.Where courseID is equals to a number below 5 and the first name of any of the instructors
print('\nA1\n')
cColl1 = course.find({"courseID": {'$lte':5}}, {"Instructor":1, '_id':0})
for aDoc in cColl1:
    pprint(aDoc)
# #A2. Where courseID is equals to a number above 5 and the lesson time is in the morning or afternoon.
print("\nA2\n")
cColl2 = course.find(
    {
        "courseID": {'$gte': 5 },
        "Sessions": {'$in': ["Morning", "Afternoon"]}
    }
)
for aDoc in cColl2:
    pprint(aDoc)

# #B.	Order by the above results by:
# #B1. startDate in “course” table
print("\nB1\n")
cColl3 = course.find().sort({"startDate":1})
for aDoc in cColl3:
    pprint(aDoc)
# #B2. MemberID in “members” table
print("\nB2\n")
mColl1 = members.find().sort({"MemberID":1})
for aDoc in mColl1:
    pprint(aDoc)

# C. Update the following:
# C1. Member’s table, change the adresses of any three members
print("\nC1\n")
mColl2 = members.update_one({"memberID":1}, {'$set':{"Address":"10 No Way Home"}})
print(mColl2.modified_count)
mColl3 = members.update_one({"memberID":2}, {'$set':{"Address":"15 No Way Home"}})
print(mColl3.modified_count)
mColl4 = members.update_one({"memberID":3}, {'$set':{"Address":"20 No Way Home"}})
print(mColl4.modified_count)
# # C2. Course table, change the startDate and Lesson time for three of the sessions
print("\nC2\n")
cColl3 = course.update_many(
    {"courseID":{'$in':[1, 2, 3]}},
    {'$set':{"startDate":"2024-10-15", "lessonTime":"14:30"}}
    )
print(f"Number of updated documents: {cColl3.modified_count}")

# D. Use the SQL MIN () and MAX () Functions to return the smallest and largest value
# D1. Of the LessonID column in the “lesson” table
print("\nD1\n")
print("min")
lColl1 = lessons.aggregate([
    {'$group':{'_id':None, 'minValue':{'$min':"$LessonID"}}}
])
for aDoc in lColl1:
    pprint(aDoc)
print("\nmax")
lColl2 = lessons.aggregate([
    {'$group':{ '_id' : None, 'maxValue':{'$max':"$LessonID"}}}
])
for aDoc in lColl2:
    pprint(aDoc)
# # D2. Of the membersID column in the “members” table
print("\nD2\n")
print("min")
mColl5 = members.aggregate([
    {'$group':{'_id':None, 'minValue':{'$min':"$memberID"}}}
])
for aDoc in mColl5:
    pprint(aDoc)
print("\nmax")
mColl6 = members.aggregate([
    {'$group':{'_id': None, 'maxValue':{'$max':"$memberID"}}}
])
for aDoc in mColl6:
    pprint(aDoc)

# E. Use the SQL COUNT (), AVG () and SUM () Functions for these:
# 1. Count the total number of members in the “members” table
print("\nE1")
mColl7 = members.count_documents({})
pprint(f"Total members: {mColl7}")
# # 2. Count the total number of sessions in the” members” table
print("\nE2")
cColl4 = course.count_documents({"Sessions":{'$exists':True}})
pprint(f"Total number of sessions: {cColl4}")
# # 3. Find the average session time for all “sessions” in course table
# print("\nE3")
cColl5 = course.aggregate([
    {'$group': {'_id': None, 'avgLessonTime':{'$avg':'$lessonTime'}}}
])
for aDoc in cColl5:
    pprint(f"Average Session Time: {aDoc}")

# F. WILDCARD queries (like operator)
# a) Find all the people from the “members” table whose last name starts with A.
print("\nF.a)")
mColl8 = members.find({"Surname": {'$regex':'^A'}})
for aDoc in mColl8:
    pprint(aDoc)
# b) Find all the people from the “members” table whose last name ends with A.
print("\nF.b)")
mColl9 = members.find({"Surname":{'$regex':'a$'}})
for aDoc in mColl9:
    pprint(aDoc)
# c) Find all the people from the “members” table that have "ab" in any position in the last name.
print("\nF.c)")
meColl = members.find({"Surname":{'$regex':'ab'}})
for aDoc in meColl:
    pprint(f"aDoc\n")
# d) Find all the people from the “members” table that that have "b" in the second position in their first name.
print("\nF.d)")
meColl1 = members.find({"FirstName":{'$regex':'^.{1}b'}})
for aDoc in meColl1:
    pprint(f"aDoc\n")
# e) Find all the people from the “members” table whose last name starts with "a" and are at least 3 characters in length:
print("\nF.e)")
meColl2 = members.find({"Surname":{"$regex": "^A.{2}"}})
for aDoc in meColl2:
    pprint(aDoc)
# f) Find all the people from the “members” table whose last name starts with "a" and ends with "y"
print("\nF.f)")
meColl3 = members.find({"Surname":{'$regex': '^A.*y$'}})
for aDoc in meColl3:
    pprint(aDoc)
# g) Find all the people from the “members” table whose last name does not starts with "a" and ends with "y"
print("\nF.g)")
meColl4 = members.find({"Surname":{'$regex':'^(?!A).*y$'}})
for aDoc in meColl4:
    pprint(aDoc)

# G. What do you understand by LEFT and RIGHT join? Explain with an example.
#  ANSWER: a join is used to combine rows from two or more tables based on related columns between them such as a foreign key   