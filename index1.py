import pathlib
import textwrap

import google.generativeai as genai

from IPython.display import display
from IPython.display import Markdown

import os
os.environ["GOOGLE_API_KEY"] = "AIzaSyA4JvVejoRDrYp-71Rps0AMetbpWTRRXCI"
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.pydantic_v1 import BaseModel
from pydantic import Field
import typing
from pytz import timezone
llm = ChatGoogleGenerativeAI(model="gemini-pro",temperature=0.8,convert_system_message_to_human=True)

import re
import pymysql
from datetime import datetime

host = 'localhost'  # Use your Ngrok URL
port = 3050 # Use the port provided by Ngrok
user = 'root'
password = 'Shiva242004'
database = 'link_too'
connection = pymysql.connect(host= host, user=user, password=password, database=database, port=port)
cursor=connection.cursor()
q='Select * from subjects;'
cursor.execute(q)
results= cursor.fetchall()
 
 
from IPython.display import Markdown


 

import re
import pymysql
from datetime import datetime
import pytz
 
from datetime import datetime
# Define IST timezone using pytz
ist_tz = pytz.timezone('Asia/Kolkata')  # Replace with 'UTC' for your system's time

# Get today's date in IST timezone
now_ist = datetime.now(ist_tz)

# Print only the date part
print(now_ist.date())

template_classifier = '''

Consider the following message: "{query}".

Which category best describes the message's intent:

INSERT: This refers to a message about a new activity being created, such as scheduling a new task or setting a deadline or a simple statement. * Does the message introduce a new activity that wasn't previously mentioned?
UPDATE: This refers to a message about an existing event being modified, such as rescheduling, preponing, postponing, or extending an existing task or deadline.
DELETE: This refers to a message about an existing event being removed, such as canceling, deleting or removing.


OUTPUT: Enum(INSERT,UPDATE,DELETE)


'''

template_event_generator = '''
Consider the following message: {query}

Today's date is {current date} in the format of YYYY-MM-DD.
All dates should be of the format YYYY-MM-DD.

Analyse: From the message, identify the (event class) variables being discussed. Pay attention to keywords like 'tomorrow' to infer event dates.

Information to be used:
Subject Details are: {Subject Details};
Different event_types that the database contains: {Event_types}.

Identify and Update: Then update (event class) variables accordingly.

Important note: Only update event class variables when explicitly mentioned in the message; otherwise, update them as ('None').

{format_instructions}

'''


template_sql_generator_i = '''
Generate SQL Query to INSERT given EVENT into EVENTS table.
NOTE : Do not insert values that are not mentioned in the input event. Use subject_id from given information.
NOTE : Do not use "==" in SQL Query. Use '=' instead.

SCHEMA OF THE EVENTS TABLE:
CREATE TABLE EVENTS(
	Event_id INT AUTO_INCREMENT PRIMARY KEY,
    Subject_id Int,
    Event_type VARCHAR(30),
    Event_date DATE,
    Event_time TIME,
 )

 Subject Details (subject_id, subject_name, subject_code):

    Subject_id	Name	Code
    1 Antennas ECPC12
    2 Wireless communications ECPC14
    3 Analog communications ECPC16
    4 Digital communication ECPC18
    5 Semiconductor physics ECPE20
    6 EM waves ECLR22
    7 Microwaves Laboratory ECLR24
    8 Fiber optics Laboratory ECPE26

{format_instructions}
The Input event is : {query}

'''


template_retriever = '''

generate a SQL code to extract rows from the EVENTS sql table whose schema is given by:
CREATE TABLE EVENTS(
    Subject_id Int,
    Event_type VARCHAR(30),
    Event_date DATE,
 )

values of coloum_names is given by {event}.
NOTE : Do not use "==" in SQL Query. Use '=' instead.

NOTE:Ignore the column_names whose values equal to 'None' and column_name : subject_name

{format_instructions}


'''

template_sql_generator_d = '''
Consider the following : {message} and
data from the table(EVENTS table) {events}

Generate a SQL code to DELETE the row that is mentioned in the {message} from the table data {events}; starting like: DELETE FROM EVENTS ......
NOTE : Do not use "==" in SQL Query. Use '=' instead.

SCHEMA OF THE EVENTS TABLE:
CREATE TABLE EVENTS(
	Event_id INT AUTO_INCREMENT PRIMARY KEY,
    Subject_id Int,
    Event_type VARCHAR(30),
    Event_date DATE,
  )

 Subject Details:

    Subject_id	Name	Code
    1 Antennas ECPC12
    2 Wireless communications ECPC14
    3 Analog communications ECPC16
    4 Digital communication ECPC18
    5 Semiconductor physics ECPE20
    6 EM waves ECLR22
    7 Microwaves Laboratory ECLR24
    8 Fiber optics Laboratory ECPE26


{format_instructions}
'''

template_event_generator_u = '''
Consider following message: {query}

Today's date is {current date} in format of YYYY-MM-DD.
All dates should be of the format YYYY-MM-DD.

Analyse : from the message you need identify the original event that is being sicussed and the new changes to it.


Information to be used :
     Subject Details are : {Subject Details};
     Different event_types that database contain : {Event_types}.
Important Note: only use this information when explicitly mentioned.

Update:
then update reschedule_event varaibles.
Important note : Only update reschedule_event variables when explicitly mentioned in the message, otherwise updat them as [None].

{format_instructions}




'''

template_sql_generator_u = '''
Consider the following events: oldevent : {old_event} and newevent : {new_event}
data from the table(EVENTS table) {events}
NOTE : Do not use "==" in SQL Query. Use '=' instead.
SCHEMA OF THE EVENTS TABLE:
CREATE TABLE EVENTS(
	Event_id INT AUTO_INCREMENT PRIMARY KEY,
    Subject_id Int,
    Event_type VARCHAR(30),
    Event_date DATE,
  )

Generate a SQL code to UPDATE the row that is mentioned in the old_event  to make it as new_event from the table data {events}; starting like: UPDATE * FROM EVENTS ......

{format_instructions}


 '''


import re
import psycopg2
import datetime
import json
from typing import TypedDict, List, Annotated
from langchain.output_parsers.enum import EnumOutputParser
from enum import Enum
from typing import Optional

from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field, validator

class WORK(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

parser_classifier = EnumOutputParser(enum=WORK)

class sqlquery(BaseModel):
   sql_query: str | None = Field(description="A single line SQL QUERY to run directly")

class event(BaseModel):
    event_type: str | None = Field(description="Type of the event")
    event_date: str | None = Field(description="Date of the event")
    subject_name : str | None = Field(description = 'The subject linked to the event')
    event_time : str | None = Field(description = 'Time of the event mentioned in the message')
    subject_id: int | str | None = Field(description="Unique identifier corresponding to the subject ")


class reschedule_event(BaseModel):
  event_type: str | None = Field(description="Type of the event")
  subject_name: str | None = Field(description="Name of the subject associated with the event")
  subject_id: int | str | None = Field(description="Unique identifier corresponding to the subject ")
  From_event_date: str | None = Field(description="Original date of the event")
  To_event_date: str | None = Field(description="Rescheduled date of the event")
  From_event_time: str | None = Field(description="Original start time of the event")
  To_event_time: str | None = Field(description="New start time for the rescheduled event")


class events(BaseModel):
     eves : List[event] = Field(description="List of events from the events table")


class AgentState(TypedDict):
    message: str
    work: str
    event: event
    event_u : reschedule_event
    final_sql_query: str
    events: List[dict]

from datetime import datetime


parser_event = PydanticOutputParser(pydantic_object=event)
parser_reschedule_event = PydanticOutputParser(pydantic_object=reschedule_event)
parser_sql = PydanticOutputParser(pydantic_object = sqlquery)

prompt_sql_generator_i = PromptTemplate(
    template= template_sql_generator_i,
    input_variables=["query"],
    partial_variables={"format_instructions": parser_sql.get_format_instructions()},)

prompt_sql_generator_d = PromptTemplate(
    template= template_sql_generator_d,
    input_variables=["message","events"],
    partial_variables={"format_instructions": parser_sql.get_format_instructions()},)

prompt_sql_generator_u = PromptTemplate(
    template= template_sql_generator_u,
    input_variables=["old_event","new_event","events"],
        partial_variables={"format_instructions": parser_sql.get_format_instructions()},)

prompt_classifier = PromptTemplate(
    template= template_classifier,
    input_variables=["query"],

)

prompt_event_generator= PromptTemplate(
    template=template_event_generator,
    input_variables=["query"],
    partial_variables={
        "format_instructions": parser_event.get_format_instructions(),
        "Event_types" : "Different event_types that database contain : Assignment ; Quiz ; Presentation ; Lab Session ; Midterm ; Project Deadline ; Discussion ; Exam ; Class",
        "current date" : str(now_ist.date()),
        "Subject Details" : "{Subject_id;Name;Code]:[1;Antennas;ECPC12];[2;Wireless communications; ECPC14];[3;Analog communications;ECPC16];[4;Digital communication;ECPC18];[5;Semiconductor physics;ECPE20];[6;EM waves;ECLR22];[7;Microwaves Laboratory;ECLR24];[8;Fiber optics Laboratory;ECPE26]"

    }
)

prompt_event_generator_u = PromptTemplate(
    template=template_event_generator_u,
    input_variables=["query"],
    partial_variables={
        "format_instructions": parser_reschedule_event.get_format_instructions(),
        "Event_types" : "Different event_types that database contain : Assignment ; Quiz ; Presentation ; Lab Session ; Midterm ; Project Deadline ; Discussion ; Exam ; Class",
        "current date" : str(now_ist.date()),
        "Subject Details" : "{Subject_id;Name;Code]:[1;Antennas;ECPC12];[2;Wireless communications; ECPC14];[3;Analog communications;ECPC16];[4;Digital communication;ECPC18];[5;Semiconductor physics;ECPE20];[6;EM waves;ECLR22];[7;Microwaves Laboratory;ECLR24];[8;Fiber optics Laboratory;ECPE26]"

    }
)

prompt_retriever = PromptTemplate(
    template=template_retriever,
    input_variables=["event"],
    partial_variables = {"format_instructions": parser_sql.get_format_instructions()})

query = "Tomorrow's Antennas exam at 5:00pm is preponed to today 8:00pm"


chain_c = prompt_classifier | llm
chain_event_generator = prompt_event_generator | llm | parser_event
chain_sql_generator_i = prompt_sql_generator_i | llm | parser_sql
chain_sql_generator_d = prompt_sql_generator_d | llm | parser_sql
chain_retriever = prompt_retriever | llm | parser_sql
chain_event_generator_u  = prompt_event_generator_u | llm | parser_reschedule_event
chain_sql_generator_u = prompt_sql_generator_u | llm | parser_sql



def classifier(AgentState) :

     message = AgentState["message"]
     result = chain_c.invoke({"query": message})
     return  {"work": result.content}

def inserter(AgentState) :
     message = AgentState["message"]
     result = chain_event_generator.invoke({"query": message})
     return {"event": result}


def sql_generator_i(AgentState) :
     event = AgentState["event"]
     event_dict = event.dict()
     filtered_event = {key: value for key, value in event_dict.items() if value != None and value != 'None'}
     print(filtered_event)
     result = chain_sql_generator_i.invoke({"query": filtered_event})

     return {"final_sql_query": result}


def decide_next_node(AgentState):
      work = AgentState["work"]
      print(work)
      if work.find('INSERT') != -1:
         return "INSERT"

      if work.find('UPDATE') != -1:
        return "UPDATE"

      if work.find('DELETE') != -1:
        return "DELETE"

def updater(AgentState) :
        message = AgentState["message"]
        result = chain_event_generator_u.invoke({"query": message})
        return {"event_u": result}

def retrieve_u(AgentState) :
        event = AgentState["event_u"]
        old_event = {
        'event_type': event.event_type,
        'subject_id': event.subject_id,
        'subject_name': event.subject_name,
        'event_date': event.From_event_date,
        'event_time': event.From_event_time
                   }
        new_event = {
        'event_type': event.event_type,
        'subject_id': event.subject_id,
        'subject_name': event.subject_name,
        'event_date': event.To_event_date,
        'event_time': event.To_event_time
         }
        filtered_event = {key: value for key, value in old_event.items() if value is not None and value != 'None'}
        print("The old event is" ,filtered_event)

        sqlquery = chain_retriever.invoke({"event": filtered_event})
        print('generated sql query:', sqlquery)
        print(sqlquery)
        connection = pymysql.connect(host= host, user=user, password=password, database=database, port=port)
        cursor=connection.cursor()
        cursor.execute(sqlquery.sql_query)
        rows = cursor.fetchall()
        print(rows)
        data_list = []
        keys = ['Event_id', 'Subject_id', 'Event_type', 'Event_date', 'Event_time']
        for data in rows:
              data_dict = dict(zip(keys, data))
              data_list.append(data_dict)
        print(data_list)

        return {"events": data_list}

def  sql_generator_u(AgentState) :
       event = AgentState["event_u"]
       events = AgentState["events"]
       old_event = {
        'event_type': event.event_type,
        'subject_id': event.subject_id,
        'subject_name': event.subject_name,
        'event_date': event.From_event_date,
        'event_time': event.From_event_time
                   }
       new_event = {
        'event_type': event.event_type,
        'subject_id': event.subject_id,
        'subject_name': event.subject_name,
        'event_date': event.To_event_date,
        'event_time': event.To_event_time
         }
       filtered_old_event = {key: value for key, value in old_event.items() if value is not None and value != 'None' and key != 'subject_name'}
       filtered_new_event = {key: value for key, value in new_event.items() if value is not None and value != 'None' and key != 'subject_name'}
       result = chain_sql_generator_u.invoke({"old_event": filtered_old_event, "new_event": filtered_new_event,"events": events})

       return {"final_sql_query": result}

def deleter(AgentState) :
    message = AgentState["message"]
    event = chain_event_generator.invoke({"query": message})
    print("the event is :" ,event)
    return  {"event": event}


def retrieve_d(AgentState):
     print("In the retriever......")
     event = AgentState["event"]
     event_dict = event.dict()
     filtered_event = {key: value for key, value in event_dict.items() if value is not None and value != 'None' and key != 'subject_name'}
     print(filtered_event)

     sqlquery = chain_retriever.invoke({"event": filtered_event})
     print('generated sql query:', sqlquery)
     connection = pymysql.connect(host= host, user=user, password=password, database=database, port=port)
     cursor=connection.cursor()
     cursor.execute(sqlquery.sql_query)
     rows = cursor.fetchall()
     print(rows)
     data_list = []
     keys = ['Event_id', 'Subject_id', 'Event_type', 'Event_date', 'Event_time']
     for data in rows:
          data_dict = dict(zip(keys, data))
          data_list.append(data_dict)
     print(data_list)
     return {"events": data_list }

def sql_generator_d(AgentState):
   events = AgentState["events"]
   event = AgentState["event"]
   event_dict = event.dict()
   filtered_event = {key: value for key, value in event_dict.items() if value != None and value != 'None' and key != 'subject_name'}
   result = chain_sql_generator_d.invoke({"events": events, "message": filtered_event})

   return {"final_sql_query": result}


from langgraph.graph import StateGraph
from langgraph.graph import END
# Define a Langchain graph
workflow = StateGraph(AgentState)
workflow.add_node("classifier", classifier)
workflow.add_node("inserter", inserter)
workflow.add_node("retrieve_d", retrieve_d)
workflow.add_node("sql_generator_i", sql_generator_i)
workflow.add_node("sql_generator_d",sql_generator_d)
workflow.add_node("sql_generator_u",sql_generator_u)
workflow.add_node("updater",updater)
workflow.add_node("retrieve_u",retrieve_u)
workflow.add_node("deleter",deleter)
workflow.add_conditional_edges(
    "classifier",
     decide_next_node,
    {

       "INSERT" : "inserter" ,
       "UPDATE" : "updater"  ,
       "DELETE" : "deleter"
    }
)

workflow.add_edge('inserter', 'sql_generator_i')
workflow.add_edge('sql_generator_i', END)
workflow.add_edge("updater","retrieve_u")
workflow.add_edge("deleter","retrieve_d")
workflow.add_edge('retrieve_u', 'sql_generator_u')
workflow.add_edge("retrieve_d","sql_generator_d")
workflow.add_edge("sql_generator_d",END)
workflow.add_edge("sql_generator_u",END)
workflow.set_entry_point("classifier")

app = workflow.compile()

