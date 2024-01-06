# YouTube-Data-Harvesting-and-Warehousing
![alt text](https://media.giphy.com/media/13Nc3xlO1kGg3S/giphy.gif)

## Overview

This project is about building a Streamlit application that permits users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data like channel information, video details, comment details and user engagement. The data would be store in **MongoDB** which is a NoSQL database and the data can be migrated to **MySQL** data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

### Tools Used:
* [Python](https://www.python.org/)
* [MongoDB](https://www.mongodb.com/atlas/database)
* [MySQL](https://www.mysql.com/)
* [YouTube Data API](https://developers.google.com/youtube/v3)
* [Streamlit](https://docs.streamlit.io/library/api-reference)
* [Pandas](https://pandas.pydata.org/)
* [Ploty](https://plotly.com/python/)

#### Approach

* Created a **API Key** to conecct with YouTube API and retrieved channel, comment and video datas.
* Created seperate functions to extra channel, comment and video datas.
* Once the data is retrieved from the YouTube API, stored it in a **MongoDB**. MongoDB is great at handling bulk data and it can handle unstructured and semi-structured data easily.
* After collecting data for multiple channels,it is then migrated/transformed to a structured data **MySQL**.
* Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.
* Stored the codes on a .pyfile and using Terminal ran the code to connect with **Streamlit** application
* Finally, the retrieved data is displayed in the Streamlit application. Also used Plotly's data visualization features to create charts and graphs to help users analyze the data.


##### Breakdown

1. Imports necessary libraries, including _googleapiclient_ for interacting with the YouTube API, _pymongo_ for working with MongoDB, _mysql.connector_ for connecting to MySQL database, _pyplot_ to create charts _datetime,re_ for converting video duration to seconds and streamlit for creating the user interface.
2. Create a YouTube API key for making requests to the YouTube API.
3. Created seperate functions to retrive channel,video and comment details **channel_data()**,**Video_Details()**,**comment_data()**.
4. Made connection with **MongoDB** by importing the libary **pymongo** and created a single function **channel_details()** to call the above three functions and store the datas in MongoDB Atlas.
5. Whenever a channel ID is given in the argument of the function **channel_details**, it extras channel,video and comment datas and store it in the database **Youtube_Harvesting**, which is created in MongoDB.
6. Made a connection with MySQL by **mysql.connector** and created a database, then created seperate tables for channels,video and comment with their respective datatype that can be passed to **MySQL**.
7. Imported **Pandas** to convert the data into Dataframe and inserted into MySQL.Defined a function parse_duration to convert the duration of a video from the YouTube API response format to seconds.
8. Defined a function **tables()** that takes a channel ID as input and migrates the channel data from MongoDB to MySQL database. It retrieves the channel data from MongoDB, checks if the data already exists in MySQL, deletes the existing data if necessary, and inserts the new data into the appropriate tables.
9. Imported streamlit and wrote scripts to create button to collect and store data, whenever the user provides a Channel ID and dropdown questions for user intraction.
10. Stored the Python codes in .py file and ran it on streamlit using terminal.
11. Finally, the retrieved data is displayed in the Streamlit application. Also used Plotly's data visualization features to create charts and graphs to help users analyze the data.

###### Conclusion

Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
