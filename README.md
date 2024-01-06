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

####Approach

* Created a **API Key** to conecct with YouTube API and retrieved channel, comment and video datas.
* Created seperate functions to extra channel, comment and video datas.
* Once the data is retrieved from the YouTube API, stored it in a **MongoDB**. MongoDB is great at handling bulk data and it can handle unstructured and semi-structured data easily.
* After collecting data for multiple channels,it is then migrated/transformed to a structured data **MySQL**.
* Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.
* Stored the codes on a .pyfile and using Terminal ran the code to connect with **Streamlit** application
* Finally, the retrieved data is displayed in the Streamlit application. Also used Plotly's data visualization features to create charts and graphs to help users analyze the data.
