
This application allows sending links to the Google indexing API using multiple service accounts. 
A shared queue of links and a list of agents for processing them is created, where each agent corresponds to one service account. 
To connect the JSON keys, create a folder named "json_keys" in the project root directory and place the credentials files inside it.

By default, the script looks for links in a file named "urls.csv" located in the project's root folder. 
The file should contain a line-by-line list of URLs without headers or other data.

For simple database recording in the project, a connection to PostgreSQL is set up.
All you need to do is create the database in advance and provide the connection values in the `.env` file.

For saving the results in an Excel fileyou need to create a folder named "result" in the project's root directory.
