﻿# tmdb2cosmos
The purpose of this project is to import tmdb datasets to Cosmos DB Gremlin (Graph) API.

## Getting things ready
* Remember to restore packages 
* Build the solution

## Running the parser
* Download files from this folder: https://drive.google.com/drive/folders/1x91YhLw7nM6FnvPfJOZpo74gpzqs_CLd?usp=sharing
* After that edit the dataPath property in App config file to match whatever directory that has your newly downloaded files
* Run the parser

The parser will create json files that will later be used by the uploader as the source of graph objects.

## Running the uploader
Uploader can be used with either Cosmos DB Emulator or real Cosmos Account. If you don't want to pay for the "real thing" you can create trial account that you can use for 30 days (remember to select Gremlin API): https://azure.microsoft.com/en-us/try/cosmosdb/

Uploader uses BulkExecutor library to import data to Cosmos DB. It is way faster than doing the same thing with individual requests.

* Update settings in config file
* If you want the uploader to create the graph container on its own then set ShouldCleanupOnStart flag to True
* If you don't want the uploader to clean already existing container then set the ShouldCleanupOnStart flag to False
* Run the uploader

## Notes
Data collection after import to Cosmos DB is over 3.5 GB in size (using small ratings file). Import itself will take a while (especially on trial account since max. throughput that you can provision is 5k RUs per container). Importing whole 26 million reviews is not recommended as it would result in graph that is around 30GB in size and running analytical queries on so big dataset on Cosmos is not a good idea. Hovewer if you want to do it anyway then please be my guest ;)

For optimal performance after import partitioned graphs are used by default.

TMDB dataset files needed to be cleaned before they were used by the parser(unknown characters, missing values, random newline characters etc.)... Because of that they are a bit different than the files that can be found on the https://www.kaggle.com/rounakbanik/the-movies-dataset site. I am saying this in case someone tries to run this parser with offical files.

## Vertices
After import you will be able to query following types of vertices:

Legend: vertexLabel
* movie
* person
* genre
* keyword
* company
* country
* language
* collection
* user

## Edges
After import you will be able to query following types of edges:

Legend: edgeLabel (sourceVertexLabel -> targetVertexLabel)
* cast (movie -> person)
* actedIn (person -> movie)
* crewMember (movie -> person)
* \<<department\>> (movie -> person)
* \<<department\>> (person -> movie)
* \<<job\>> (movie -> person)
* \<<job\>> (person -> movie)
* belongsToCollection (movie -> collection)
* rated (user -> movie)
* movieKeyword (movie -> keyword)
* movieKeyword (keyword -> movie)
* movieGenre (movie -> genre)
* movieGenre (genre -> movie)
* movieLanguage (movie -> language)
* movieLanguage (language -> movie)
* producedMovie (person -> movie)
* producedBy (movie -> person)
* productionCountry (country -> movie)
* producedIn (movie -> country)
  
\<<department\>> and \<<job\>> are produced dynamically based on the parsed json file content.
