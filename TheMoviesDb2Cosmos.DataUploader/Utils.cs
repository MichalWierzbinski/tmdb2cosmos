using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TheMoviesDb2Cosmos.Common;
using TheMoviesDb2Cosmos.Common.Models;

namespace TheMoviesDb2Cosmos.DataUploader
{
    internal sealed class Utils
    {
        #region Database Operations

        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        public static DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName,
            string collectionName) => GetDatabaseIfExists(client, databaseName) == null
                ? null
                : client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        public static Database GetDatabaseIfExists(DocumentClient client, string databaseName) => client
            .CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        public static async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client,
            string databaseName,
            string collectionName, int collectionThroughput)
        {
            var partitionKey = new PartitionKeyDefinition
            {
                Paths = new Collection<string> { $"/{ConfigurationManager.AppSettings["CollectionPartitionKey"]}" }
            };
            var collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };

            try
            {
                collection = await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e)
            {
                throw e;
            }

            return collection;
        }

        #endregion Database Operations

        #region Vertex Generators

        /// <summary>
        /// Generates the person vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GeneratePersonVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\persons.json");

            var persons = JsonConvert.DeserializeObject<List<Person>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var person in persons)
            {
                var v = new GremlinVertex(person.id, "person");
                v.AddProperty("pk", person.id);
                v.AddProperty("name", person.name);
                v.AddProperty("gender", person.gender);
                v.AddProperty("profilePath", person.profile_path ?? string.Empty);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the keywords vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateKeywordVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\keywords.json");

            var keywords = JsonConvert.DeserializeObject<List<Keyword>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var keyword in keywords)
            {
                var v = new GremlinVertex(keyword.id, "keyword");
                v.AddProperty("pk", keyword.id);
                v.AddProperty("name", keyword.name);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the genre vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateGenreVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\genres.json");

            var genres = JsonConvert.DeserializeObject<List<Genre>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var genre in genres)
            {
                var v = new GremlinVertex(genre.id, "genre");
                v.AddProperty("pk", genre.id);
                v.AddProperty("name", genre.name);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the country vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateCountryVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\countries.json");

            var countries = JsonConvert.DeserializeObject<List<ProductionCountry>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var country in countries)
            {
                var v = new GremlinVertex(country.iso_3166_1, "country");
                v.AddProperty("pk", country.iso_3166_1);
                v.AddProperty("name", country.name);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the company vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateCompanyVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\companies.json");

            var companies = JsonConvert.DeserializeObject<List<ProductionCompany>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var company in companies)
            {
                var v = new GremlinVertex(company.id, "company");
                v.AddProperty("pk", company.id);
                v.AddProperty("name", company.name);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the language vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateLanguageVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\languages.json");

            var languages = JsonConvert.DeserializeObject<List<SpokenLanguage>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var language in languages)
            {
                var v = new GremlinVertex(language.iso_639_1, "language");
                v.AddProperty("pk", language.iso_639_1);
                v.AddProperty("name", language.name);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the movies vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateMovieVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\movies.json");

            var movies = JsonConvert.DeserializeObject<List<Movie>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var movie in movies)
            {
                var v = new GremlinVertex(movie.id, "movie");
                v.AddProperty("pk", movie.id);
                v.AddProperty("title", movie.title);

                if (movie.adult != null)
                    v.AddProperty("isAdult", movie.adult);

                v.AddProperty("budget", movie.budget ?? -1);
                v.AddProperty("homepage", movie.homepage ?? string.Empty);
                v.AddProperty("imdbId", movie.imdb_id ?? string.Empty);
                v.AddProperty("originalLanguage", movie.original_language ?? string.Empty);
                v.AddProperty("originalTitle", movie.original_title ?? string.Empty);
                v.AddProperty("overview", movie.overview ?? string.Empty);
                v.AddProperty("popularity", movie.popularity ?? -1.0);
                v.AddProperty("posterPath", movie.poster_path ?? string.Empty);
                v.AddProperty("releaseDate", movie.release_date ?? DateTime.MinValue.ToUniversalTime());
                v.AddProperty("revenue", movie.revenue ?? -1);
                v.AddProperty("runtime", movie.runtime ?? -1.0);
                v.AddProperty("status", movie.status ?? string.Empty);
                v.AddProperty("tagline", movie.tagline ?? string.Empty);

                if (movie.video != null)
                    v.AddProperty("isVideo", movie.video);

                v.AddProperty("voteAverage", movie.vote_average ?? -1.0);
                v.AddProperty("voteCount", movie.vote_count ?? -1);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the collection vertices.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateCollectionVertices()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Vertices\collections.json");

            var collections = JsonConvert.DeserializeObject<List<BelongsToCollection>>(text);
            var vertices = new List<GremlinVertex>();

            foreach (var collection in collections)
            {
                var v = new GremlinVertex(collection.id, "collection");
                v.AddProperty("pk", collection.id);
                v.AddProperty("name", collection.name);
                v.AddProperty("backdropPath", collection.backdrop_path ?? string.Empty);
                v.AddProperty("posterPath", collection.poster_path ?? string.Empty);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the user vertices (from small file - 100K ratings).
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateUserVerticesSmall()
        {
            var lines =
                File.ReadAllLines(
                    ConfigurationManager.AppSettings["dataPath"] + @"\ratings_small.csv");

            var vertices = new List<GremlinVertex>();

            foreach (var line in lines)
            {
                var data = line.Split(',');
                var v = new GremlinVertex(data[0], "collection");
                v.AddProperty("pk", data[0]);
                v.AddProperty("name", data[0]);

                vertices.Add(v);
            }

            return vertices;
        }

        /// <summary>
        /// Generates the user vertices (from big file - 20M ratings).
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinVertex> GenerateUserVerticesFull()
        {
            var lines =
                File.ReadAllLines(
                    ConfigurationManager.AppSettings["dataPath"] + @"\ratings.csv");

            var vertices = new List<GremlinVertex>();

            foreach (var line in lines)
            {
                var data = line.Split(',');
                var v = new GremlinVertex(data[0], "collection");
                v.AddProperty("pk", data[0]);
                v.AddProperty("name", data[0]);

                vertices.Add(v);
            }

            return vertices;
        }

        #endregion Vertex Generators

        #region Edge Generators

        /// <summary>
        /// Generates the rating edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateRatingEdges()
        {
            var lines =
                File.ReadAllLines(
                    ConfigurationManager.AppSettings["dataPath"] + @"\ratings_small.csv");

            var edges = new List<GremlinEdge>();

            for (var index = 1; index < lines.Length; index++)
            {
                var line = lines[index];
                var data = line.Split(',');

                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "rated",
                    data[0],
                    data[1],
                    "user",
                    "movie",
                    data[0],
                    data[1]);

                e.AddProperty("rating", data[2]);
                e.AddProperty("timestamp", long.Parse(data[3]));

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the belongsToCollection edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateBelongsToCollectionEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\belongsToCollectionEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is collection ID
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "belongsToCollection",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "collection",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the actedIn and cast edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateActedInAndCastEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\castEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is castMember object
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, CastMember>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "actedIn",
                    tuple.Item2.id,
                    tuple.Item1,
                    "person",
                    "movie",
                    tuple.Item2.id,
                    tuple.Item1);

                e.AddProperty("character", tuple.Item2.character);
                e.AddProperty("creditId", tuple.Item2.credit_id);
                e.AddProperty("order", tuple.Item2.order);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // and we label that edge as 'cast'
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "cast",
                    tuple.Item1,
                    tuple.Item2.id,
                    "movie",
                    "person",
                    tuple.Item1,
                    tuple.Item2.id);

                e.AddProperty("character", tuple.Item2.character);
                e.AddProperty("creditId", tuple.Item2.credit_id);
                e.AddProperty("order", tuple.Item2.order);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the belongsToCollection edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateCrewEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\crewEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is crewMember object
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, CrewMember>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    tuple.Item2.job.ToCamelCase() ?? tuple.Item2.department.ToCamelCase() ?? "crewMember",
                    tuple.Item2.id,
                    tuple.Item1,
                    "person",
                    "movie",
                    tuple.Item2.id,
                    tuple.Item1);

                e.AddProperty("department", tuple.Item2.department);
                e.AddProperty("creditId", tuple.Item2.credit_id);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // as above we generate edge labels based on the job property of the CrewMember object
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    tuple.Item2.job.ToCamelCase() ?? tuple.Item2.department.ToCamelCase() ?? "crewMember",
                    tuple.Item1,
                    tuple.Item2.id,
                    "movie",
                    "person",
                    tuple.Item1,
                    tuple.Item2.id);

                e.AddProperty("department", tuple.Item2.department);
                e.AddProperty("creditId", tuple.Item2.credit_id);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the movieKeyword edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateMovieKeywordEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\keywordEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is keyword ID object
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieKeyword",
                    tuple.Item2,
                    tuple.Item1,
                    "keyword",
                    "movie",
                    tuple.Item2,
                    tuple.Item1);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieKeyword",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "keyword",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the movieGenre edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateMovieGenreEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\movieGenreEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is genre ID
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieGenre",
                    tuple.Item2,
                    tuple.Item1,
                    "genre",
                    "movie",
                    tuple.Item2,
                    tuple.Item1);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieGenre",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "genre",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the movieLanguage edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateMovieLanguageEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\movieLanguageEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is language ID
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieLanguage",
                    tuple.Item2,
                    tuple.Item1,
                    "language",
                    "movie",
                    tuple.Item2,
                    tuple.Item1);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "movieLanguage",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "language",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the producedBy and producedMovie edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateProducedByAndProducedMovieEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\producedByEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is company ID
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "producedMovie",
                    tuple.Item2,
                    tuple.Item1,
                    "company",
                    "movie",
                    tuple.Item2,
                    tuple.Item1);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "producedBy",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "company",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        /// <summary>
        /// Generates the producedIn and productionCountry edges.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<GremlinEdge> GenerateProducedInAndProductionCountryEdges()
        {
            // Read the file as one string.
            var text =
                File.ReadAllText(
                    ConfigurationManager.AppSettings["dataPath"] + @"\Edges\producedInEdges.json");

            // deserialize tuple list. First item in every tuple is movie ID and the second one is country ID
            var tuples = JsonConvert.DeserializeObject<List<Tuple<string, string>>>(text);
            var edges = new List<GremlinEdge>();

            foreach (var tuple in tuples)
            {
                var e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "productionCountry",
                    tuple.Item2,
                    tuple.Item1,
                    "country",
                    "movie",
                    tuple.Item2,
                    tuple.Item1);

                edges.Add(e);

                // we add second edge going in opposite direction for better query performance
                // when traversing the graph from movie vertices...
                // the downside to that approach is information duplication together with bigger graph & index size
                e = new GremlinEdge(Guid.NewGuid().ToString(),
                    "producedIn",
                    tuple.Item1,
                    tuple.Item2,
                    "movie",
                    "country",
                    tuple.Item1,
                    tuple.Item2);

                edges.Add(e);
            }

            return edges;
        }

        #endregion Edge Generators
    }
}