using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace TheMoviesDb2Cosmos.DataUploader
{
    internal class Program
    {
        #region config

        /// <summary>
        /// The endpoint URL
        /// </summary>
        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];

        /// <summary>
        /// The authorization key
        /// </summary>
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];

        /// <summary>
        /// The database name
        /// </summary>
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];

        /// <summary>
        /// The collection name
        /// </summary>
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];

        /// <summary>
        /// The collection throughput
        /// </summary>
        private static readonly int CollectionThroughput =
            int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

        /// <summary>
        /// The connection policy
        /// </summary>
        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        /// <summary>
        /// The client
        /// </summary>
        private readonly DocumentClient client;

        /// <summary>
        /// The total uploaded graph elements count
        /// </summary>
        private static long _totalUploadedGraphElementsCount;

        /// <summary>
        /// The total average writes per sec
        /// </summary>
        private static double _totalAvgWritesPerSec;

        /// <summary>
        /// The total average RUs
        /// </summary>
        private static double _totalAvgRUs;

        /// <summary>
        /// The total upload time sec
        /// </summary>
        private static double _totalUploadTimeSec;

        /// <summary>
        /// The uploader runs
        /// </summary>
        private static int _uploaderRuns;

        #endregion config

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client) => this.client = client;

        /// <summary>
        /// Defines the entry point of the application.
        /// </summary>
        /// <param name="args">The arguments.</param>
        public static void Main(string[] args)
        {
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine($"Endpoint: {EndpointUrl}");
            Console.WriteLine($"Collection : {DatabaseName}.{CollectionName}");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");

            try
            {
                using (var client = new DocumentClient(new Uri(EndpointUrl), AuthorizationKey, ConnectionPolicy))
                {
                    var program = new Program(client);

                    var graphBulkExecutor = program.InitializeBulkExecutor().Result;

                    // Upload vertices
                    UploadVertices(program, graphBulkExecutor);

                    // upload edges
                    UploadEdges(program, graphBulkExecutor);

                    DisplaySummary();
                    //program.CleanupOnFinish().Wait();

                    Console.WriteLine("\nPress any key to exit.");
                    Console.ReadKey();
                }
            }
            catch (AggregateException e)
            {
                Console.WriteLine("Caught AggregateException in Main, Inner Exception:\n" + e);
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Uploads the edges.
        /// </summary>
        /// <param name="program">The program.</param>
        /// <param name="graphBulkExecutor">The graph bulk executor.</param>
        private static void UploadEdges(Program program, IBulkExecutor graphBulkExecutor)
        {
            IEnumerable<GremlinEdge> edges;
            Console.WriteLine("Running: Utils::GenerateRatingEdges...");
            edges = Utils.GenerateRatingEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateBelongsToCollectionEdges...");
            edges = Utils.GenerateBelongsToCollectionEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateActedInAndCastEdges...");
            edges = Utils.GenerateActedInAndCastEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateCrewEdges...");
            edges = Utils.GenerateCrewEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateMovieKeywordEdges...");
            edges = Utils.GenerateMovieKeywordEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateMovieGenreEdges...");
            edges = Utils.GenerateMovieGenreEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateMovieLanguageEdges...");
            edges = Utils.GenerateMovieLanguageEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateProducedByAndProducedMovieEdges...");
            edges = Utils.GenerateProducedByAndProducedMovieEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateProducedInAndProductionCountryEdges...");
            edges = Utils.GenerateProducedInAndProductionCountryEdges();
            Console.WriteLine("Uploading edges...");
            program.RunBulkImportAsync(edges, graphBulkExecutor).Wait();
        }

        /// <summary>
        /// Uploads the vertices.
        /// </summary>
        /// <param name="program">The program.</param>
        /// <param name="graphBulkExecutor">The graph bulk executor.</param>
        private static void UploadVertices(Program program, IBulkExecutor graphBulkExecutor)
        {
            IEnumerable<GremlinVertex> vertices;
            Console.WriteLine("Running: Utils::GeneratePersonVertices...");
            vertices = Utils.GeneratePersonVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateKeywordVertices...");
            vertices = Utils.GenerateKeywordVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateGenreVertices...");
            vertices = Utils.GenerateGenreVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateCountryVertices...");
            vertices = Utils.GenerateCountryVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateCompanyVertices...");
            vertices = Utils.GenerateCompanyVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateLanguageVertices...");
            vertices = Utils.GenerateLanguageVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateMovieVertices...");
            vertices = Utils.GenerateMovieVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateCollectionVertices...");
            vertices = Utils.GenerateCollectionVertices();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
            Console.WriteLine("Running: Utils::GenerateUserVerticesSmall...");
            vertices = Utils.GenerateUserVerticesSmall();
            Console.WriteLine("Uploading vertices...");
            program.RunBulkImportAsync(vertices, graphBulkExecutor).Wait();
        }

        /// <summary>
        /// Driver function for bulk import.
        /// </summary>
        /// <returns></returns>
        private async Task RunBulkImportAsync(IEnumerable<object> documentsToUpload, IBulkExecutor bulkExecutor)
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            BulkImportResponse response = null;

            try
            {
                response = await bulkExecutor.BulkImportAsync(
                    documentsToUpload,
                    enableUpsert: true,
                    disableAutomaticIdGeneration: true,
                    maxConcurrencyPerPartitionKeyRange: null,
                    maxInMemorySortingBatchSize: null,
                    cancellationToken: token);
            }
            catch (DocumentClientException de)
            {
                Console.WriteLine("Document client exception: {0}", de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: {0}", e);
            }

            DisplayBatchSummary(response);
        }

        /// <summary>
        /// Initializes the bulk executor.
        /// </summary>
        /// <returns></returns>
        private async Task<IBulkExecutor> InitializeBulkExecutor()
        {
            var dataCollection = await CleanupOnStart();

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            IBulkExecutor graphBulkExecutor = new GraphBulkExecutor(client, dataCollection);
            await graphBulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;
            return graphBulkExecutor;
        }

        /// <summary>
        /// Cleanups the on finish.
        /// </summary>
        /// <returns></returns>
        private async Task CleanupOnFinish()
        {
            // Cleanup on finish if set in config.
            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Console.WriteLine("Deleting Database {0}", DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }
        }

        /// <summary>
        /// Displays the batch summary.
        /// </summary>
        /// <param name="response">The bulk import response.</param>
        private static void DisplayBatchSummary(BulkImportResponse response)
        {
            Console.WriteLine("\nSummary for batch");
            Console.WriteLine("--------------------------------------------------------------------- ");

            var docCount = response.NumberOfDocumentsImported;
            var writesPerSec =
                Math.Round(response.NumberOfDocumentsImported / response.TotalTimeTaken.TotalSeconds);
            var rus = Math.Round(response.TotalRequestUnitsConsumed / response.TotalTimeTaken.TotalSeconds);
            var avgRus = response.TotalRequestUnitsConsumed / response.NumberOfDocumentsImported;
            var time = response.TotalTimeTaken.TotalSeconds;

            Console.WriteLine(
                "Inserted {0} graph elements @ {1} writes/s, {2} RU/s in {3} sec)",
                docCount,
                writesPerSec,
                rus,
                time);
            Console.WriteLine(
                "Average RU consumption per insert: {0}",
                avgRus);
            Console.WriteLine("---------------------------------------------------------------------\n ");

            _totalUploadTimeSec += time;
            _totalUploadedGraphElementsCount += docCount;
            _totalAvgRUs += rus;
            _uploaderRuns++;
            _totalAvgWritesPerSec += writesPerSec;
        }

        /// <summary>
        /// Displays the total bulk executor job summary.
        /// </summary>
        private static void DisplaySummary()
        {
            Console.WriteLine("Final stats");
            Console.WriteLine("--------------------------------------------------------------------- ");

            Console.WriteLine(
                "Inserted {0} graph elements @ avg. {1} writes/s, avg {2} RU/s in {3} sec)",
                _totalUploadedGraphElementsCount,
                _totalAvgWritesPerSec / _uploaderRuns,
                _totalAvgRUs / _uploaderRuns,
                _totalUploadTimeSec);
            Console.WriteLine("---------------------------------------------------------------------\n ");
        }

        /// <summary>
        /// Cleanups the on start.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">The data collection does not exist</exception>
        private async Task<DocumentCollection> CleanupOnStart()
        {
            // Cleanup on start if set in config.

            DocumentCollection dataCollection;
            try
            {
                if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]))
                {
                    var database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Console.WriteLine("Creating database {0}", DatabaseName);
                    await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                    Console.WriteLine($"Creating collection {CollectionName} with {CollectionThroughput} RU/s");
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName,
                        CollectionThroughput);
                }
                else
                {
                    dataCollection = Utils.GetCollectionIfExists(client, DatabaseName, CollectionName);
                    if (dataCollection == null)
                    {
                        throw new Exception("The data collection does not exist");
                    }
                }
            }
            catch (Exception de)
            {
                Console.WriteLine("Unable to initialize, exception message: {0}", de.Message);
                throw;
            }

            return dataCollection;
        }
    }
}