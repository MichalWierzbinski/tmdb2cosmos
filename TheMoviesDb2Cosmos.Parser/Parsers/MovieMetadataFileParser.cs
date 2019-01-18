using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using TheMoviesDb2Cosmos.Common.Models;

namespace TheMoviesDb2Cosmos.Parser.Parsers

{
    /// <summary>
    /// Represents movie metadata parser.
    /// </summary>
    public class MovieMetadataFileParser : FileParser<MovieMetadata>
    {
        /// <summary>
        /// The genres
        /// </summary>
        public List<Genre> Genres = new List<Genre>();

        /// <summary>
        /// The spoken languages
        /// </summary>
        public List<SpokenLanguage> SpokenLanguages = new List<SpokenLanguage>();

        /// <summary>
        /// The production countries
        /// </summary>
        public List<ProductionCountry> ProductionCountries = new List<ProductionCountry>();

        /// <summary>
        /// The production companies
        /// </summary>
        public List<ProductionCompany> ProductionCompanies = new List<ProductionCompany>();

        /// <summary>
        /// The collections
        /// </summary>
        public List<BelongsToCollection> Collections = new List<BelongsToCollection>();

        /// <summary>
        /// The movies
        /// </summary>
        public List<Movie> Movies = new List<Movie>();

        /// <summary>
        /// The belongs to collection edges
        /// </summary>
        public List<Tuple<string, string>> BelongsToCollectionEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// The belongs to genre edges
        /// </summary>
        public List<Tuple<string, string>> BelongsToGenreEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// The produced by edges
        /// </summary>
        public List<Tuple<string, string>> ProducedByEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// The produced in edges
        /// </summary>
        public List<Tuple<string, string>> ProducedInEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// The spoken in edges
        /// </summary>
        public List<Tuple<string, string>> SpokenInEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// Main methods for parsing movie metadata files.
        /// </summary>
        public void Run()
        {
            var records =
                ReadFileContent(
                   ConfigurationManager.AppSettings["dataPath"] + "\\movies_metadata.csv");

            // Lists of objects

            var totalProcessedEntries = 0;

            // loop through the records
            ParseRecords(records, totalProcessedEntries);

            // save all collections as json files
            SaveFiles(new Dictionary<string, object>()
            {
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\genres.json",
                    Genres
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\languages.json",
                    SpokenLanguages
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\countries.json",
                    ProductionCountries
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\companies.json",
                    ProductionCompanies
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\collections.json",
                    Collections
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\movies.json",
                    Movies
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\belongsToCollectionEdges.json",
                    BelongsToCollectionEdges
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\movieLanguageEdges.json",
                    SpokenInEdges
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\producedInEdges.json",
                    ProducedInEdges
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\producedByEdges.json",
                    ProducedByEdges
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\movieGenreEdges.json",
                    BelongsToGenreEdges
                },
            });
        }

        /// <summary>
        /// Parses all object lists.
        /// </summary>
        /// <param name="records">List of records from the file.</param>
        /// <param name="totalProcessedEntries">Total processed lines count.</param>
        private void ParseRecords(IEnumerable<MovieMetadata> records, int totalProcessedEntries)
        {
            foreach (var record in records)
            {
                Movies.Add(new Movie()
                {
                    adult = record.adult,
                    budget = record.budget,
                    homepage = record.homepage,
                    id = "movie-" + record.id,
                    imdb_id = record.imdb_id,
                    original_language = record.original_language,
                    original_title = record.original_title,
                    overview = record.overview,
                    popularity = record.popularity,
                    poster_path = record.poster_path,
                    release_date = record.release_date,
                    revenue = record.revenue,
                    runtime = record.runtime,
                    status = record.status,
                    tagline = record.tagline,
                    title = record.title,
                    video = record.video,
                    vote_average = record.vote_average,
                    vote_count = record.vote_count,
                });

                ParseGenres(Genres, record);

                ParseSpokenLanguages(SpokenLanguages, record);

                ParseCountries(ProductionCountries, record);

                ParseCompanies(ProductionCompanies, record);

                ParseCollections(Collections, record);

                totalProcessedEntries++;
                if (totalProcessedEntries % 10000 == 0)
                    Console.WriteLine($"processed: {totalProcessedEntries} entries");
            }
        }

        /// <summary>
        /// Parses the collection list.
        /// </summary>
        /// <param name="collections">The collection list.</param>
        /// <param name="record">The movie metadata record.</param>
        private void ParseCollections(ICollection<BelongsToCollection> collections, MovieMetadata record)
        {
            try
            {
                var belongsToCollection =
                    JsonConvert.DeserializeObject<BelongsToCollection>(record.belongs_to_collection);

                if (belongsToCollection == null) return;

                belongsToCollection.id = "collection-" + belongsToCollection.id;

                var collectionIds = collections.Select(x => x.id).ToList();

                // add only new collections to the list
                if (!collectionIds.Contains(belongsToCollection.id))
                    collections.Add(belongsToCollection);

                BelongsToCollectionEdges.Add(new Tuple<string, string>("movie-" + record.id, belongsToCollection.id));
            }
            catch (Exception e)
            {
                Trace.TraceError("Error while parsing collection info..." + e.Message);
            }
        }

        /// <summary>
        /// Parses the companies list.
        /// </summary>
        /// <param name="productionCompanies">The companies list.</param>
        /// <param name="record">The movie metadata record.</param>
        private void ParseCompanies(List<ProductionCompany> productionCompanies, MovieMetadata record)
        {
            try
            {
                var productionCompanyArray =
                    JsonConvert.DeserializeObject<List<ProductionCompany>>(record.production_companies);

                foreach (var productionCompany in productionCompanyArray)
                {
                    productionCompany.id = "prodCompany-" + productionCompany.id;
                }

                var companyIds = productionCompanies.Select(x => x.id).ToList();

                // add only new companies to the list
                productionCompanies.AddRange(productionCompanyArray.Where(company => !companyIds.Contains(company.id)));

                foreach (var productionCompany in productionCompanyArray)
                {
                    ProducedByEdges.Add(
                        new Tuple<string, string>("movie-" + record.id, productionCompany.id));
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error while parsing production companies info..." + e.Message);
            }
        }

        /// <summary>
        /// Parses the countries list.
        /// </summary>
        /// <param name="productionCountries">The countries list.</param>
        /// <param name="record">The movie metadata record.</param>
        private void ParseCountries(List<ProductionCountry> productionCountries, MovieMetadata record)
        {
            try
            {
                var productionCountryArray =
                    JsonConvert.DeserializeObject<List<ProductionCountry>>(record.production_countries);

                foreach (var prodCountry in productionCountryArray)
                {
                    prodCountry.iso_3166_1 = "prodCountry-" + prodCountry.iso_3166_1;
                }

                var prodCountryIds = productionCountries.Select(x => x.iso_3166_1).ToList();

                // add only new countries to the list
                productionCountries.AddRange(
                    productionCountryArray.Where(country => !prodCountryIds.Contains(country.iso_3166_1)));

                foreach (var productionCountry in productionCountryArray)
                {
                    ProducedInEdges.Add(new Tuple<string, string>("movie-" + record.id, productionCountry.iso_3166_1));
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error while parsing production countries info..." + e.Message);
            }
        }

        /// <summary>
        /// Parses the languages list.
        /// </summary>
        /// <param name="spokenLanguages">The language list.</param>
        /// <param name="record">The movie metadata record.</param>
        private void ParseSpokenLanguages(List<SpokenLanguage> spokenLanguages, MovieMetadata record)
        {
            try
            {
                var spokenLanguagesArray =
                    JsonConvert.DeserializeObject<List<SpokenLanguage>>(record.spoken_languages);

                foreach (var spokenLanguage in spokenLanguagesArray)
                {
                    spokenLanguage.iso_639_1 = "spokenLang-" + spokenLanguage.iso_639_1;
                }

                var langIds = spokenLanguages.Select(x => x.iso_639_1).ToList();

                // add only new languages to the list
                spokenLanguages.AddRange(spokenLanguagesArray.Where(lang => !langIds.Contains(lang.iso_639_1)));

                foreach (var spokenLanguage in spokenLanguagesArray)
                {
                    SpokenInEdges.Add(new Tuple<string, string>("movie-" + record.id, spokenLanguage.iso_639_1));
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error while parsing spoken languages info..." + e.Message);
            }
        }

        /// <summary>
        /// Parses the genre list.
        /// </summary>
        /// <param name="genres">The genre list.</param>
        /// <param name="record">The movie metadata record.</param>
        private void ParseGenres(List<Genre> genres, MovieMetadata record)
        {
            try
            {
                var genreArray = JsonConvert.DeserializeObject<List<Genre>>(record.genres);

                foreach (var genre in genreArray)
                {
                    genre.id = "genre-" + genre.id;
                }

                var genreIds = genres.Select(x => x.id).ToList();

                // add only new genres to the list
                genres.AddRange(genreArray.Where(genre => !genreIds.Contains(genre.id)));

                foreach (var genre in genreArray)
                {
                    BelongsToGenreEdges.Add(new Tuple<string, string>("movie-" + record.id, genre.id));
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error while parsing genre info..." + e.Message);
            }
        }
    }
}