using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using TheMoviesDb2Cosmos.Common.Models;

namespace TheMoviesDb2Cosmos.Parser.Parsers
{
    /// <summary>
    /// Represents movie credits file parser.
    /// </summary>
    public class MovieKeywordsFileParser : FileParser<Keywords>
    {
        /// <summary>
        /// The keywords
        /// </summary>
        public List<Keyword> Keywords = new List<Keyword>();

        /// <summary>
        /// The keyword ids
        /// </summary>
        public HashSet<string> KeywordIds = new HashSet<string>();

        /// <summary>
        /// The crew edges
        /// </summary>
        public List<Tuple<string, string>> KeywordEdges = new List<Tuple<string, string>>();

        /// <summary>
        /// Runs file parsing operation.
        /// </summary>
        public void Run()
        {
            var records =
                ReadFileContent(
                   ConfigurationManager.AppSettings["dataPath"] + "\\keywords.csv");

            var totalProcessedEntries = 0;

            // loop through the records
            ParseRecords(records, totalProcessedEntries);

            // save all collections as json files
            SaveFiles(new Dictionary<string, object>()
            {
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\keywords.json",
                    Keywords
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\keywordEdges.json",
                    KeywordEdges
                },
            });
        }

        /// <summary>
        /// Parses the records.
        /// </summary>
        /// <param name="records">The records.</param>
        /// <param name="totalProcessedLines">The total processed lines.</param>
        public void ParseRecords(IEnumerable<Keywords> records, int totalProcessedLines)
        {
            // loop through the records.
            foreach (var record in records)
            {
                try
                {
                    // Get keywords.
                    var keywords = JsonConvert.DeserializeObject<List<Keyword>>(record.keywords);

                    // now check if we already have these keywords in the keyword collection and add them
                    // to edge collections.
                    foreach (var keyword in keywords)
                    {
                        keyword.id = "keyword-" + keyword.id;

                        if (!KeywordIds.Contains(keyword.id))
                        {
                            KeywordIds.Add(keyword.id);
                            Keywords.Add(keyword);
                        }

                        KeywordEdges.Add(new Tuple<string, string>("movie-" + record.id, keyword.id));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

                totalProcessedLines++;
                if (totalProcessedLines % 10000 == 0)
                    Console.WriteLine($"processed: {totalProcessedLines} entries");
            }
        }
    }
}