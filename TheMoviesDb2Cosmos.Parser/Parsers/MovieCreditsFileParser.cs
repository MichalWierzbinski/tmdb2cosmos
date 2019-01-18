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
    public class MovieCreditsFileParser : FileParser<Credits>
    {
        /// <summary>
        /// The persons
        /// </summary>
        public List<Person> Persons = new List<Person>();

        /// <summary>
        /// The person ids
        /// </summary>
        public HashSet<string> PersonIds = new HashSet<string>();

        /// <summary>
        /// The cast edge
        /// </summary>
        public List<Tuple<string, CastMember>> CastEdges = new List<Tuple<string, CastMember>>();

        /// <summary>
        /// The crew edges
        /// </summary>
        public List<Tuple<string, CrewMember>> CrewEdges = new List<Tuple<string, CrewMember>>();

        /// <summary>
        /// Runs file parsing operation.
        /// </summary>
        public void Run()
        {
            var records =
                ReadFileContent(ConfigurationManager.AppSettings["dataPath"] + "\\credits.csv");

            var totalProcessedEntries = 0;

            // loop through the records
            ParseRecords(records, totalProcessedEntries);

            // save all collections as json files
            SaveFiles(new Dictionary<string, object>()
            {
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Vertices\\persons.json",
                    Persons
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\castEdges.json",
                    CastEdges
                },
                {
                   ConfigurationManager.AppSettings["dataPath"] + "\\Edges\\crewEdges.json",
                    CrewEdges
                },
            });
        }

        /// <summary>
        /// Parses the records.
        /// </summary>
        /// <param name="records">The records.</param>
        /// <param name="totalProcessedLines">The total processed lines.</param>
        public void ParseRecords(IEnumerable<Credits> records, int totalProcessedLines)
        {
            // loop through the records.
            foreach (var record in records)
            {
                try
                {
                    // Get crew and cast.
                    var castMembers = JsonConvert.DeserializeObject<List<CastMember>>(record.cast);
                    var crewMembers = JsonConvert.DeserializeObject<List<CrewMember>>(record.crew);

                    // now check if we already have those people in the people collection and add them
                    // to edge collections.
                    foreach (var castMember in castMembers)
                    {
                        castMember.id = "person-" + castMember.id;

                        if (!PersonIds.Contains(castMember.id))
                        {
                            PersonIds.Add(castMember.id);
                            Persons.Add(new Person()
                            {
                                id = castMember.id,
                                gender = castMember.gender,
                                name = castMember.name,
                                profile_path = castMember.profile_path
                            });
                        }
                        CastEdges.Add(new Tuple<string, CastMember>("movie-" + record.id, castMember));
                    }

                    foreach (var crewMember in crewMembers)
                    {
                        crewMember.id = "person-" + crewMember.id;

                        if (!PersonIds.Contains(crewMember.id))
                        {
                            PersonIds.Add(crewMember.id);
                            Persons.Add(new Person()
                            {
                                id = crewMember.id,
                                gender = crewMember.gender,
                                name = crewMember.name,
                                profile_path = crewMember.profile_path
                            });
                        }

                        CrewEdges.Add(new Tuple<string, CrewMember>("movie-" + record.id, crewMember));
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