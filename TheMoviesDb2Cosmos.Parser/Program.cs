using System;
using TheMoviesDb2Cosmos.Parser.Parsers;

namespace TheMoviesDb2Cosmos.Parser
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Parsing movie metadata file...");
            var movieMetadataFileParser = new MovieMetadataFileParser();
            movieMetadataFileParser.Run();

            Console.WriteLine("Parsing movie credits file...");
            var movieCreditsParser = new MovieCreditsFileParser();
            movieCreditsParser.Run();

            Console.WriteLine("Parsing movie keywords file...");
            var movieKeywordsParser = new MovieKeywordsFileParser();
            movieKeywordsParser.Run();

            Console.ReadKey();
        }
    }
}