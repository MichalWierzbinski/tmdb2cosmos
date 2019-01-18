using CsvHelper;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace TheMoviesDb2Cosmos.Parser.Parsers
{
    public abstract class FileParser<T>
    {
        /// <summary>
        /// Reads file content.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> ReadFileContent(string path)
        {
            // read file data
            var reader = new StreamReader(path);

            // config
            var conf = new CsvHelper.Configuration.Configuration
            {
                Delimiter = ",",
                Quote = '\"',
                BadDataFound = null,
                HeaderValidated = null,
                MissingFieldFound = null,
                CultureInfo = CultureInfo.InvariantCulture
            };
            var csv = new CsvReader(reader, conf);

            var records = csv.GetRecords<T>();
            return records;
        }

        /// <summary>
        /// Saves specific file.
        /// </summary>
        /// <param name="items">The items to write to file.</param>
        /// <param name="path">File path.</param>
        public static void SaveJson(object items, string path)
        {
            if (!File.Exists(path))
            {
                // Create a file to write to.
                using (var sw = File.CreateText(path))
                {
                    sw.WriteLine(JsonConvert.SerializeObject(items));
                }
            }
        }

        /// <summary>
        /// Saves all objects in specified paths.
        /// </summary>
        /// <param name="pathItemsDictionary">The path items dictionary.</param>
        public void SaveFiles(Dictionary<string, object> pathItemsDictionary)
        {
            foreach (KeyValuePair<string, object> pathItemsKVP in pathItemsDictionary)
            {
                var path = pathItemsKVP.Key;
                SaveJson(pathItemsKVP.Value, path);
            }
        }
    }
}