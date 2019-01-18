using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Credits
    {
        [JsonProperty("cast")]
        public string cast { get; set; }

        [JsonProperty("crew")]
        public string crew { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }
    }
}