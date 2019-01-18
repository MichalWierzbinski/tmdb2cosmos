using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class BelongsToCollection
    {
        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("poster_path")]
        public string poster_path { get; set; }

        [JsonProperty("backdrop_path")]
        public string backdrop_path { get; set; }
    }
}