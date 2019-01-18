using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Keywords
    {
        [JsonProperty("id")]
        public long id { get; set; }

        [JsonProperty("keywords")]
        public string keywords { get; set; }
    }
}