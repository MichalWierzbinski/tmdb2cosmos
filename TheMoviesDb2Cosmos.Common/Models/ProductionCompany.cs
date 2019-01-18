using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class ProductionCompany
    {
        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }
    }
}