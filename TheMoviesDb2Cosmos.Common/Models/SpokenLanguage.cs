using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class SpokenLanguage
    {
        [JsonProperty("iso_639_1")]
        public string iso_639_1 { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }
    }
}