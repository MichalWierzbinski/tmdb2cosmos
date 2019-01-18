using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Person
    {
        [JsonProperty("gender")]
        public int gender { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("profilePath")]
        public string profile_path { get; set; }
    }
}