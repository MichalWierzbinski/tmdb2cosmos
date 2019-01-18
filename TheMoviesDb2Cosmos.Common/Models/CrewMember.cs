using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class CrewMember
    {
        [JsonProperty("credit_id")]
        public string credit_id { get; set; }

        [JsonProperty("department")]
        public string department { get; set; }

        [JsonProperty("gender")]
        public int gender { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("job")]
        public string job { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("profile_path")]
        public string profile_path { get; set; }
    }
}