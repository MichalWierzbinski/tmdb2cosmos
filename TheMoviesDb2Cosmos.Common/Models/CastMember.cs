using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class CastMember
    {
        [JsonProperty("credit_id")]
        public string credit_id { get; set; }

        [JsonProperty("character")]
        public string character { get; set; }

        [JsonProperty("gender")]
        public int gender { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("order")]
        public long order { get; set; }

        [JsonProperty("profile_path")]
        public string profile_path { get; set; }
    }
}