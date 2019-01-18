using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    internal class MovieCreditEdge
    {
        [JsonProperty("creditId")]
        public string credit_id { get; set; }

        [JsonProperty("department")]
        public string department { get; set; }

        [JsonProperty("movieId")]
        public long movieId { get; set; }

        [JsonProperty("job")]
        public string job { get; set; }

        [JsonProperty("character")]
        public string character { get; set; }
    }
}