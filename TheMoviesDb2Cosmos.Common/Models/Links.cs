using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Links
    {
        [JsonProperty("movieId")]
        public long movieId { get; set; }

        [JsonProperty("imdbId")]
        public string imdbId { get; set; }

        [JsonProperty("tmdbId")]
        public long tmdbId { get; set; }
    }
}