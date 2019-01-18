using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Ratings
    {
        [JsonProperty("movieId")]
        public long movieId { get; set; }

        [JsonProperty("userId")]
        public long userId { get; set; }

        [JsonProperty("rating")]
        public float rating { get; set; }

        [JsonProperty("timestamp")]
        public long timestamp { get; set; }
    }
}