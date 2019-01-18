using Newtonsoft.Json;
using System;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Movie
    {
        [JsonProperty("adult")]
        public bool? adult { get; set; }

        [JsonProperty("budget")]
        public long? budget { get; set; }

        [JsonProperty("homepage")]
        public string homepage { get; set; }

        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("imdb_id")]
        public string imdb_id { get; set; }

        [JsonProperty("original_language")]
        public string original_language { get; set; }

        [JsonProperty("original_title")]
        public string original_title { get; set; }

        [JsonProperty("overview")]
        public string overview { get; set; }

        [JsonProperty("popularity")]
        public double? popularity { get; set; }

        [JsonProperty("poster_path")]
        public string poster_path { get; set; }

        [JsonProperty("release_date")]
        public DateTime? release_date { get; set; }

        [JsonProperty("revenue")]
        public long? revenue { get; set; }

        [JsonProperty("runtime")]
        public double? runtime { get; set; }

        [JsonProperty("status")]
        public string status { get; set; }

        [JsonProperty("tagline")]
        public string tagline { get; set; }

        [JsonProperty("title")]
        public string title { get; set; }

        [JsonProperty("video")]
        public bool? video { get; set; }

        [JsonProperty("vote_average")]
        public double? vote_average { get; set; }

        [JsonProperty("vote_count")]
        public long? vote_count { get; set; }
    }
}