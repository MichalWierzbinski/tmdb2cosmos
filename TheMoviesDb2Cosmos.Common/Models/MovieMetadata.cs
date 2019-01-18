using Newtonsoft.Json;
using System;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class MovieMetadata
    {
        [JsonProperty("adult")]
        public bool? adult { get; set; }

        [JsonProperty("belongsToCollection")]
        public string belongs_to_collection { get; set; }

        [JsonProperty("budget")]
        public long? budget { get; set; }

        [JsonProperty("genres")]
        public string genres { get; set; }

        [JsonProperty("homepage")]
        public string homepage { get; set; }

        [JsonProperty("id")]
        public long id { get; set; }

        [JsonProperty("imdbId")]
        public string imdb_id { get; set; }

        [JsonProperty("originalLanguage")]
        public string original_language { get; set; }

        [JsonProperty("originalTitle")]
        public string original_title { get; set; }

        [JsonProperty("overview")]
        public string overview { get; set; }

        [JsonProperty("popularity")]
        public double? popularity { get; set; }

        [JsonProperty("posterPath")]
        public string poster_path { get; set; }

        [JsonProperty("productionCompanies")]
        public string production_companies { get; set; }

        [JsonProperty("productionCountries")]
        public string production_countries { get; set; }

        [JsonProperty("releaseDate")]
        public DateTime? release_date { get; set; }

        [JsonProperty("revenue")]
        public long? revenue { get; set; }

        [JsonProperty("runtime")]
        public double? runtime { get; set; }

        [JsonProperty("spokenLanguages")]
        public string spoken_languages { get; set; }

        [JsonProperty("status")]
        public string status { get; set; }

        [JsonProperty("tagline")]
        public string tagline { get; set; }

        [JsonProperty("title")]
        public string title { get; set; }

        [JsonProperty("video")]
        public bool? video { get; set; }

        [JsonProperty("voteAverage")]
        public double? vote_average { get; set; }

        [JsonProperty("voteCount")]
        public long? vote_count { get; set; }
    }
}