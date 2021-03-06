﻿using Newtonsoft.Json;

namespace TheMoviesDb2Cosmos.Common.Models
{
    public class Genre
    {
        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }
    }
}