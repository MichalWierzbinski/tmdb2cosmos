using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace TheMoviesDb2Cosmos.Common
{
    public static class StringExtensions
    {
        /// <summary>
        /// Transforms string to the pascal case notation.
        /// </summary>
        /// <param name="s">The s.</param>
        /// <returns></returns>
        public static string ToPascalCase(this string s)
        {
            // Find word parts using the following rules:
            // 1. all lowercase starting at the beginning is a word
            // 2. all caps is a word.
            // 3. first letter caps, followed by all lowercase is a word
            // 4. the entire string must decompose into words according to 1,2,3. Note that 2&3
            // together ensure MPSUser is parsed as "MPS" + "User".

            var m = Regex.Match(s, "^(?<word>^[a-z]+|[A-Z]+|[A-Z][a-z]+)+$");
            var g = m.Groups["word"];

            // Take each word and convert individually to TitleCase to generate the final output.
            // Note the use of ToLower before ToTitleCase because all caps is treated as an abbreviation.
            var t = Thread.CurrentThread.CurrentCulture.TextInfo;
            var sb = new StringBuilder();
            foreach (var c in g.Captures.Cast<Capture>())
                sb.Append(t.ToTitleCase(c.Value.ToLower()));
            return sb.ToString();
        }

        /// <summary>
        /// Transforms string to the camel case notation.
        /// </summary>
        /// <param name="s">The s.</param>
        /// <returns></returns>
        public static string ToCamelCase(this string s)
        {
            if (s == null || s.Length < 2)
                return s;

            s = s.Replace("_", string.Empty).Replace("-", string.Empty);

            var words = s.Split(
                new char[] { },
                StringSplitOptions.RemoveEmptyEntries);

            var result = words[0].ToLower();
            for (var i = 1; i < words.Length; i++)
            {
                result +=
                    words[i].Substring(0, 1).ToUpper() +
                    words[i].Substring(1);
            }

            return result;
        }
    }
}