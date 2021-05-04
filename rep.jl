ENV["PYTHONPATH"] = "C:\\Users\\Jamie\\Documents\\Uni\\Research\\venv2\\Lib\\site-packages"

using Pkg
Pkg.activate(".\\..\\Longurl.jl")

println("Loading packages")

using DataFrames
using Longurl
using CSV
using Dates
using URIs
using Plots

reliability = CSV.File("reliability.csv") |> DataFrame

short_urls = [
  "ow.ly",
  "tinyurl.com",
  "chng.it",
  "dlvr.it",
  "buff.ly",
  "bit.ly",
  "disq.us",
  "is.gd"
]

date_format = DateFormat("e u d H:M:S +0000 Y")
start_date_format = DateFormat("Y-m-d")
start_date = "2021-01-01"
end_date = "now"
collect_new_data = true
use_stored_data = false
start_date_date = Date(start_date, start_date_format)

if collect_new_data
    using PyCall
    using esextract

    all_data = get_query_dataframe( 
        return_fields=["id_str", "retweet_count", "reply_count", "favorite_count", "entities.urls.expanded_url"],
        fields_to_search=["tags"],
        search_string="covid",
        start_date=start_date,
        end_date=end_date,
        field_to_exist="entities.urls.expanded_url")

    write_dataframe("all_data.csv", all_data)
end

if !use_stored_data
    println("Reading in data")
    all_data = CSV.File("all_data.csv") |> DataFrame

    println("Cleaning data")

    clean_data = all_data |>
                    x -> rename(x, [:TweetID,:NRetweet, :NReplies, :NLikes, :url, :ID, :Date]) |>
                    x -> select(x, Not(:ID)) |>
                    x -> transform(x, :Date => ByRow(x -> Date(x, date_format)) => :Date)

    println("Processing urls")

    url_data = clean_data |>
                    x -> transform(x, :url => ByRow(x -> 
                            replace(x, r"\[|\]|'" => s"") |> 
                            x -> split(x, ",")) => :url) |>
                    x -> flatten(x, :url) |>
                    x -> transform(x, :url => ByRow(x ->
                            replace(x, r"\"" => s"")) => :orig_url,
                        :url => ByRow(x ->
                            URI(x).host) => :clean_url)

    println("Joining reliability data")

    tweets_joined = innerjoin(url_data, reliability, on = :clean_url => :url)
    tweets_antijoined = antijoin(url_data, reliability, on = :clean_url => :url)

    println("Expanding urls")

    short_expanded = tweets_antijoined |>
                        x -> filter(row -> row.clean_url in short_urls, x)[!, :orig_url] |>
                        x -> unique(x) |>
                        x -> strip.(x) |>
                        x -> String.(x) |>
                        x -> expand_urls(x, 10, "cache")

    original_urls = map(x->x.original_url, short_expanded)
    expanded_urls = map(x->x.expanded_url, short_expanded)

    short_expanded_df = DataFrame(orig_url = original_urls, expanded_url = expanded_urls)

    tweets_antijoined_short = innerjoin(tweets_antijoined, short_expanded_df, on = :orig_url)

    tweets_joined_short = tweets_antijoined_short |>
                    x -> dropmissing(x) |>
                    x -> transform(x, :expanded_url => :orig_url,
                        :orig_url => ByRow(x ->
                            URI(x).host) => :clean_url) |>
                    x -> select(x, :TweetID, :NRetweet, :NReplies, :NLikes, :ID, :Date, :url, :orig_url, :clean_url) |>
                    x -> innerjoin(x, reliability, on = :clean_url => :url)

    url_df = [tweets_joined;tweets_joined_short]

    CSV.write("url_df.csv", url_df)
else
    println("Reading in saved data")
    url_df = CSV.File("url_df.csv") |> DataFrame
end

#show(url_df)

function weeks(date) 
    return (date - start_date_date) ÷ 7
end

function months(date) 
    return (date - start_date_date) ÷ 30
end

plot_1 = url_df |>
            x -> filter(row -> row.unreliable == true, x) |>
            x -> transform(x, :Date => ByRow(x ->
                    weeks(x)) => :weeks) |>
            x -> groupby(x, :weeks) |>
            x -> combine(x, nrow => :n) |>
            x -> plot(x.weeks, x.n)

savefig(plot_1, "plot.png")
