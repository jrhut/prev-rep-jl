ENV["PYTHONPATH"] = "C:\\Users\\Jamie\\Documents\\Uni\\Research\\venv2\\Lib\\site-packages"

using Pkg
Pkg.activate(".\\..\\Longurl.jl")

println("Loading packages")

using DataFrames
using Longurl
using CSV
using Dates
using URIs

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
start_date = "2021-04-01"
end_date = "now"
collect_new_data = false
use_stored_data = true

if collect_new_data
    using PyCall
    using esextract

    all_data = get_query_dataframe( 
        return_fields=["entities.urls.expanded_url","user.id_str"],
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
                    x -> rename(x, [:url,:userid,:id,:date]) |>
                    x -> select(x, Not(:id)) |>
                    x -> transform(x, :date => ByRow(x -> Date(x, date_format)) => :date)

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
                    x -> select(x, :date, :url, :orig_url, :clean_url, :userid) |>
                    x -> innerjoin(x, reliability, on = :clean_url => :url)

    url_df = [tweets_joined;tweets_joined_short]

    CSV.write("url_df.csv", url_df)
else
    url_df = CSV.File("url_df.csv") |> DataFrame
end

show(url_df)