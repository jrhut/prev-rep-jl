ENV["PYTHONPATH"] = "/Users/jamie/Documents/Uni/Research/Work/venv/lib/python3.8/site-packages"

using Pkg
Pkg.activate("../Longurl.jl")

println("Loading packages")

using DataFrames
using Longurl
using CSV
using Dates
using URIs
using Plots
using PyCall
using esextract
using Query

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

raw_date_format = DateFormat("e u d H:M:S +0000 Y")
date_format = DateFormat("Y-m-d")

start_date_str = "2020-01-01"
start_date = Date(start_date_str, date_format)

end_date_str = "2021-01-01"
end_date = Date(end_date_str, date_format)

collect_new_data = false
use_stored_data = true


function get_raw_data(sd, ed)
    all_data = get_query_dataframe( 
        return_fields=["id_str", "retweet_count", "reply_count", "favorite_count", "entities.urls.expanded_url"],
        fields_to_search=["tags"],
        search_string="covid",
        start_date=sd,
        end_date=ed,
        field_to_exist="entities.urls.expanded_url")

    write_dataframe("all_data.csv", all_data)
    return all_data
end


function clean_data(all_data)
    println("Cleaning data")

    clean_data = all_data |>
                    x -> rename(x, [:TweetID, :NRetweet, :NReplies, :NLikes, :url, :ID, :Date]) |>
                    x -> select(x, Not(:ID)) |>
                    x -> transform(x, :Date => ByRow(x -> Date(x, raw_date_format)) => :Date)

    println("Processing urls")

    url_data = clean_data |>
                    x -> transform(x, :url => ByRow(x -> 
                            replace(x, r"\[|\]|'" => s"") |> 
                            x -> split(x, ",")) => :url) |>
                    x -> flatten(x, :url) |>
                    x -> transform(x, :url => ByRow(x ->
                            replace(x, r"\"" => s"")) => :orig_url,
                        :url => ByRow(x ->
                            replace(URI(x).host, r"^(http[s]?://)?(www1?\.)?" => s"")) => :clean_url)

    println("Joining reliability data")

    tweets_joined = innerjoin(url_data, reliability, on = :clean_url => :url)
    tweets_antijoined = antijoin(url_data, reliability, on = :clean_url => :url)

    url_df |>
        @filter(_.clean_url in short_urls) |>
        DataFrame

    println("Expanding urls")

    short_expanded = tweets_antijoined |>
                        x -> filter(row -> row.clean_url in short_urls, x)[!, :orig_url] |>
                        x -> unique(x) |>
                        x -> strip.(x) |>
                        x -> String.(x) |>
                        x -> expand_urls(x, 10, "cache")

    original_urls = map(x->x.original_url, short_expanded)
    expanded_urls = map(x->x.expanded_url, short_expanded)
    
    short_expanded_df = DataFrame(orig_url = original_urls, expanded_url = expanded_urls) |> @filter(_.expanded_url != nothing) |> @mutate(expanded_url = string(_.expanded_url)) |> DataFrame

    map(x->URI(x.expanded_url).host, eachrow(short_expanded_df))

    tweets_antijoined_short = innerjoin(tweets_antijoined, short_expanded_df, on = :orig_url)

    CSV.write("tweets_antijoined_short.csv", tweets_antijoined_short)
    
    tweets_antijoined_short = CSV.File("tweets_antijoined_short.csv") |> DataFrame

    tweets_joined_short = tweets_antijoined_short |>
                    x -> transform(x, 
                            :expanded_url => ByRow(z -> replace(z, r"\"" => s"")) => :orig_url) |>
                    x -> transform(x,
                        :expanded_url => ByRow(x ->
                        replace(URI("http://"*x).host, r"^(http[s]?://)?(www1?\.)?" => s"")) => :clean_url) |>
                    x -> select(x, :TweetID, :NRetweet, :NReplies, :NLikes, :Date, :url, :orig_url, :clean_url) |>
                    x -> innerjoin(x, reliability, on = :clean_url => :url)

    url_df = [tweets_joined;tweets_joined_short]

    CSV.write("url_df.csv", url_df)
    return url_df
end


function load_raw_data()
    println("Reading in raw data")
    all_data = CSV.File("all_data.csv") |> DataFrame
    return all_data
end


function load_clean_data()
    println("Reading in saved data")
    url_df = CSV.File("url_df.csv") |> DataFrame
    return url_df
end


function get_week(date) 
    dyear = year(date)
    dweek = week(date)
    if dweek > 52
        dweek -= 52
    end
    return string(dyear)*" "*string(dweek)
end


function get_month(date) 
    return string(year(date))*" "*Dates.monthabbr(month(date))
end


url_df = undef

if collect_new_data
    raw_data = get_raw_data(start_date_str, end_date_str)
    raw_data = load_raw_data()
    url_df = clean_data(raw_data)
elseif !use_stored_data
    raw_data = load_raw_data()
    url_df = clean_data(raw_data)
elseif use_stored_data
    url_df = load_clean_data()
end

show(url_df)

url_df = url_df |>
            @filter(_.Date < end_date) |> 
            DataFrame

#Plotting

plot_s_w = url_df |>
            x -> filter(row -> row.unreliable == true, x) |>
            x -> transform(x, :Date => ByRow(x->get_week(x)) => :Week) |>
            x -> groupby(x, :Week) |>
            x -> combine(x, nrow => :N) |>
            x -> plot(
                    x.Week, x.N, 
                    title="Unreliable sources per week", 
                    xlabel="weeks", 
                    ylabel="n",
                    xticks=(collect(1:10:length(x.Week)),x.Week[1:10:end])
                    )

plot_s_m = url_df |>
            x -> filter(row -> row.unreliable == true, x) |>
            x -> transform(x, :Date => ByRow(x->get_month(x)) => :Month) |>
            x -> groupby(x, :Month) |>
            x -> combine(x, nrow => :N) |>
            x -> plot(
                x.Month, x.N, 
                title = "Unreliable sources per month", 
                xlabel = "months", 
                ylabel = "n",
                xticks=(collect(1:3:length(x.Month)-1),x.Month[1:3:end])
                )

rel_gw_df = url_df |>
                x -> filter(row -> row.unreliable == false, x) |>
                x -> transform(x, :Date => ByRow(x->get_week(x)) => :Week) |>
                x -> groupby(x, :Week) |>
                x -> combine(x, nrow => :RN)

plot_p_w = url_df |>
                x -> filter(row -> row.unreliable == true, x) |>
                x -> transform(x, :Date => ByRow(x->get_week(x)) => :Week) |>
                x -> groupby(x, :Week) |>
                x -> combine(x, nrow => :UN) |>
                x -> innerjoin(x, rel_gw_df, on=:Week) |>
                @mutate(prevalence = _.UN/(_.UN + _.RN)) |> 
                DataFrame |>
                x -> plot(
                    x.Week, x.prevalence, 
                    title = "Prevalenc unreliable sources per week", 
                    xlabel = "weeks", 
                    ylabel = "prevalence",
                    xticks=(collect(1:10:length(x.Week)),x.Week[1:10:end])
                    )

rel_gm_df = url_df |>
                x -> filter(row -> row.unreliable == false, x) |>
                x -> transform(x, :Date => ByRow(x->get_month(x)) => :Month) |>
                x -> groupby(x, :Month) |>
                x -> combine(x, nrow => :RN)
    
plot_p_m = url_df |>
                x -> filter(row -> row.unreliable == true, x) |>
                x -> transform(x, :Date => ByRow(x->get_month(x)) => :Month) |>
                x -> groupby(x, :Month) |>
                x -> combine(x, nrow => :UN) |>
                x -> innerjoin(x, rel_gm_df, on=:Month) |>
                @mutate(prevalence = _.UN/(_.UN + _.RN)) |> 
                DataFrame |>
                x -> plot(
                    x.Month, x.prevalence, 
                    title = "Prevalenc unreliable sources per month", 
                    xlabel = "months", 
                    ylabel = "prevalence",
                    xticks=(collect(1:3:length(x.Month)-1),x.Month[1:3:end])
                    )

plot_4_way = plot(plot_s_w, plot_s_m, plot_p_w, plot_p_m, 
                layout=4, 
                grid=false,
                legend=false,
                size=(1000,1000))

engage_rel_df = url_df |>
                @groupby([_.Date, _.unreliable]) |>
                @map({Key=key(_), RetweetSum=sum(_.NRetweet), RepliesSum=sum(_.NReplies), LikesSum=sum(_.NLikes)}) |>
                @filter(_.Key[2]==false) |> 
                @mutate(Key=_.Key[1]) |>
                DataFrame |>
                x-> DataFrame(Date = x.Key, RetweetCum = cumsum(x.RetweetSum), ReplyCum = cumsum(x.RepliesSum), LikesCum = cumsum(x.LikesSum))

engage_unrel_df = url_df |>
                    @groupby([_.Date, _.unreliable]) |>
                    @map({Key=key(_), RetweetSum=sum(_.NRetweet), RepliesSum=sum(_.NReplies), LikesSum=sum(_.NLikes)}) |>
                    @filter(_.Key[2]==true) |> 
                    @mutate(Key=_.Key[1]) |>
                    DataFrame |>
                    x-> DataFrame(Date = x.Key, RetweetCum = cumsum(x.RetweetSum), ReplyCum = cumsum(x.RepliesSum), LikesCum = cumsum(x.LikesSum))

plot_e_ret = plot(
                engage_rel_df.Date, [engage_rel_df.RetweetCum, engage_unrel_df.RetweetCum],
                title="Cumulative Retweets",
                label=["Reliable" "Unreliable"],
                xlabel="Date",
                ylabel="N Retweets",
                legend = :outertopright
                )

plot_e_rep = plot(
                engage_rel_df.Date, [engage_rel_df.ReplyCum, engage_unrel_df.ReplyCum],
                title="Cumulative Replies",
                label=["Reliable" "Unreliable"],
                xlabel="Date",
                ylabel="N Replies",
                legend = :outertopright
                )

plot_e_like = plot(
                engage_rel_df.Date, [engage_rel_df.LikesCum, engage_unrel_df.LikesCum],
                title="Cumulative Likes",
                label=["Reliable" "Unreliable"],
                xlabel="Date",
                ylabel="N Likes",
                legend = :outertopright
                )

plot_3_way = plot(
                plot_e_ret,plot_e_rep,plot_e_like,
                layout = (3,1),
                grid=false,
                legend = :outertopright,
                size=(1000,1000)
                )
            

savefig(plot_4_way, "plot1.png")
savefig(plot_3_way, "plot2.png")
