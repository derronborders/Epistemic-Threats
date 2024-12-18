import tweepy
import csv
from datetime import datetime, timedelta
import time

# Function to authenticate with the X (Twitter) API
def authenticate_x_api():
    # Replace these with your X API credentials
    API_KEY = ""
    API_SECRET = ""
    BEARER_TOKEN = ""

    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    return client

# Function to fetch tweets from X API
def fetch_tweets(client, query, start_time, end_time, max_results=100, next_token=None):
    try:
        response = client.search_recent_tweets(
            query=query,
            start_time=start_time,
            end_time=end_time,
            max_results=max_results,
            tweet_fields=["created_at", "public_metrics", "entities"],
            user_fields=["id", "username", "name"],
            expansions=["author_id"],
            next_token=next_token
        )
        return response
    except Exception as e:
        print(f"Error fetching tweets: {e}")
        return None

# Function to process and clean tweets
def process_tweets(response):
    cleaned_data = []

    if not response or not response.data:
        return cleaned_data

    # Create a mapping of user_id to user details
    users = {user.id: user for user in response.includes.get("users", [])}

    for tweet in response.data:
        user = users.get(tweet.author_id, {})
        metrics = tweet.public_metrics

        # Extract embedded URLs if present
        urls = []
        if "entities" in tweet.data and "urls" in tweet.data["entities"]:
            for url in tweet.data["entities"]["urls"]:
                urls.append(url.get("expanded_url", ""))

        # Add cleaned data
        cleaned_data.append({
            "User ID": user.id if user else "",
            "Handle": user.username if user else "",
            "Display Name": user.name if user else "",
            "CreatedAt": tweet.created_at.isoformat(),
            "Text": tweet.text,
            "Article Links": "; ".join(urls),
            "ReplyCount": metrics.get("reply_count", 0),
            "RepostCount": metrics.get("retweet_count", 0),
            "LikeCount": metrics.get("like_count", 0),
            "QuoteCount": metrics.get("quote_count", 0),
            "IndexedAt": tweet.created_at.isoformat(),
        })
    return cleaned_data

# Function to save data to CSV
def save_to_csv(data, filename):
    keys = ["User ID", "Handle", "Display Name", "CreatedAt", "Text", 
            "Article Links", "ReplyCount", "RepostCount", "LikeCount", 
            "QuoteCount", "IndexedAt"]
    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"Data saved to {filename}")

# Main script
if __name__ == "__main__":
    query = "UnitedHealthcare"  # Search keyword
    start_date = datetime(2024, 12, 4)
    end_date = datetime(2024, 12, 17)
    delta = timedelta(days=1)  # 1-day intervals
    max_results = 100

    client = authenticate_x_api()
    all_tweets = []

    # Loop through each day in the range
    current_date = start_date
    while current_date < end_date:
        start_time = current_date.isoformat("T") + "Z"
        next_date = current_date + delta
        end_time = next_date.isoformat("T") + "Z"
        print(f"Fetching tweets from {start_time} to {end_time}")

        next_token = None
        while True:
            response = fetch_tweets(client, query, start_time, end_time, max_results, next_token)
            if not response:
                print("No more tweets or error occurred.")
                break

            tweets = process_tweets(response)
            all_tweets.extend(tweets)
            print(f"Fetched {len(tweets)} tweets. Total so far: {len(all_tweets)}")

            next_token = response.meta.get("next_token", None)
            if not next_token:
                break
            time.sleep(3)  # Add delay to avoid rate limiting

        current_date += delta  # Move to the next day

    # Save all data to CSV
    save_to_csv(all_tweets, "x_all_tweets_with_links.csv")
    print(f"Script complete. Total tweets fetched: {len(all_tweets)}")
