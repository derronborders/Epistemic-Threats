import requests
import csv
from dateutil.parser import isoparse
from datetime import datetime, timedelta
import time

# Function to fetch posts with pagination
def search_bluesky_posts(query, sort="latest", since=None, until=None, lang=None, limit=100, cursor=None):
    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    params = {
        "q": query,
        "sort": sort,
        "since": since,
        "until": until,
        "lang": lang,
        "limit": limit,
        "cursor": cursor,
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get("posts", []), data.get("cursor", None)
        else:
            print(f"Error: Received status code {response.status_code}")
            return [], None
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        return [], None

# Function to save cleaned data to CSV
def save_to_csv(data, filename):
    keys = [
        "DID", "Handle", "Display Name", "CreatedAt", "Text", 
        "Article Title", "Article Link", "ReplyCount", 
        "RepostCount", "LikeCount", "QuoteCount", "IndexedAt"
    ]
    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"Data saved to {filename}")

# Function to process and clean posts
def process_posts(posts):
    cleaned_data = []
    for post in posts:
        record = post.get("record", {})
        author = post.get("author", {})
        embed = record.get("embed", {})

        # Extract fields safely
        did = author.get("did", "")
        handle = author.get("handle", "")
        display_name = author.get("displayName", "")
        created_at = record.get("createdAt", "")
        text = record.get("text", "")
        reply_count = post.get("replyCount", 0)
        repost_count = post.get("repostCount", 0)
        like_count = post.get("likeCount", 0)
        quote_count = post.get("quoteCount", 0)
        indexed_at = post.get("indexedAt", "")

        # Extract external embed (title and URI)
        article_title = ""
        article_link = ""
        if embed.get("$type") == "app.bsky.embed.external#view":
            external = embed.get("external", {})
            article_title = external.get("title", "")
            article_link = external.get("uri", "")

        # Append the cleaned data
        cleaned_data.append({
            "DID": did,
            "Handle": handle,
            "Display Name": display_name,
            "CreatedAt": created_at,
            "Text": text,
            "Article Title": article_title,
            "Article Link": article_link,
            "ReplyCount": reply_count,
            "RepostCount": repost_count,
            "LikeCount": like_count,
            "QuoteCount": quote_count,
            "IndexedAt": indexed_at,
        })
    return cleaned_data

# Main script
if __name__ == "__main__":
    query = "UnitedHealthcare"
    sort = "latest"
    lang = "en"
    limit = 100

    # Set up date ranges
    start_date = datetime(2024, 12, 4)
    end_date = datetime(2024, 12, 17)
    delta = timedelta(days=1)  # 1-day intervals

    all_posts = []

    # Loop through each day in the range
    current_date = start_date
    while current_date <= end_date:
        since = current_date.isoformat() + "Z"
        until = (current_date + delta).isoformat() + "Z"
        print(f"Fetching posts from {since} to {until}")

        cursor = None
        while True:
            posts, cursor = search_bluesky_posts(query, sort, since, until, lang, limit, cursor)
            if not posts:
                print("No more posts for this date range.")
                break

            all_posts.extend(process_posts(posts))
            print(f"Fetched {len(posts)} posts. Total so far: {len(all_posts)}")

            if not cursor:
                break
            time.sleep(5)  # Add delay to avoid rate limiting

        current_date += delta  # Move to the next day

    # Save all data to CSV
    save_to_csv(all_posts, "bluesky_all_posts_with_articles.csv")
    print(f"Script complete. Total posts fetched: {len(all_posts)}")
