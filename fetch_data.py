import requests  # type: ignore
import csv
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

# Function to fetch thread details, including parent or quoted posts, and replies
def fetch_thread_details(uri, fetch_replies=False):
    if not uri:
        return {
            "DID": "",
            "Handle": "",
            "Display Name": "",
            "CreatedAt": "",
            "Text": "",
            "ReplyCount": 0,
            "RepostCount": 0,
            "LikeCount": 0,
            "QuoteCount": 0,
            "Replies": []
        }
    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread"
    params = {"uri": uri}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            thread = response.json()
            if "thread" in thread and "post" in thread["thread"]:
                post = thread["thread"]["post"]
                record = post.get("record", {})
                author = post.get("author", {})

                replies = []
                if fetch_replies and "replies" in thread["thread"]:
                    for reply in thread["thread"]["replies"][:20]:  # Limit to 20 replies
                        reply_post = reply.get("post", {})
                        reply_record = reply_post.get("record", {})
                        reply_author = reply_post.get("author", {})
                        replies.append({
                            "DID": reply_author.get("did", ""),
                            "Handle": reply_author.get("handle", ""),
                            "Display Name": reply_author.get("displayName", ""),
                            "CreatedAt": reply_record.get("createdAt", ""),
                            "Text": reply_record.get("text", "")
                        })

                return {
                    "DID": author.get("did", ""),
                    "Handle": author.get("handle", ""),
                    "Display Name": author.get("displayName", ""),
                    "CreatedAt": record.get("createdAt", ""),
                    "Text": record.get("text", ""),
                    "ReplyCount": post.get("replyCount", 0),
                    "RepostCount": post.get("repostCount", 0),
                    "LikeCount": post.get("likeCount", 0),
                    "QuoteCount": post.get("quoteCount", 0),
                    "Replies": replies
                }
        else:
            print(f"Skipping URI {uri} due to status code {response.status_code}")
            return {
                "DID": "",
                "Handle": "",
                "Display Name": "",
                "CreatedAt": "",
                "Text": "",
                "ReplyCount": 0,
                "RepostCount": 0,
                "LikeCount": 0,
                "QuoteCount": 0,
                "Replies": []
            }
    except requests.exceptions.RequestException as e:
        print(f"Skipping URI {uri} due to network error: {e}")
        return {
            "DID": "",
            "Handle": "",
            "Display Name": "",
            "CreatedAt": "",
            "Text": "",
            "ReplyCount": 0,
            "RepostCount": 0,
            "LikeCount": 0,
            "QuoteCount": 0,
            "Replies": []
        }

# Function to process posts
def process_posts(posts):
    cleaned_data = []
    for post in posts:
        record = post.get("record", {})
        author = post.get("author", {})

        # Extract main post fields
        did = author.get("did", "")
        handle = author.get("handle", "")
        display_name = author.get("displayName", "")
        created_at = record.get("createdAt", "")
        text = record.get("text", "")
        reply_count = post.get("replyCount", 0)
        repost_count = post.get("repostCount", 0)
        like_count = post.get("likeCount", 0)
        quote_count = post.get("quoteCount", 0)

        # Fetch parent or quoted post details
        reply_to = record.get("reply", {}).get("parent", {}).get("uri", "")
        quoted_post = record.get("embed", {}).get("quoted", {}).get("post", {}).get("uri", "")

        parent_details = fetch_thread_details(reply_to) if reply_to else {
            "DID": "",
            "Handle": "",
            "Display Name": "",
            "CreatedAt": "",
            "Text": "",
            "ReplyCount": 0,
            "RepostCount": 0,
            "LikeCount": 0,
            "QuoteCount": 0
        }

        quoted_details = fetch_thread_details(quoted_post) if quoted_post else {
            "DID": "",
            "Handle": "",
            "Display Name": "",
            "CreatedAt": "",
            "Text": "",
            "ReplyCount": 0,
            "RepostCount": 0,
            "LikeCount": 0,
            "QuoteCount": 0
        }

        # Fetch replies for the current post
        thread_details = fetch_thread_details(post.get("uri"), fetch_replies=True)
        replies = thread_details.get("Replies", [])

        # Extract image links (if available)
        image_links = []
        embeds = record.get("embed", {}).get("images", [])
        for embed in embeds:
            if "fullsize" in embed:
                image_links.append(embed["fullsize"])

        # Extract general embedded links
        general_links = []
        external_links = record.get("embed", {}).get("external", {})
        if external_links:
            general_links.append(external_links.get("uri", ""))

        # Prepare base data for the main post
        post_data = {
            "DID": did,
            "Handle": handle,
            "Display Name": display_name,
            "CreatedAt": created_at,
            "Text": text,
            "ReplyCount": reply_count,
            "RepostCount": repost_count,
            "LikeCount": like_count,
            "QuoteCount": quote_count,
            "Parent DID": parent_details["DID"],
            "Parent Handle": parent_details["Handle"],
            "Parent Display Name": parent_details["Display Name"],
            "Parent CreatedAt": parent_details["CreatedAt"],
            "Parent Text": parent_details["Text"],
            "Quoted DID": quoted_details["DID"],
            "Quoted Handle": quoted_details["Handle"],
            "Quoted Display Name": quoted_details["Display Name"],
            "Quoted CreatedAt": quoted_details["CreatedAt"],
            "Quoted Text": quoted_details["Text"],
            "Image Links": "; ".join(image_links),  # Join multiple image links with a semicolon
            "General Links": "; ".join(general_links)  # Join multiple general links with a semicolon
        }

        # Add replies as separate fields
        for i, reply in enumerate(replies):
            post_data[f"Reply_{i+1}_DID"] = reply.get("DID", "")
            post_data[f"Reply_{i+1}_Handle"] = reply.get("Handle", "")
            post_data[f"Reply_{i+1}_Display Name"] = reply.get("Display Name", "")
            post_data[f"Reply_{i+1}_CreatedAt"] = reply.get("CreatedAt", "")
            post_data[f"Reply_{i+1}_Text"] = reply.get("Text", "")

        # Append the cleaned data
        cleaned_data.append(post_data)
    return cleaned_data

# Function to save cleaned data to CSV
def save_to_csv(data, filename):
    # Prepare column headers dynamically to account for replies
    base_columns = [
        "DID", "Handle", "Display Name", "CreatedAt", "Text", "ReplyCount", "RepostCount", "LikeCount", "QuoteCount",
        "Parent DID", "Parent Handle", "Parent Display Name", "Parent CreatedAt", "Parent Text",
        "Quoted DID", "Quoted Handle", "Quoted Display Name", "Quoted CreatedAt", "Quoted Text",
        "Image Links",  # New column for image links
        "General Links"  # New column for general links
    ]

    reply_columns = []
    for i in range(1, 21):  # For up to 20 replies
        reply_columns.extend([
            f"Reply_{i}_DID", f"Reply_{i}_Handle", f"Reply_{i}_Display Name", f"Reply_{i}_CreatedAt", f"Reply_{i}_Text"
        ])

    all_columns = base_columns + reply_columns

    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=all_columns)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"Data saved to {filename}")

# Main script
if __name__ == "__main__":
    query = "this is fake"
    sort = "latest"
    lang = "en"
    limit = 100

    # Set up date ranges
    start_date = datetime(2023, 7, 1)
    end_date = datetime(2024, 12, 19)
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
    save_to_csv(all_posts, "bluesky_fake.csv")
    print(f"Script complete. Total posts fetched: {len(all_posts)}")